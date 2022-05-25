import datetime
import json
import logging
import operator
from functools import partial

import yaml
import django
from django.contrib.contenttypes.fields import GenericForeignKey

if django.VERSION[0] >= 3:
    from django.db.models import JSONField
else:
    from django.contrib.postgres.fields.jsonb import JSONField
from django.core.exceptions import ValidationError, FieldDoesNotExist
from django.core.management.color import no_style
from django.core.serializers import python as serializers
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.db.models import ForeignKey, TextField, CharField
from django.db.models import fields as django_fields
from django.utils.functional import keep_lazy_text
from tabulate import tabulate

NoneType = type(None)

logger = logging.getLogger(__name__)


def get_field(model, field):
    """
    Recursive model field finder, you can pass it something simple like this:
      > get_field(Model1, 'field1')
      <DjangoField: field1>
    Or complex like this:
      > get_field(Model2, 'model1__model3__model4__id')
      <DjangoField: model4>
    """
    field, _, related = field.partition('__')
    Field = model._meta.get_field(field)
    if not related:
        return Field
    return get_field(Field.related_model, related)


def extract_field_value(data_or_model, field, raise_exceptions=False):
    """
    Similar to get_field() but this works on the gherkin provided data
    """
    if not raise_exceptions and data_or_model is None:
        return data_or_model

    get_val = getattr
    if isinstance(data_or_model, dict):
        get_val = operator.getitem
        if field in data_or_model:
            return data_or_model[field]

    try:
        if '__' not in field:
            val = get_val(data_or_model, field)
            return str(val) if isinstance(val, (dict, list)) and not isinstance(data_or_model._meta.get_field(field), JSONField) else val
        base_field, _, remaining_field = field.partition('__')
        return extract_field_value(get_val(data_or_model, base_field), remaining_field, raise_exceptions)
    except AttributeError:
        logger.exception('')
        if raise_exceptions:
            raise
        return None


@keep_lazy_text
def pretty_print_table(fields, data):
    """
    Stitching two lists, fields + data; the reason is that if we give the fields/headers to tabulate, it will add a
    weird line under the fields/headers that gherkin doesn't like and would complicate the code in order to remove it.
    """
    try:
        rows = [fields] + [[extract_field_value(row, key) for key in fields] for row in data]
    except AttributeError:
        # We could't format it, just dump it.
        return f'\n\nWhat we actually got back was:\n{json.dumps(data, indent=2, cls=DjangoJSONEncoder)}'

    pretty_table = tabulate(rows, tablefmt='orgtbl', numalign='left')
    return f'\n\nWhat we actually got back was:\n{pretty_table}'


class GenericSerializer(serializers.Serializer):
    """
    Serializer to convert the GenericForeignKey fields to natural keys
    """
    def serialize(self, *args, **kwargs):
        self.selected_all_fields = kwargs['fields']
        kwargs['fields'] = [field for field in kwargs['fields'] if field not in '__']
        return super().serialize(*args, **kwargs)

    def handle_fk_field(self, obj, field):
        """
        We convert any nk value entered as `[NK]` into what natural key methods expect: `['NK']`.

        We also convert values for ContentType fields, where the natural key consists of more than one field. We allow
        the developer to enter `[poll, Question]` as the value for the content type field in the test. We then convert
        that value to what content type really expects: `['poll', 'Question']`.
        """
        super().handle_fk_field(obj, field)

        # When serializing models from our testing MigrationGherkin framework, those models will never have
        # natural_key() or get_by_natural_key() in the model's Manager because the model is built dynamically from
        # the current migration script and all prior migration scripts. This extra step compensates for that.
        if obj.__module__ == '__fake__':
            related = getattr(obj, field.name)
            model_name = related.__class__.__name__
            if related:
                if hasattr(related, 'nk'):
                    self._current[field.name] = [related.nk]
                elif model_name == 'ContentType':
                    self._current[field.name] = [related.app_label, related.model]
                else:
                    raise AttributeError(f'Fake model {model_name} does not have a natural key')

        if self._current[field.name]:
            self._current[field.name] = yaml.dump(list(self._current[field.name])).strip()

    def start_object(self, obj):
        self._current = {}

    def get_dump_object(self, obj):
        missing_fields = set(self._current.keys()) ^ set(self.selected_fields)
        for field_name in missing_fields:
            field = get_field(obj, field_name)
            if isinstance(field, GenericForeignKey):
                data = [yaml.dump(list(extract_field_value(obj, field.ct_field).natural_key())).strip(),
                        yaml.dump(list(extract_field_value(obj, field.name).natural_key())).strip()]
                self._current[field.name] = f'[{", ".join(data)}]'
            else:
                self._current[field_name] = extract_field_value(obj, field_name)
        return self._current


class ParseQuery:
    """
    Serializes the queryset and parses the fields into pretty dicts to be used by the tabulate function

    Note that the name is this way because this class is more of a callable than an instance.
    See https://www.python.org/dev/peps/pep-0008/#class-names for more details.
    """
    serialize = partial(GenericSerializer().serialize, use_natural_primary_keys=True, use_natural_foreign_keys=True)

    def __init__(self, queryset, *fields):
        self.queryset = queryset
        self.fields = fields

    def __iter__(self):
        for data in self.serialize(self.queryset, fields=self.fields):
            yield data


def parse_step_objects(context, Model, raise_exceptions=True):
    """
    Parses a table that represents a model/factory.

    This can be used for both display or create/update of models/factories.

    :param Model Model: Model the aloe step table represents
    :param bool raise_exceptions: When doing NK lookups should DoesNotExist be thrown
    :return: yields step row dict with python data
    """
    obj_by_nk_by_field = {}  # {field_name: {nk_value: fk_object}}
    obj_by_id_by_field = {}  # {field_name: {id_value: fk_object}}
    fk_model_by_field = {}  # {field_name: fk_model}
    generic_foreign_key_fields = set()

    # Examine each field specified in the hash table.
    # If it is a generic foreign key, add it to generic_foreign_key_fields.
    # If it is an FK supporting natural keys, add it to obj_by_nk_by_field.
    for field_name in context.table.headings:
        field = get_field(Model, field_name)
        if isinstance(field, GenericForeignKey):
            # GenericForeignKey needs special treatment since there are two pieces to it: 1) the content_type_field,
            # and 2) the object_id_field. Together they evaluate to a single object and that object is the one we care
            # about.
            ForeignModel = Model._meta.get_field(field.ct_field).related_model
            generic_foreign_key_fields.add(field_name)
            is_generic_foreign_key = True
        else:
            ForeignModel = field.related_model  # This is null if this field is not a ForeignKey
            is_generic_foreign_key = False

        if ForeignModel:
            # GenericForeignKey cannot be referenced by ids
            if not is_generic_foreign_key:
                # Tags the field as needing to query all ids in order to show/evaluate the table properly
                obj_by_id_by_field[field_name] = {}
                fk_model_by_field[field_name] = ForeignModel

            # Tags the field as having the ability to search for/display NKs
            foreign_model_fields = [x.name for x in ForeignModel._meta.get_fields()]
            if hasattr(ForeignModel.objects, 'get_by_natural_key') or 'nk' in foreign_model_fields:
                obj_by_nk_by_field[field_name] = {}
                fk_model_by_field[field_name] = ForeignModel

    # Populate a lookup dicts with all the nks and ids that need to be filled in with real models
    for _item in context.table.rows:
        item = _item.as_dict()
        for lookup_field in obj_by_nk_by_field.keys():
            value = item[lookup_field]
            if not value:
                # No need to look for something that's not there..
                pass
            elif value.startswith('[') and value.endswith(']'):
                # Handle special "[..]" nk format
                obj_by_nk_by_field[lookup_field][value] = yaml.load(value, Loader=yaml.FullLoader)
            elif not value.isdigit():
                raise ValueError(f'Please specify a natural key as "[{value}]"')
            else:
                # If the number is a digit, it's probably a literal id -- thus we can ignore the lookup
                pass

    for lookup_field in obj_by_id_by_field.keys():
            value = item[lookup_field]
            if value.isdigit():
                obj_by_id_by_field[lookup_field][value] = None

    # Do a single lookup call for all ids at once.
    for lookup_field, value_map in obj_by_id_by_field.items():
        for instance in get_field(Model, lookup_field).related_model.objects.filter(pk__in=value_map.keys()):
            value_map[str(instance.id)] = instance

    # Find the FK object for each nk.
    for field_name, obj_by_nk in obj_by_nk_by_field.items():
        queryset = fk_model_by_field[field_name].objects
        for original, nk_args in list(obj_by_nk.items()):
            if field_name in generic_foreign_key_fields:
                # Process a generic foreign key field.
                try:
                    # The content type is the first element in the list of nk args for a generic foreign key.
                    # The first element has a nested list of args: app, model_name
                    content_type = queryset.get_by_natural_key(*nk_args[0])

                    # The nk value(s) is the second element in the list of nk args for a generic foreign key.
                    # If it's a simple string, the nk has one element. Otherwise it's a list of nk elements.
                    nk_elements = nk_args[1]
                    if isinstance(nk_elements, str):
                        nk_elements = [nk_elements]

                    # Get the object the generic foreign key points to.
                    obj = content_type.model_class().objects.get_by_natural_key(*nk_elements)
                    obj_id = obj.pk
                except fk_model_by_field[field_name].DoesNotExist:
                    if raise_exceptions:
                        raise
                    content_type = obj = obj_id = None

                # The generic foreign key field actually points to 2 other fields that are persisted to the DB:
                # ct_field: holds the id of the content type of the Model class
                # fk_field: holds the pk of the actual object_ in that Model class
                model_field = get_field(Model, field_name)
                obj_by_nk_by_field[field_name][original] = ReplaceField({
                    model_field.ct_field: content_type,
                    model_field.fk_field: obj_id,
                    field_name: obj
                })
            else:
                try:
                    value = queryset.get_by_natural_key(*nk_args)
                except fk_model_by_field[field_name].DoesNotExist:
                    value = None
                    if raise_exceptions:
                        raise fk_model_by_field[field_name]\
                            .DoesNotExist(f'{fk_model_by_field[field_name]} matching {nk_args} does not exist.')
                except AttributeError:
                    value = queryset.get(nk=nk_args[0])
                # Process a simple foreign key field.
                obj_by_nk_by_field[field_name][original] = value

    for row in context.table.rows:
        value_by_field = {}
        # Convert the string value specified for each field in the hash table to its true python value object.
        for field in row.headings:
            str_value = row.get(field)
            if str_value in obj_by_nk_by_field.get(field, {}):
                # If we found an object by its natural key or generic foreign key, use that object.
                # If no object was found by natural key or generic foreign key, use the key as the value so when we
                # get an error, the error will show the key value.
                value = obj_by_nk_by_field[field].get(str_value, str_value)
            elif str_value in obj_by_id_by_field.get(field, {}):
                value = obj_by_id_by_field[field].get(str_value, str_value)
                # Need to rename the field, there is no concept of '<field>_id' only '<field>'
                field = get_field(Model, field).name
            else:
                # If the field was not a natural key or generic foreign key, the field converts it to its python value.
                value = get_to_python_field(Model, field, str_value)

            if isinstance(value, ReplaceField):
                # If the value is a ReplaceField object, it actually holds the value for several fields.
                # Populate each field the ReplaceField object holds.
                value_by_field.update(value)
            else:
                # Otherwise we have a single value for a single field.
                value_by_field[field] = value
        yield value_by_field


class ReplaceField(dict):
    """
    Special dictionary object used to tag a field that needs to be ignored completely and replaced with whatever this
    holds in it's content. This is mainly used to replace fake/private fields like "GenericForeignKey".
    """
    pass


def get_to_python_field(model, field, value):
    """
    Model.Meta.field.to_python() override, there are some to_python functions that don't behave as expected, this
    function is here to ensure that the correct outcome is returned.

    Example: ForeignKey.to_python() returns the exact value you send it, if you send it a dict, it sends back the same
             dict, nothing changed. This is a problem when you have a number coalesced as a string and you need it to
             be converted to a integer.
    """
    field = get_field(model, field)
    if isinstance(field, ForeignKey):
        # ForeignKey don't like empty strings, they need to be converted to None
        if value == '':
            return None
        field = field.related_model._meta.pk
    elif isinstance(field, (TextField, CharField)):
        if value == '':
            return None
        # Since an empty value is always converted to None, an empty string must be specified as '""' or "''".
        elif value == '""' or value == "''":
            return ''
    elif isinstance(field, JSONField):
        # JSONField.to_python() is not implemented. See the reason at https://code.djangoproject.com/ticket/29147
        # Handle this case to fulfill our own needs.
        return json.loads(value)
    try:
        return field.to_python(value)
    except ValidationError:
        return None


def reset_db_seq(Model, next_value=None):
    """
    Reset a DB table's sequence to desired next_value.

    :param Model :type Model
    :param next_value :type int. If specified, set the sequence so the next value assigned is next_value.
                                 If unspecified, set the sequence so the next value is max(id) + 1.
    """
    try:
        Model._meta.get_field('id')
    except FieldDoesNotExist:
        return  # If id field does not exist, do not reset the sequence.

    with connection.cursor() as cursor:
        if not next_value:
            # Find the highest id value in use if next_value is None or 0.
            cursor.execute(f"SELECT MAX(id) FROM {Model._meta.db_table}")
            highest_value = cursor.fetchone()[0]
            if not isinstance(highest_value, int):
                return

            # If the table has a max(id) of None, it is empty so start the next value at 1.
            # If the table has a max(id) of 1, start the next value at 2.
            # If the table has a max(id) of 3, start the next value at 4.
            highest_value = highest_value or 0
            next_value = highest_value + 1
        # The 3rd parameter we send to setval() specifies whether the specified value has already been used.
        # If false, the next value returned will be the specified value.
        # If true,  the next value returned will be the specified value + 1.
        # https://www.postgresql.org/docs/9.6/static/functions-sequence.html
        # cursor.execute(f"SELECT NVL('{next_value}) {Model._meta.db_table}_id_seq', %s, %s)", (next_value, value_was_used))
        sequence_sql = connection.ops.sequence_reset_sql(no_style(), [Model])
        for sql in sequence_sql:
            cursor.execute(sql)

    return next_value


def convert_type(obj):
    """
    Given the instance of an object, how do we convert a string to that object.

    Limitations: iterables are NOT supported, you'll get strange results, example type `[1,2,3]` will correctly
                 respond with `list()` BUT when you pass in `'[1,2,3]'` your output will be pure strangeness:
                 `['[', '1', ',', '2', ',', '3', ']']` -- for right now, this can we solved when there is a need
                 for it to behave correctly.

    >>> convert_type(datetime.date(2000,1,1))('2000-1-2')
    datetime.date(2000, 1, 2)
    >>> convert_type('hello')(1)
    '1'
    >>> convert_type(None)('')
    >>> convert_type(1.2)(1)
    1.0
    >>> convert_type(1)('3')
    3
    """
    obj_type = type(obj)
    if obj_type == NoneType:
        return lambda x: None if x == '' else x
    elif obj_type == datetime.datetime:
        return partial(django_fields.DateTimeField.to_python, None)
    elif obj_type == datetime.date:
        return partial(django_fields.DateField.to_python, None)
    return obj_type


def get_model(model):
    try:
        app_label, model_name, through_model = model.split('.')
        Model = django.apps.apps.get_model(app_label=app_label, model_name=model_name)
        return getattr(Model, through_model).through
    except ValueError:
        app_label, model_name = model.split('.')
        return django.apps.apps.get_model(app_label=app_label, model_name=model_name)

