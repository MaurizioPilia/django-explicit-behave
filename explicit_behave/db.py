import inspect
import operator
from ast import literal_eval
from collections import defaultdict
from functools import reduce, partial

import yaml
from behave import *
from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model, models
from django.core.files.base import ContentFile
from django.db import reset_queries, connection
from django.db.models import Q, signals
from django.utils.functional import lazystr
from factory.django import mute_signals
from model_bakery import baker

from .utils import (pretty_print_table, extract_field_value, reset_db_seq, parse_step_objects, ParseQuery,
                    get_model)

UserModel = get_user_model()

all_model_signals = [signal for signal in vars(signals).values() if isinstance(signal, signals.ModelSignal)]


@step('limpio "([^\"]+)"( without resetting its sequence)?')
def clear_the_db(context, model, without_reset_seq):
    Model = get_model(model)
    Model.objects.all().delete()
    if not without_reset_seq:
        reset_db_seq(Model)


@step('reseteo las sequencias de "([^\"]+)"')
def clear_the_db(context, model):
    Model = get_model(model)
    reset_db_seq(Model)


@step('(limpio e )?inserto las siguientes lineas del modelo "([^\"]+)"( sin signals)?')
def insert_to_db(context, limpio, model, no_signals):
    """
    I insert the following rows for "app.Model":
      | nk | name       | age | student__nk |
      | S1 | John Smith | 18  | S1          |
      | S2 | Jane Brown | 19  | S2          |
    I insert the following rows for "app.Model" without signals:
    I clear and insert the following rows for "full.path.to.Factory":
    I clear and insert the following rows for "full.path.to.Factory" without signals:
    """
    Model = get_model(model)

    _all_model_signals = all_model_signals
    if not no_signals:
        # `without signals` means to suppress all signals fired by saving a model row.
        _all_model_signals = []

    if limpio:
        # `clear and` means to delete all rows in the table first.
        # Use the Model to delete all rows.
        Model.objects.all().delete()
        reset_db_seq(Model, next_value=1)

    many_to_many_names = {field.name: field.related_model for field in Model._meta.many_to_many}
    with mute_signals(*_all_model_signals):
        for data in list(parse_step_objects(context, Model, raise_exceptions=False)):
            for field in context.table.headings:
                if field in many_to_many_names.keys():
                    data[field] = many_to_many_names[field].objects.filter(pk__in=literal_eval(data[field]))
            baker.make(Model, **data, _create_files=True)
    reset_db_seq(Model)


@step('modifico las siguientes lineas del modelo "([^\"]+)" identificadas por "([^\"]+)"( sin signals)?')
def update_row_in_db(context, model, filter_fields, without_signals):
    """
    Update existing table rows.

    `without signals` means to suppress all signals fired by saving a model row.

    Examples:
      I update the following rows for "app.Model" identified by "nk, name":
        | nk | name       | age |
        | S1 | John Smith | 18  |
        | S2 | Jane Brown | 19  |
      I update the following rows for "app.Model" identified by "nk" without signals:

    Please note, you CANNOT do multiple field nesting, example: `foreign_field__attribute=False`, you will need to
    update `attribute` in a separate steps.
    """
    Model = get_model(model)
    # The fields that identify each row are separated by a comma.
    filter_fields = [field.strip() for field in filter_fields.split(',')]

    for value_by_field in parse_step_objects(context, Model):
        # Remove the filtering (identifying) field from the data values.
        filters = {key: value_by_field.pop(key) for key in filter_fields}
        # Get the row identified by the filter fields, and update it with the data field values.
        Model.objects.filter(**filters).update(**value_by_field)
        if not without_signals:
            # qs.update does not call signals, but qs.save does.
            Model.objects.get(**filters).save()


@step('hay "([0-9]+)" "([^\"]+)" en base de datos')
def step_impl(context, count, model):
    Model = get_model(model)
    assert Model.objects.count() == int(count)


@step('"([^\"]+)" (tiene exactamente|contiene) las siguientes lineas identificadas por "([^\"]+)"(?: ordenadas por "([^\"]+)")?')
def database_has_rows(context, model, exact_contain, filter_fields, order_fields):
    """
    Verify the contents of a table.

    Examples:
      Then "app.Model" has exactly the following rows identified by "id":
        | id | name       | age | student |
        | 1  | John Smith | 18  | [S1]    |
        | 2  | Jane Brown | 19  | [S2]    |
      Then "app.Model" contains the following rows identified by "id, age":
        | id | name       | age | student |
        | 1  | John Smith | 18  | [S1]    |

      # Note: We can traverse field and check foreign key attributes
      Then "app.Model" contains the following rows identified by "id, age":
        | id | name       | age | student__email   |
        | 1  | John Smith | 18  | jsmith@schoo.com |

    """
    Model = get_model(model)

    fields = context.table.headings
    filter_fields = [field.strip() for field in filter_fields.split(',')]

    actual_values_by_id = {}
    queryset = Model.objects.select_related()
    if order_fields:
        order_fields = [field.strip() for field in order_fields.split(',')]
        queryset = queryset.order_by(*order_fields)
    else:
        queryset = queryset.order_by('pk')
    # Only filter the query if it's a contain, otherwise we'll always retrieve the entire table contents
    if exact_contain == 'contiene':
        filters = []
        for row in context.table.rows:
            _filter = {}
            for field in filter_fields:
                if field in fields:
                    _filter[field] = row[field]
                else:
                    raise KeyError(f'Rows cannot be identified by "{field}". '
                                   f'Ensure that "{field}" is present in the hash table.')

                ForeignModel = getattr(queryset.model._meta.get_field(field), 'related_model')
                if ForeignModel and hasattr(ForeignModel.objects, 'get_by_natural_key'):
                    keys = inspect.getfullargspec(ForeignModel.objects.get_by_natural_key).args[1:]
                    _filter.update(dict(zip([f'{field}__{k}' for k in keys], yaml.load(_filter.pop(field), Loader=yaml.FullLoader))))
            if _filter:
                filters.append(Q(**_filter))
        queryset = queryset.filter(reduce(operator.or_, filters))

    error_msg = lazystr(partial(pretty_print_table, fields, ParseQuery(queryset, *fields)))

    for model in queryset:
        clean_row = {field: extract_field_value(model, field) for field in fields}
        key = tuple([clean_row[field] for field in filter_fields])
        if key in actual_values_by_id:
            raise Model.MultipleObjectsReturned(f'Uniquely identifying rows by {filter_fields} is not enough. '
                                                f'Specify fields whose combination is guaranteed to be unique.')
        actual_values_by_id[key] = clean_row

    # hashes = [{'student': <Student ...>, 'age': 18, 'nk': 'A1'}, {'student': <Student ...>, 'age': 19, 'nk': 'A2'}]
    # Note: all the row values are already converted to Python values
    try:
        hashes = list(parse_step_objects(context, Model, raise_exceptions=False))
    except ValueError:
        print(error_msg)
        raise

    expected_values_by_id = {}
    # Clean up the values that will then be compared, aloe gives it all to us in string format, using the model
    # we then convert it to a python value.
    for row_dict in hashes:
        clean_row = {field: extract_field_value(row_dict, field) for field in fields}
        key = tuple([clean_row[key] for key in filter_fields])
        expected_values_by_id[key] = clean_row
    # Ensure that all the ids we got back are the same as what we expected
    assert actual_values_by_id.keys() == expected_values_by_id.keys(), (actual_values_by_id.keys(), expected_values_by_id.keys())

    # Ensure that all the rows match one by one
    for id_ in expected_values_by_id.keys():
        assert actual_values_by_id[id_] == expected_values_by_id[id_], (actual_values_by_id[id_], expected_values_by_id[id_])


@step('limpio la cache de las queries de base de datos')
def clear_database_query_cache(context):
    reset_queries()


@step('veo que se han hecho "([0-9]+)" queries en base de datos(?: ignorando)?')
def confirm_num_database_queries(context, expected_queries):
    """
    Allows the user to get an accurate count of the amount of queries ran.

    The user can also choose to exclude certain queries that are not wanted in the query count.
    Some examples are:

        # A basic example that ensures that a total of 5 queries ran:
        Then I see that "5" queries were made on the database

        # A more advanced example that ensures that only 7 queries were counted if we IGNORED all queries that
        # started with "SET search_path" OR contained "SAVEPOINT" (these are case insensitive comparisons)
        # There is no limit to the number of rows. Any row that succeeds will eliminate the query.
        # The query column must contain a string
        method that takes one arg.
        # The value column is the value of the arg to pass into the string method.
        Then I see that "7" queries were made on the database while ignoring:
          | method     | value           |
          | startswith | SET search_path |
          | startswith | SET schema      |
          | contains   | savepoint       |
    """
    if getattr(settings, 'SKIP_QUERY_COUNT', False):
        return True
    included_queries = []
    excluded_queries = []
    values_by_method = defaultdict(list)  # key: str method that takes one arg. values: list of args to try 1 at a time
    if getattr(context, 'table'):
        for row in context.table.rows:
            values_by_method[row['metodo']].append(row['valor'].lower())

    for query in connection.queries:
        # Nothing needs to be excluded, just add everything and exit early.
        if not values_by_method:
            included_queries.append(query)
            continue

        include = True
        for method, values in values_by_method.items():
            # If there are any hits, this "query" needs to be ignored
            if hasattr(operator, method):
                # Operators include "contains"
                if any([getattr(operator, method)(query['sql'].lower(), value) for value in values]):
                    include = False
                    break
            else:
                # This gets the specified method name from the str class and evaluates it with one arg: value
                # Example: method='startswith'. 'select * from table'.startswith(value)
                if any([getattr(query['sql'].lower(), method)(value) for value in values]):
                    include = False
                    break

        # Add query to included or excluded list.
        if include:
            included_queries.append(query)
        else:
            excluded_queries.append(query)

    def print_issue():
        pretty_included_queries = '\n'.join([f'{q["sql"]} ({q["time"]})' for q in included_queries])
        pretty_excluded_queries = '\n'.join([f'{q["sql"]} ({q["time"]})' for q in excluded_queries])
        return f'\n{len(included_queries)}\n'\
               f'\n{pretty_included_queries}\n\n' \
               f'------ Excluded queries not in the count ----\n\n{pretty_excluded_queries}'

    assert int(expected_queries) == len(included_queries), print_issue()


class adjust_searchpath_for_model:
    """
    There are some instances when dealing with public tables where the search path must contain only the public schema
    so that we don't unintentionally hit the table with the same name in the tenant schema. ContentType is an example of
    this as it table exists in both the public and tenant schemas. The tables can have very different Django ids for the
    same natural key, so we must use the correct table in the public schema when the model with an FK to ContentType is
    in the public schema. If we don't remove the tenant schema from the search path, the ContentType table in the tenant
    schema will be used as the tenant schema always comes before the public schema when the search path is set.

    Also when manipulating rows in tenant.Member, django-tenant-schemas REQUIRES that only the public schema be on the
    connection's search path. Both the save() and delete() methods will raise an exception if the search path contains
    more than just the public schema. Remove the current tenant schema when entering this context and put it back when
    leaving.
    """

    def __init__(self, Model):
        full_app_path = apps.get_app_config(Model._meta.app_label).name
        self.is_public_model = full_app_path in settings.INSTALLED_APPS

    def __enter__(self):
        if self.is_public_model:
            self.context = schema_context(get_public_schema_name())
            self.context.__enter__()

    def __exit__(self, type, value, traceback):
        if self.is_public_model:
            self.context.__exit__(type, value, traceback)


@step('(limpio y )?asigno los siguientes permisos al usuario con username "([^"]+)"')
def insert_to_db(context, limpio, username):
    user = UserModel.objects.get(username=username)
    if limpio:
        user.user_permissions.all().delete()

    for row in context.table:
        filters = dict(zip(context.table.headings, row))
        user.user_permissions.add(models.Permission.objects.get(**filters))


@step('(limpio y )?asigno los siguientes grupos al usuario con username "([^"]+)"')
def insert_to_db(context, limpio, username):
    user = UserModel.objects.get(username=username)
    if limpio:
        user.groups.all().delete()

    for row in context.table:
        filters = row.as_dict()
        user.groups.add(models.Group.objects.get(**filters))


@step('inserto un file con nombre "([^\"]+)" en el campo "([^\"]+)" del modelo "([^\"]+)" identificado por "([^\"]+)"')
def insert_to_db(context, filename, field, model, filter_fields):
    """
    Y inserto un file con nombre "test.py" en el campo "adjunto" del modelo "permiso.PeticionPermiso" identificado por "id=1"
    """
    Model = get_model(model)
    # The fields that identify each row are separated by a comma.
    filter_fields = dict(x.split("=") for x in filter_fields.split(","))
    instance = Model.objects.get(**filter_fields)
    getattr(instance, field).save(filename, ContentFile(b'asdf'))
