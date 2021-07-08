import ast
import json

from behave import *
from django.template import Template, Context
from jq import jq

from .utils import extract_field_value, pretty_print_table


@step('hago las peticiones desde la url "([^"]+)"')
def set_url(context, url):
    try:
        context.http_headers['HTTP_REFERER'] = url
    except AttributeError:
        context.http_headers = {'HTTP_REFERER': url}


@step('hago un "([^"]+)" a la url "([^"]+)"(?: con los argumentos "([^"]+)")?(?: (?:y|con) los parametros "([^"]+)")?(?: (?:y|con) body)?')
def step_impl(context, method_name, url, url_args, url_params):
    """
    Hago un "get" a la url "factura"
    Hago un "get" a la url "factura" con los argumentos "id=1"
    Hago un "get" a la url "factura" con los parametros "ordering=nombre,nombre=mario"
    Hago un "get" a la url "factura" con body:
    Hago un "get" a la url "factura" con los argumentos "id=1" y body:
        | name  |
        | mario |
        | luigi |
    Hago un "get" a la url "factura" con los argumentos "id=1" y body:
        | key   | value |
        | mario | rossi |
        | luigi | verdi |
    Hago un "get" a la url "factura" con los argumentos "id=1" y body:
        '''
        { raw json }
        '''
        # Use triple double-quotes
    """
    headers = {'content_type': 'application/json'}
    try:
        headers = dict(headers, **context.http_headers)
    except AttributeError:
        pass

    data = None
    if context.text:
        data = context.text
    elif context.table:
        fields = context.table.headings
        # This is used to send a single dict as the payload
        if len(fields) == 2 and 'key' in fields and 'value' in fields:
            data = {item['key']: item['value'] for item in context.table.headings}
        else:
            data = list(context.table.headings)
    if not url.startswith('/'):
        # Hack!!
        # Could not find an easier way to parse the named url WITH parameters
        url = Template(f"{{% url '{url}' {url_args or ''} %}}").render(Context())
    if url_params:
        url = f'{url}?{"&".join([param.strip().replace(";", ",") for param in url_params.split(", ")])}'
    method = getattr(context.test.client, method_name.lower())
    context.response = method(url, data=data, **headers)


@step('configuro los headers( usando literales|)')
def add_request_headers(context, use_literals):
    # The way django settings is made, it allows for settings and headers to be passed in as one
    context.http_headers = {}
    cast = ast.literal_eval if bool(use_literals) else lambda x: x
    for item in context.table.rows:
        context.http_headers[item['name']] = cast(item['value'])


@step('el codigo de retorno es "([0-9]{3})"')
def step_impl(context, status_code):
    assert context.response.status_code == int(status_code), (context.response.status_code, context.response.content)


@step('hay "([0-9]+)" elementos en la response')
def step_impl(context, count):
    assert len(context.response.json()['results']) == int(count)


@step('(?:utilizando el formato jq "(.*)")? la response es')
def check_request_response(context, jq_format):
    """
    More details about the jq can be found at their docs: https://stedolan.github.io/jq/manual/#Basicfilters

    List within list

        [
          {"nk": "coursenk1", "prereqs_met": ["coursenkX", "coursenkY"], "prereqs_missed": ["courseidW", "courseidZ"]},
          {"nk": "coursenk2", "prereqs_met": ["coursenkA", "coursenkB"], "prereqs_missed": ["courseidC", "courseidD"]},
        ]

        # And using jq format check the expected values for prereqs_met
        And using jq format "[.[].prereqs_met[] | {value: .}]" the response is:
          | value     |
          | coursenkX |
          | coursenkY |
          | coursenkA |
          | coursenkB |

        # And using jq format check the expected values for prereqs_missed
        And using jq format "[.[].prereqs_missed[] | {value: .}]" the response is:
          | value     |
          | courseidW |
          | courseidZ |
          | courseidC |
          | courseidD |

    List within list within dict

        {
          "courses":
            [
              {"nk": "coursenk1", "prereqs_met": ["coursenkX", "coursenkY"],
               "prereqs_missed": ["courseidW", "courseidZ"]},
              {"nk": "coursenk2", "prereqs_met": ["coursenkA", "coursenkB"],
               "prereqs_missed": ["courseidC", "courseidD"]},
            ],
          "units": 16
        }

        # And using jq format check the expected values for prereqs_met
        And using jq format "[.courses[].prereqs_met[] | {value: .}]" the response is:
          | value     |
          | coursenkX |
          | coursenkY |
          | coursenkA |
          | coursenkB |

        # And using jq format check the expected values for prereqs_missed
        And using jq format "[.courses[].prereqs_missed[] | {value: .}]" the response is:
          | value     |
          | courseidW |
          | courseidZ |
          | courseidC |
          | courseidD |

    Dict within dict

        {
          "courses":
            {
              "MATH101": {"nk": "MATH101", "title": "Algebra"},
              "MATH202": {"nk": "MATH202", "title": "Linear Algebra"},
            },
          "hobbies":
            {
              "Swimming": {"nk": "Swimming", "area": "Outdoors"},
              "Ping Pong": {"nk": "Ping Pong", "area": "Indoors"},
            },
        }

        # And using jq format check the expected values for hobby nk & course nk
        And using jq format "[.[][].nk | {value: .}]" the response is:
          | value     |
          | MATH101   |
          | MATH202   |
          | Swimming  |
          | Ping Pong |

        # And using jq format check the expected values for course nk
        And using jq format "[.courses[].nk | {value: .}]" the response is:
          | value     |
          | Swimming  |
          | Ping Pong |

    """
    is_json = context.response.get('Content-Type') == 'application/json'
    if is_json:
        actual = context.response.json()
        if jq_format:
            # Before trying to debug the lines below, why not test it at this website: https://jqplay.org/
            # It's simpler than adding breakpoints and print statements everywhere...
            # Also, the docs are here: https://stedolan.github.io/jq/manual/#Basicfilters
            try:
                actual = jq(jq_format).transform(actual)
            except StopIteration:
                # jq returns this if nothing was returned
                actual = None
    else:
        actual = context.response.content.decode()

    if context.table:
        fields = context.table.headings
        if isinstance(actual, dict):
            actual = [actual]
        cleaned_data = [{field: str(extract_field_value(item, field)) for field in fields} for item in actual]
        expected_data = [{field: item[field] for field in fields} for item in context.table.rows]
        context.test.assertEquals(cleaned_data, expected_data, pretty_print_table(fields, cleaned_data))
    elif context.text is not None:
        if is_json:
            expected = json.loads(context.text)
        else:
            # Remove all leading whitespace and new line chars from multiline before comparing.
            # Remove whitespace between dict key & value.
            lines = context.text.split('\n')
            lines_without_leading_whitespace = [line.lstrip() for line in lines]
            one_str_of_all_lines = ''.join(lines_without_leading_whitespace)
            without_space_after_colon = one_str_of_all_lines.replace(': ', ':')
            expected = without_space_after_colon
            actual = actual.replace(': ', ':')
        context.test.assertEquals.__self__.maxDiff = None
        context.test.assertEquals(actual, expected)
    else:
        raise Exception('Nothing to compare')


@step('la respuesta contiene los siguientes headers')
def check_headers(context):
    for row in context.table.rows:
        assert row['key'] in context.response.headers, (f'key {row["key"]} not found', context.response.headers.keys())
        assert context.response.headers[row['key']] == row['value'], (context.response.headers[row['key']], row['value'])
