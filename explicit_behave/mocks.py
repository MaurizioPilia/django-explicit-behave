"""
Read the docstring in class MockDict for why this file exists.
"""

# Standard library imports.
import inspect
from collections import defaultdict
from copy import deepcopy
from functools import partial, wraps
from os import environ
from types import GeneratorType

from behave import *

from .utils import convert_type


class MockDict:
    """
    Mock registry, keeps track of all the active mocks, either at the scenario level or at the feature/example level.

    The scope of a patch/mock can be for a single scenario, a single feature, or global. Unfortunately any test that
    calls patch.stopall() wipes them all out. No test has the knowledge to know that it is ok to stop absolutely
    everything else. So we must NEVER call patch.stopall().

    This means that every patch/mock must declare/register its desired scope (duration). This allows us to have exactly
    one test step that runs after every scenario to deactivate only the patches/mocks for the current scenario.
    Similarly we can have exactly one test step that runs after every feature to deactivate only the patches/mocks for
    the current feature. This allows us to honor the desired scope of each patch/mock.

    The examples below show how to do this. Note that each mock:
    1) Gives itself a unique name.
    2) Starts its mock(s).
    3) Calls the yield operation to allow the test to continue after starting its mock(s).
    4) Stops its mock(s) after the yield.
       Note that the framework will call the function when it knows the registered scope has ended. The function
       resumes at the statement after the yield, which causes the stop mock logic to run.
    5) The feature file itself specifies the scope of the mock when it invokes the mock.

    Example:

      @mocker('app_label.ThingBeingMockedHumanName')
      def unique_function_name(step_):
          mock = patch('path.to.function.or.Class')
          mock.start()

          yield

          mock.stop()

      # Inside your feature file do:
      I turn on the mock named "app_label.ThingBeingMockedHumanName" in this "scenario"


      @mocker('app.Foo')
      def unique_function_name2(step_, func1_value, func2_value):
          mock1 = patch('path.to.function1', return_value=func1_value)
          mock2 = patch('path.to.function1', return_value=func2_value)
          mock1.start()
          mock2.start()

          yield

          mock1.stop()
          mock2.stop()

      # Inside your feature file do:
      I turn on the mock named "app.Foo" in this "scenario" with arguments:
        | func1_value | func2_value |
        | op1         | op2         |


      @mocker_step(r'I start mock my custom request mock, returning status code "([^\"]+)", and body:$')
      def mock_example(step_, value):
          mock_response = Mock()
          mock_response.json = step_.multiline
          mock_response.status = value
          mock = patch('path.to.function', mock_response)
          mock.start()
          yield
          mock.stop()

       I start mock my custom request mock, returning status code "200", and body:
        '''
        {custom json}
        '''
    """

    def __init__(self):
        self.mocks = {}
        self.active_mock_by_name_by_scope = defaultdict(dict)

    def register(self, name, func):
        """Every mock must be registered with a unique name."""
        if name in self.mocks:
            raise ValueError(f'Mock "{name}" already registered, mock names must be unique')
        self.mocks[name] = func

    def stop(self, scope):
        """
        Call the same function that started the mock. That function has a yield statement, so calling it a second time
        will run the logic after the yield statement that stops the mock.
        TODO Buzzi: Explain why this "stop()" function is different from the "stop_mock()" function. Very confusing.
        """
        active_mock_by_name = self.active_mock_by_name_by_scope[scope]
        for mock_func in active_mock_by_name.values():
            try:
                mock_func()
            except StopIteration:
                # Because the logic after the yield runs without hitting another yield statement, StopIteration will be
                # raised. We expect that.
                pass

        # Now that we've stopped all mocks for this scope, remove them from this scope to show they are not active.
        active_mock_by_name.clear()

    def activate(self, scope, name, func):
        """Start a mock as long as it is not already active. You cannot start a mock twice within the same scope."""
        active_mocks = (mock for scope_ in self.active_mock_by_name_by_scope.values() for mock in scope_.keys())
        # Check that the mock told to run is not currently active
        active_current_mock = (mock for mock in active_mocks if mock == name)

        if any(active_current_mock):
            raise ValueError(f'Mock "{name}" is already active')
        self.active_mock_by_name_by_scope[scope][name] = func

    def stop_mock(self, name):
        # TODO Buzzi: Explain why this "stop()" function is different from the "stop_mock()" function. Very confusing.
        stopped = False
        for scope, active_mock_by_name in self.active_mock_by_name_by_scope.items():
            if name in active_mock_by_name:
                stopped = True
                try:
                    mock_func = active_mock_by_name.pop(name)
                    mock_func()
                except StopIteration:
                    pass
                break
        if not stopped:
            raise ValueError(f'Mock named {name} was never started, cannot be turned off')

    def run(self, scope, name, *args, **kwargs):
        """Check that a mock is registered before activating it."""
        name_ = name.format(*args, **kwargs)

        if name not in self.mocks:
            raise ValueError('Unknown mock. You must register the mock before you use it.')

        self.activate(scope, name_, self.mocks[name](*args, **kwargs))

    def mock(self, name):
        """
        Main wrapper around the functions mocked.

        Note: this is the function that becomes "mocker" decorator
        """

        def wrapper(func):
            @wraps(func)
            def wrapped(*args, **kwargs):
                """
                The wrapped function MUST return a generator, this generator will be called once to start the mock and
                called once more to stop the mocks. It is your responsibility to clean up after your functions.
                """
                # The original generator from the function, note that at this point nothing has been started
                gen = func(*args, **kwargs)
                # We ensure that what we received is indeed a generator, otherwise this won't work
                assert isinstance(gen, GeneratorType), f'Mocked named {name} MUST return a generator'
                # This is the first time we call the function, runs everything above the yield and stops.
                next(gen)
                # We then save the last part of the yield, that will have the clean up code to be run later by either
                # the stop_scenario() or the stop_feature()
                return partial(next, gen)

            # The name is will now be registered, the name MUST be unique!
            self.register(name, wrapped)

            wrapped.original = func

            return wrapped
        return wrapper

    def step(self, step_func_or_sentence, mock_name=None, scope='scenario'):
        """
        This is a wrapper around aloe.step to add our custom code.
        """
        def wrapper(func):
            name = mock_name or f'{func.__module__}.{func.__name__}'
            # Wrap the function around our normal mock() function, to handle the generator logic
            wrapped = self.mock(name)(func)

            @wraps(wrapped)
            def internal(*args, **kwargs):
                self.run(scope, name, *args, **kwargs)

            # Wrap our mock() around func, then wrap that around our mock state management, finally around the
            # standard aloe step(), this is so that we can call it like any other aloe step -- hides the complexity.
            return step(step_func_or_sentence)(internal)

        return wrapper


MOCK_REGISTRY = MockDict()

mocker = MOCK_REGISTRY.mock
mocker_step = MOCK_REGISTRY.step


@step('enciendo el mock llamado "([^\"]+)" para este "(caracteristica|escenario)"( con los argumentos)?')
def start_mocking(context, function_name, scope, receiving_args):
    """
    Starts the named mock functions.

    Examples:
      And I turn on the mock named "app.Foo" in this "feature"

      And I turn on the mock named "app.Foo" in this "scenario" with arguments:
        | named_arg_to_be_passed |
        | example                |

    """
    kwargs = {}
    if receiving_args:
        if context.table:
            assert 1 == len(context.table.rows), 'Mock objects only accept a single of arguments'
            kwargs = context.table.rows[0].as_dict()
    if scope == 'caracteristica':
        MOCK_REGISTRY.run('caracteristica', function_name, context, **kwargs)
    elif scope == 'escenario':
        MOCK_REGISTRY.run('escenario', function_name, context, **kwargs)
    else:
        raise ValueError(f'{scope} is not a valid scope')


@step('apago el mock llamado "([^\"]+)"')
def stop_mocking(step_, name):
    """
    Stops the named mock function given a name.

    If you don't do this, the mock will live for the remainder of it's scope, either feature wide or scenario wide.

    Example:

      And I turn off the mock named "app.Foo"'
    """
    MOCK_REGISTRY.stop_mock(name)


@fixture
def stop_feature_mocks(*args, **kwargs):
    """
    Turns off all the pending feature level mocks.
    """
    MOCK_REGISTRY.stop(scope='caracteristica')


@fixture
def stop_scenario_mocks(*args, **kwargs):
    """
    Turns off all the pending scenario level mocks.

    This will fire after each scenario and will fire after each example inside a scenario.
    """
    MOCK_REGISTRY.stop(scope='escenario')


@step('verifico que el mock "([^\"]+)" ha sido llamado( con los siguientes parametros)?')
def verify_mocked_call_args(context, mock_name, params):
    """
    This step function can be used to assert the parameter with which a mock was called.

    Example:
      @mocker('app_label.ThingBeingMockedHumanName')
      def unique_function_name(step_):
          world.mock_name = mock = patch('path.to.function.or.Class')
          mock.start()

          yield

          mock.stop()

      # Inside your feature file do to activate the mock.
      I turn on the mock named "app_label.ThingBeingMockedHumanName" in this "scenario"

      # To assert mocked function arguments.
      I verify that mock "mock_name" was called with the following parameters:
        | param1 | param2       |
        | 10     | {'test': []} |
        | 2      | {}           |

    """
    patch_obj = getattr(context, mock_name)
    mock_obj = getattr(patch_obj.target, patch_obj.attribute)

    if not params:
        mock_obj.assert_called_once()
        return

    context.test.assertEqual(mock_obj.call_count, len(context.table.rows))

    # Get the original function being patched.
    func = patch_obj.temp_original
    func_signature = inspect.signature(func)
    headers = context.table.headings

    # Return a generator of dict having actual argument name and value pair.
    # def x(a, b, d=1):
    #    pass
    # If x is patched and called as: x(1, 2, 5) then actual_call_args => {a: 1, b:2, d=5}
    def actual_call_args():
        for call in mock_obj.call_args_list:
            call_args = inspect.getcallargs(func, *call[0], **call[1])
            x = {}
            for header in headers:
                if header in call_args.keys():
                    x[header] = call_args[header]
            yield x

    # Generator of dict having expected argument name and value pair.
    expected_call_args = (row.as_dict() for row in context.table.rows)

    for actual, expected in zip(actual_call_args(), expected_call_args):
        for header in headers:
            # Convert expected value from string to whatever type is actual value and assert that they are equal.
            assert actual[header] == convert_type(actual[header])(expected[header]), (actual[header], convert_type(actual[header])(expected[header]))


@mocker_step('(limpio e |)mockeo las siguientes variables de entorno')
def mock_environment_variables(context, clear_environ):
    """ Mock in environment variables to use during tests. """
    old_environ = deepcopy(environ)
    if clear_environ:
        # Only the variables we account for should be there.
        environ.clear()
    for row in context.table.rows:
        # Other environment variables
        row_dict = row.as_dict()
        environ[row_dict['key']] = row_dict['value']

    yield

    # Reset the os.environ dictionary
    environ.clear()
    environ.update(old_environ)
