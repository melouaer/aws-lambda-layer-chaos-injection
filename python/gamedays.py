from __future__ import division, unicode_literals

import sys

sys.path.insert(0, "/opt/python/.vendor")

from ssm_cache import SSMParameter
from ssm_cache.cache import InvalidParameterError
from functools import wraps, partial
import os
import subprocess
import logging
import time
import random
import json
import requests


logger = logging.getLogger(__name__)


def get_config(config_key):
    """
Retrieve the configuration from the SSM parameter store
The config always returns a tuple (value, rate)
value: requested configuration
rate: the injection probability (default 1 --> 100%)

How to use::

    >>> import os
    >>> from chaos_lib import get_config
    >>> os.environ['FAILURE_INJECTION_PARAM'] = 'chaoslambda.config'
    >>> get_config('delay')
    (400, 1)
    >>> get_config('exception_msg')
    ('I really failed seriously', 1)
    >>> get_config('error_code')
    (404, 1)
    """
    param = SSMParameter(os.environ["FAILURE_INJECTION_PARAM"])
    try:
        value = json.loads(param.value)
        if not value["isEnabled"]:
            return 0, 0
        return value[config_key], value.get("rate", 1)
    except InvalidParameterError as ex:
        # key does not exist in SSM
        raise InvalidParameterError("{} is not a valid SSM config".format(ex))
    except KeyError as ex:
        # not a valid Key in the SSM configuration
        raise KeyError("key {} not valid or found in SSM config".format(ex))


def gamedays_scenario1(func=None, delay=None):
    """
Add delay to the lambda function - delay is returned from the SSM paramater
using ``get_config('delay')`` which returns a tuple delay, rate.

Default use::

    >>> @gamedays_scenario1
    ... def handler(event, context):
    ...    return {
    ...       'statusCode': 200,
    ...       'body': 'Hello from Lambda!'
    ...    }
    >>> handler('foo', 'bar')
    Injecting 400 of delay with a rate of 1
    Added 402.20ms to handler
    {'statusCode': 200, 'body': 'Hello from Lambda!'}

With argument::

    >>> @gamedays_scenario1(delay=1000)
    ... def handler(event, context):
    ...    return {
    ...       'statusCode': 200,
    ...       'body': 'Hello from Lambda!'
    ...    }
    >>> handler('foo', 'bar')
    Injecting 1000 of delay with a rate of 1
    Added 1002.20ms to handler
    {'statusCode': 200, 'body': 'Hello from Lambda!'}

    """
    if not func:
        return partial(corrupt_delay, delay=delay)

    @wraps(func)
    def wrapper(*args, **kwargs):
        if isinstance(delay, int):
            _delay = delay
            rate = 1
        else:
            _delay, rate = get_config("delay")
            if not _delay:
                return func(*args, **kwargs)

        start = time.time()
        if _delay > 0 and rate >= 0:
            # add latency approx rate% of the time
            if round(random.random(), 5) <= rate:
                # print("Injecting {0} of delay with a rate of {1}".format(
                #     _delay, rate))
                time.sleep(_delay / 1000.0)

        end = time.time()

        # print('Added {1:.2f}ms to {0:s}'.format(
        #     func.__name__,
        #     (end - start) * 1000
        # ))
        return func(*args, **kwargs)

    return wrapper


def gamedays_scenario2_1(func=None, exception_type=None, exception_msg=None):
    """
Forces the lambda function to fail and raise an exception
using ``get_config('exception_msg')`` which returns a tuple exception_msg, rate.

Default use (Error type is Exception)::

    >>> @gamedays_scenario2_1
    ... def handler(event, context):
    ...     return {
    ...        'statusCode': 200,
    ...        'body': 'Hello from Lambda!'
    ...     }
    >>> handler('foo', 'bar')
    Injecting exception_type <class "Exception"> with message I really failed seriously a rate of 1
    corrupting now
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "/.../chaos_lambda.py", line 316, in wrapper
            raise _exception_type(_exception_msg)
    Exception: I really failed seriously

With Error type argument::

    >>> @gamedays_scenario2_1(exception_type=ValueError)
    ... def lambda_handler_with_exception_arg_2(event, context):
    ...     return {
    ...         'statusCode': 200,
    ...         'body': 'Hello from Lambda!'
    ...     }
    >>> lambda_handler_with_exception_arg_2('foo', 'bar')
    Injecting exception_type <class 'ValueError'> with message I really failed seriously a rate of 1
    corrupting now
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "/.../chaos_lambda.py", line 316, in wrapper
            raise _exception_type(_exception_msg)
    ValueError: I really failed seriously

With Error type and message argument::

    >>> @gamedays_scenario2_1(exception_type=TypeError, exception_msg='foobar')
    ... def lambda_handler_with_exception_arg(event, context):
    ...     return {
    ...         'statusCode': 200,
    ...         'body': 'Hello from Lambda!'
    ...     }
    >>> lambda_handler_with_exception_arg('foo', 'bar')
    Injecting exception_type <class 'TypeError'> with message foobar a rate of 1
    corrupting now
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "/.../chaos_lambda.py", line 316, in wrapper
            raise _exception_type(_exception_msg)
    TypeError: foobar

    """
    if not func:
        return partial(
            corrupt_exception,
            exception_type=exception_type,
            exception_msg=exception_msg,
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        _is_enabled, _ = get_config("isEnabled")
        if not _is_enabled:
            return func(*args, **kwargs)

        rate = 1
        if isinstance(exception_type, type):
            _exception_type = exception_type
        else:
            _exception_type = Exception

        if exception_msg:
            _exception_msg = exception_msg
        else:
            _exception_msg, rate = get_config("exception_msg")

            _exception_type,
            _exception_msg,
            rate
        # add injection approx rate% of the time
        if round(random.random(), 5) <= rate:
            raise _exception_type(_exception_msg)

        return func(*args, **kwargs)

    return wrapper


def gamedays_scenario3(func=None, error_code=None):
    """
Forces the lambda function to return with a specific Status Code
using ``get_config('error_code')`` which returns a tuple error_code, rate.

Default use::

    >>> @gamedays_scenario3
    ... def handler(event, context):
    ...    return {
    ...       'statusCode': 200,
    ...       'body': 'Hello from Lambda!'
    ...    }
    >>> handler('foo', 'bar')
    Injecting Error 404 at a rate of 1
    corrupting now
    {'statusCode': 404, 'body': 'Hello from Lambda!'}

With argument::

    >>> @gamedays_scenario3(error_code=400)
    ... def lambda_handler_with_statuscode_arg(event, context):
    ...     return {
    ...         'statusCode': 200,
    ...         'body': 'Hello from Lambda!'
    ...     }
    >>> lambda_handler_with_statuscode_arg('foo', 'bar')
    Injecting Error 400 at a rate of 1
    corrupting now
    {'statusCode': 400, 'body': 'Hello from Lambda!'}
    """
    if not func:
        return partial(corrupt_statuscode, error_code=error_code)

    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(error_code, int):
            _error_code = error_code
            rate = 1
        else:
            _error_code, rate = get_config("error_code")
        # print("Injecting Error {0} at a rate of {1}".format(_error_code, rate))
        # add injection approx rate% of the time
        if round(random.random(), 5) <= rate:
            # result['statusCode'] = _error_code
            return ["Lambda operation {}", _error_code]

        return result

    return wrapper


def gamedays_scenario2_2(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        file_size, rate = get_config("file_size")
        if not file_size:
            return result
        # add injection approx rate% of the time
        if random.random() <= rate:
            o = subprocess.check_output(
                [
                    "fallocate",
                    "-l",
                    str(file_size) + "M",
                    "/tmp/corrupt-diskspace-" + str(time.time()) + ".tmp",
                ],
                stderr=subprocess.STDOUT,
            )
            return result
        else:
            return result

    return wrapper


class SessionWithDelay(requests.Session):
    """
    This is a class for injecting delay to 3rd party dependencies.
    Subclassing the requests library is useful if you want to conduct other chaos experiments
    within the library, like error injection or requests modification.
    This is a simple subclassing of the parent class requests.Session to add delay to the request method.

    Usage::

       >>> from chaos_lambda import SessionWithDelay
       >>> def dummy():
       ...     session = SessionWithDelay(delay=300)
       ...     session.get('https://stackoverflow.com/')
       ...     pass
       >>> dummy()
       Added 300.00ms of delay to GET

    """

    def __init__(self, delay=None):
        super(SessionWithDelay, self).__init__()
        self.delay = delay

    def request(self, method, url, **kwargs):
        print("Added {1:.2f}ms of delay to {0:s}".format(method, self.delay))
        time.sleep(self.delay / 1000.0)
        return super(SessionWithDelay, self).request(method, url, **kwargs)
