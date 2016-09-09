import json
import os
import time
import sys
from functools import wraps

import dcos
import requests
import shakedown


PACKAGE_NAME = 'kafka'
WAIT_TIME_IN_SECONDS = 300

ACS_TOKEN = shakedown.run_dcos_command('config show core.dcos_acs_token')[0].strip()
DCOS_URL = shakedown.run_dcos_command('config show core.dcos_url')[0].strip()

DEFAULT_PARTITION_COUNT = 1
DEFAULT_REPLICATION_FACTOR = 1
DEFAULT_BROKER_COUNT = 3

OPTIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'options')
DYNAMIC_PORT_OPTIONS_FILE = os.path.join(OPTIONS_DIR, 'dynamic_port.json')
STATIC_PORT_OPTIONS_FILE = os.path.join(OPTIONS_DIR, 'static_port.json')

TASK_RUNNING_STATE = 'TASK_RUNNING'

REQUEST_HEADERS = {
    'authorization': 'token=%s' % ACS_TOKEN
}


def as_json(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return json.loads(fn(*args, **kwargs))
        except ValueError:
            return None

    return wrapper


def check_health():
    def fn():
        try:
            return shakedown.get_service_tasks(PACKAGE_NAME)
        except dcos.errors.DCOSHTTPException:
            return []

    def success_predicate(tasks):
        running_tasks = [t for t in tasks if t['state'] == TASK_RUNNING_STATE]
        return (
            len(running_tasks) == DEFAULT_BROKER_COUNT,
            'Service did not become healthy'
        )

    return spin(fn, success_predicate)


def get_broker_list():
    def fn():
        return get_kafka_command('broker list')

    def success_predicate(result):
        return (
            len(result) == DEFAULT_BROKER_COUNT, 'Not all brokers were revived'
        )

    return spin(fn, success_predicate)


@as_json
def get_dcos_command(command):
    result, error = shakedown.run_dcos_command(command)
    if error:
        raise RuntimeError(
            'command dcos {} {} failed'.format(command, PACKAGE_NAME)
        )

    return result


@as_json
def get_kafka_command(command):
    result, error = shakedown.run_dcos_command(
        '{} {}'.format(PACKAGE_NAME, command)
    )
    if error:
        raise RuntimeError(
            'command dcos {} {} failed'.format(command, PACKAGE_NAME)
        )

    return result


def get_kafka_config():
    response = requests.get(
        marathon_api_url('apps/kafka/versions'),
        headers=REQUEST_HEADERS
    )
    assert response.status_code == 200, 'Marathon versions request failed'

    version = response.json()['versions'][0]

    response = requests.get(
        marathon_api_url('apps/kafka/versions/%s' % version),
        headers=REQUEST_HEADERS
    )
    assert response.status_code == 200

    config = response.json()
    del config['uris']
    del config['version']

    return config


def kafka_api_url(basename):
    return '{}/v1/{}'.format(shakedown.dcos_service_url('kafka'), basename)


def marathon_api_url(basename):
    return '{}/v2/{}'.format(shakedown.dcos_service_url('marathon'), basename)


def request(request_fn, *args, **kwargs):
    def success_predicate(response):
        return (
            response.status_code == 200,
            'Request failed: %s' % response.content,
        )

    return spin(request_fn, success_predicate, *args, **kwargs)


def spin(fn, success_predicate, *args, **kwargs):
    end_time = time.time() + WAIT_TIME_IN_SECONDS
    while time.time() < end_time:
        try:
            result = fn(*args, **kwargs)
        except Exception as e:
            is_successful, error_message = False, str(e)
        else:
            is_successful, error_message = success_predicate(result)

        if is_successful:
            break
        time.sleep(1)

    assert is_successful, error_message

    return result


def uninstall():
    def fn():
        try:
            shakedown.uninstall_package_and_wait(PACKAGE_NAME, app_id=PACKAGE_NAME)
            pkg_uninstall_ok = True
        except (dcos.errors.DCOSException, ValueError) as e:
            print('Got exception when uninstalling package, continuing with janitor anyway: {}'.format(e))
            pkg_uninstall_ok = False

        return shakedown.run_command_on_master(
            'docker run mesosphere/janitor /janitor.py '
            '-r kafka-role -p kafka-principal -z dcos-service-kafka '
            '--auth_token={}'.format(
                shakedown.run_dcos_command(
                    'config show core.dcos_acs_token'
                )[0].strip()
            )
        ) and pkg_uninstall_ok

    spin(fn, lambda x: (x, 'Uninstall failed'))
