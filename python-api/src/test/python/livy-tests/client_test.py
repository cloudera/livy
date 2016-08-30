#
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import pytest
import responses
import socket
import threading
from configparser import NoOptionError
from livy.client import HttpClient

session_id = 0
job_id = 1
base_uri = 'http://{}:{}'.format(socket.gethostname(), 8998)
client_test = None
invoked_queued_callback = False
invoked_running_callback = False


@responses.activate
def mock_and_validate_create_new_session(defaults):
    global client_test
    create_session_request_mock_uri = base_uri + "/sessions/"
    app_name = 'Test App'
    conf_dict = {'spark.app.name': app_name}
    json_data = {
        u'kind': u'pyspark', u'log': [], u'proxyUser': None,
        u'state': u'starting', u'owner': None, u'id': session_id
    }
    responses.add(responses.POST, create_session_request_mock_uri,
        json=json_data, status=201, content_type='application/json')
    client_test = HttpClient(base_uri, conf_dict=conf_dict,
        load_defaults=defaults)
    assert client_test._config.get(client_test._CONFIG_SECTION,
        'spark.app.name') == app_name
    if defaults:
        assert client_test._config.has_option(client_test._CONFIG_SECTION,
            'spark.config')
        assert client_test._config.get(client_test._CONFIG_SECTION,
            'spark.config') == 'override'


def mock_submit_job_and_poll_result(
    job,
    job_state,
    result=None,
    error=None
):
    submit_request_mock_uri = base_uri + "/sessions/" + str(session_id) \
        + "/submit-job"
    poll_request_mock_uri = base_uri + "/sessions/" + str(session_id) \
        + "/jobs/" + str(job_id)

    post_json_data = {
        u'state': u'SENT', u'error': None, u'id': job_id, u'result': None
    }
    responses.add(responses.POST, submit_request_mock_uri, status=201,
        json=post_json_data, content_type='application/json')

    get_json_data = {
        u'state': job_state, u'error': error, u'id': job_id, u'result': result
    }
    responses.add(responses.GET, poll_request_mock_uri, status=200,
        json=get_json_data, content_type='application/json')

    submit_job_future = client_test.submit(job)
    return submit_job_future


def mock_file_apis(job_command, job_func, job_func_arg):
    request_uri = base_uri + "/sessions/" + str(session_id) + \
        "/" + job_command
    responses.add(responses.POST, request_uri, status=201, body='',
        content_type='application/json')
    test_file_api_future = job_func(job_func_arg)
    return test_file_api_future


def simple_spark_job(context):
    elements = [10, 20, 30]
    sc = context.sc
    return sc.parallelize(elements, 2).count()


def failure_job(context):
    return "hello" + 1


def test_create_new_session_without_default_config():
    mock_and_validate_create_new_session(False)


def test_create_new_session_with_default_config():
    os.environ["LIVY_CLIENT_CONF_DIR"] = \
        os.path.dirname(os.path.abspath(__file__)) + "/resources"
    mock_and_validate_create_new_session(True)


def test_connect_to_existing_session():
    reconnect_mock_request_uri = base_uri + "/sessions/" + str(session_id) + \
        "/connect"
    reconnect_session_uri = base_uri + "/sessions/" + str(session_id)
    json_data = {
        u'kind': u'pyspark', u'log': [], u'proxyUser': None,
        u'state': u'starting', u'owner': None, u'id': session_id
    }
    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, reconnect_mock_request_uri, json=json_data,
            status=201, content_type='application/json')

        client_reconnect = HttpClient(reconnect_session_uri,
            load_defaults=False)

    assert isinstance(client_reconnect, HttpClient)
    with pytest.raises(NoOptionError):
        client_reconnect._config.get(client_reconnect._CONFIG_SECTION,
            'spark.app.name') == 'Test App'


@responses.activate
def test_submit_job_verify_running_state():
    submit_job_future = mock_submit_job_and_poll_result(simple_spark_job,
        u'STARTED')
    lock = threading.Event()

    def handle_job_running_callback(f):
        global invoked_running_callback
        invoked_running_callback = f.running()
        lock.set()
    submit_job_future.add_running_callback(handle_job_running_callback)
    lock.wait(15)
    assert invoked_running_callback


@responses.activate
def test_submit_job_verify_queued_state():
    submit_job_future = mock_submit_job_and_poll_result(simple_spark_job,
        u'QUEUED')
    lock = threading.Event()

    def handle_job_queued_callback(f):
        global invoked_queued_callback
        invoked_queued_callback = f.queued()
        lock.set()
    submit_job_future.add_queued_callback(handle_job_queued_callback)
    lock.wait(15)
    assert invoked_queued_callback


@responses.activate
def test_submit_job_verify_succeeded_state():
    submit_job_future = mock_submit_job_and_poll_result(simple_spark_job,
        u'SUCCEEDED',
        result='Z0FKVkZGc3hNREFzSURJd01Dd2dNekF3TENBME1EQmRjUUF1')
    result = submit_job_future.result(15)
    assert result == '[100, 200, 300, 400]'


@responses.activate
def test_submit_job_verify_failed_state():
    submit_job_future = mock_submit_job_and_poll_result(failure_job, u'FAILED',
        error='Error job')
    exception = submit_job_future.exception(15)
    assert isinstance(exception, Exception)


@responses.activate
def test_add_file():
    file_uri = "file://" + os.path.dirname(os.path.abspath(__file__)) + \
        "/resources/text_file.txt"
    add_file_future = mock_file_apis('add-file', client_test.add_file,
         file_uri)
    add_file_future.result(15)
    assert add_file_future.done()


@responses.activate
def test_upload_file():
    file_path = os.path.dirname(os.path.abspath(__file__)) + \
        "/resources/text_file.txt"
    upload_file_future = mock_file_apis('upload-file', client_test.upload_file,
        file_path)
    upload_file_future.result(15)
    assert upload_file_future.done()


@responses.activate
def test_add_pyfile():
    file_uri = "file://" + os.path.dirname(os.path.abspath(__file__)) + \
        "/resources/zip_file.zip"
    add_file_future = mock_file_apis('add-pyfile', client_test.add_pyfile,
         file_uri)
    add_file_future.result(15)
    assert add_file_future.done()


@responses.activate
def test_upload_pyfile():
    file_path = os.path.dirname(os.path.abspath(__file__)) + \
        "/resources/zip_file.zip"
    pyfile_future = mock_file_apis('upload-pyfile', client_test.upload_pyfile,
        file_path)
    pyfile_future.result(15)
    assert pyfile_future.done()


@responses.activate
def test_add_jar():
    file_uri = "file://" + os.path.dirname(os.path.abspath(__file__)) + \
        "/resources/jar_file.jar"
    add_file_future = mock_file_apis('add-jar', client_test.add_jar, file_uri)
    add_file_future.result(15)
    assert add_file_future.done()
