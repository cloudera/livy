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
import base64
import json
import time
from urlparse import urlparse
import requests
import cloudpickle
import pytest
import httplib
from flaky import flaky

global session_id, job_id
session_id = None
job_id = None

livy_end_point = os.environ.get("LIVY_END_POINT")
add_file_url = os.environ.get("ADD_FILE_URL")
add_pyfile_url = os.environ.get("ADD_PYFILE_URL")
upload_file_url = os.environ.get("UPLOAD_FILE_URL")
upload_pyfile_url = os.environ.get("UPLOAD_PYFILE_URL")


def process_job(job, expected_result):
    global job_id

    pickled_job = cloudpickle.dumps(job)
    base64_pickled_job = base64.b64encode(pickled_job).decode('utf-8')
    base64_pickled_job_json = json.dumps({'job': base64_pickled_job})
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/submit-job"
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, data=base64_pickled_job_json)

    assert response.status_code == httplib.CREATED
    job_id = response.json()['id']

    poll_time = 0.1
    max_poll_time = 10
    poll_response = None
    while (poll_response is None or poll_response.json()['state'] == 'STARTED') and poll_time < \
            max_poll_time:
        time.sleep(poll_time)
        poll_request_uri = livy_end_point + "/sessions/" + str(session_id) + \
                           "/jobs/" + str(job_id)
        poll_header = {'X-Requested-By': 'livy'}
        poll_response = requests.request('GET', poll_request_uri, headers=poll_header)
        poll_time *= 2

    assert poll_response.status_code == httplib.OK
    assert poll_response.json()['id'] == job_id
    result = poll_response.json()['result']
    b64_decoded = base64.b64decode(result)
    b64_decoded_decoded = base64.b64decode(b64_decoded)
    deserialized_object = cloudpickle.loads(b64_decoded_decoded)
    assert deserialized_object == expected_result


def delay_rerun(*args):
    time.sleep(8)
    return True


def stop_session():
    global session_id

    request_url = livy_end_point + "/sessions/" + str(session_id)
    headers = {'X-Requested-By': 'livy'}
    response = requests.request('DELETE', request_url, headers=headers)
    assert response.status_code == httplib.OK


def test_create_session():
    global session_id

    request_url = livy_end_point + "/sessions"
    uri = urlparse(request_url)
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    json_data = json.dumps({'kind': 'pyspark', 'conf': {'livy.uri': uri.geturl()}})
    response = requests.request('POST', request_url, headers=header, data=json_data)

    assert response.status_code == httplib.CREATED
    session_id = response.json()['id']


@flaky(max_runs=6, rerun_filter=delay_rerun)
def test_wait_for_session_to_become_idle():
    request_url = livy_end_point + "/sessions/" + str(session_id)
    header = {'X-Requested-By': 'livy'}
    response = requests.request('GET', request_url, headers=header)
    assert response.status_code == httplib.OK
    session_state = response.json()['state']

    assert session_state == 'idle'


def test_spark_job():
    def simple_spark_job(context):
        elements = [10, 20, 30]
        sc = context.sc
        return sc.parallelize(elements, 2).count()

    process_job(simple_spark_job, 3)


def test_add_file():
    add_file_name = os.path.basename(add_file_url)
    json_data = json.dumps({'uri': add_file_url})
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/add-file"
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, data=json_data)

    assert response.status_code == httplib.OK

    def add_file_job(context):
        from pyspark import SparkFiles
        with open(SparkFiles.get(add_file_name)) as testFile:
            file_val = testFile.readline()
        return file_val

    process_job(add_file_job, "hello from addfile")


def test_add_pyfile():
    add_pyfile_name_with_ext = os.path.basename(add_pyfile_url)
    add_pyfile_name = add_pyfile_name_with_ext.rsplit('.', 1)[0]
    json_data = json.dumps({'uri': add_pyfile_url})
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/add-pyfile"
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    response_add_pyfile = requests.request('POST', request_url, headers=header, data=json_data)

    assert response_add_pyfile.status_code == httplib.OK

    def add_pyfile_job(context):
       pyfile_module = __import__ (add_pyfile_name)
       return pyfile_module.test_add_pyfile()

    process_job(add_pyfile_job, "hello from addpyfile")


def test_upload_file():
    upload_file = open(upload_file_url)
    upload_file_name = os.path.basename(upload_file.name)
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/upload-file"
    files = {'file': upload_file}
    header = {'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, files=files)

    assert response.status_code == httplib.OK

    def upload_file_job(context):
        from pyspark import SparkFiles
        with open(SparkFiles.get(upload_file_name)) as testFile:
            file_val = testFile.readline()
        return file_val

    process_job(upload_file_job, "hello from uploadfile")


def test_upload_pyfile():
    upload_pyfile = open(upload_pyfile_url)
    upload_pyfile_name_with_ext = os.path.basename(upload_pyfile.name)
    upload_pyfile_name = upload_pyfile_name_with_ext.rsplit('.', 1)[0]
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/upload-pyfile"
    files = {'file': upload_pyfile}
    header = {'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, files=files)
    assert response.status_code == httplib.OK

    def upload_pyfile_job(context):
        pyfile_module = __import__ (upload_pyfile_name)
        return pyfile_module.test_upload_pyfile()
    process_job(upload_pyfile_job, "hello from uploadpyfile")


if __name__ == '__main__':
    test_dir_path = os.getcwd() + "/src"
    value = pytest.main(test_dir_path)
    if value != 0:
        raise Exception("One or more test cases have failed.")
