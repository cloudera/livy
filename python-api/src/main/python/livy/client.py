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
import base64
import cloudpickle
import os
import re
import requests
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from ConfigParser import SafeConfigParser
from StringIO import StringIO
from urlparse import ParseResult, urlparse
from job_handle import JobHandle


class HttpClient(object):
    """A http based client for submitting Spark-based jobs to a Livy backend.

    Parameters
    ----------
    url_str : string
        Livy server url
    load_defaults : boolean, optional
        This parameter decides if the default config needs to be loaded
        Default is True
    conf_dict : dict, optional
        The key-value pairs in the conf_dict will be loaded to the config
        Default is None

    Examples
    --------
    Imports needed to create an instance of HttpClient
    >>> from src.main.python.livy.client import HttpClient

    1) Creates a client that is loaded with default config
       as 'load_defaults' is True by default
    >>> client = HttpClient("http://example:8998/")

    2) Creates a client that does not load default config, but loads
       config that are passed in 'config_dict'
    >>> config_dict = {'spark.app.name', 'Test App'}
    >>> client = HttpClient(
    >>>             "http://example:8998/", load_defaults=False,
    >>>             config_dict=config_dict)

    """

    _CONFIG_SECTION = 'env'
    _LIVY_CLIENT_CONF_DIR = "LIVY_CLIENT_CONF_DIR"

    def __init__(self, url, load_defaults=True, conf_dict=None):
        uri = urlparse(url)
        self._config = SafeConfigParser()
        self._load_config(load_defaults, conf_dict)
        match = re.match(r'(.*)/sessions/([0-9]+)', uri.path)
        if match:
            base = ParseResult(scheme=uri.scheme, netloc=uri.netloc,
                               path=match.group(1), params=uri.params,
                               query=uri.query, fragment=uri.fragment)
            self._set_uri(base)
            self._conn = _LivyConnection(base)
            self._session_id = int(match.group(2))
            self._reconnect_to_existing_session()
        else:
            self._set_uri(uri)
            session_conf_dict = dict(self._config.items(self._CONFIG_SECTION))
            self._conn = _LivyConnection(uri)
            self._session_id = self._create_new_session(
                session_conf_dict).json()['id']
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._stopped = False

    def submit(self, job):
        """
        Submits a job for execution to the spark cluster.

        Parameters
        ----------
        job : function
            The function must accept a single parameter, which is an instance
            of JobContext.

        Returns
        -------
        job_handle : an instance of the class JobHandle
            A handle that can be used to monitor the job

        Examples
        -------
        >>> def simple_spark_job(context):
        >>>     elements = [10, 20, 30, 40, 50]
        >>>     return context.sc.parallelize(elements, 2).count()

        >>> client.submit(simple_spark_job)

        """
        return self._send_job('submit-job', job)

    def run(self, job):
        """
        Asks the remote context to run a job immediately.

        Normally, the remote context will queue jobs and execute them based on
        how many worker threads have been configured. This method will run
        the submitted job in the same thread processing the RPC message,
        so that queueing does not apply.

        It's recommended that this method only be used to run code that
        finishes quickly. This avoids interfering with the normal operation
        of the context.

        Parameters
        ----------
        job : function
            The function must accept a single parameter, which is an instance
            of JobContext. Spark jobs can be created with the help of
            JobContext, which exposes the Spark libraries.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> def simple_job(context):
        >>>     return "hello"

        >>> client.run(simple_job)
        """
        return self._send_job("run-job", job)

    def add_file(self, file_path):
        """
        Adds a file to the running remote context.

        Note that the URL should be reachable by the Spark driver process. If
        running the driver in cluster mode, it may reside on a different
        host, meaning "file:" URLs have to exist on that node (and not on
        the client machine).

        Parameters
        ----------
        file_path : string
        Representation of the path to a local file using filesystem encoding.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.add_file("/test_add.txt")

        >>> # Example job using the file added using add_file function
        >>> def add_file_job(context):
        >>>    from pyspark import SparkFiles
        >>>    def func(iterator):
        >>>        with open(SparkFiles.get("test_add.txt")) as testFile:
        >>>        fileVal = int(testFile.readline())
        >>>        return [x * fileVal for x in iterator]
        >>>    return context.sc.parallelize([1, 2, 3, 4])
        >>>             .mapPartitions(func).collect()

        >>> client.submit(add_file_job)
        """
        return self._add_file_or_pyfile_job("add-file", file_path)

    def add_jar(self, file_path):
        """
        Adds a jar file to the running remote context.

        Note that the URL should be reachable by the Spark driver process. If
        running the driver  in cluster mode, it may reside on a different host,
        meaning "file:" URLs have to exist on that node (and not on the
        client machine).

        Parameters
        ----------
        file_path : string
            Representation of path to a local file using filesystem encoding.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.add_jar("/test_package.jar")

        """
        return self._add_file_or_pyfile_job("add-jar", file_path)

    def add_pyfile(self, file_path):
        """
        Adds a .py or .zip to the running remote context.

        Note that the URL should be reachable by the Spark driver process. If
        running the driver  in cluster mode, it may reside on a different host,
        meaning "file:" URLs have to exist on that node (and not on the
        client machine).

        Parameters
        ----------
        file_path : string
            Representation of path to a local file using filesystem encoding.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.add_pyfile("/test_package.egg")

        >>> # Example job using the file added using add_pyfile function
        >>> def add_pyfile_job(context):
        >>>    # Importing module from test_package.egg
        >>>    from test.pyfile_test import TestClass
        >>>    test_class = TestClass()
        >>>    return test_class.say_hello()

        >>> client.submit(add_pyfile_job)
        """
        return self._add_file_or_pyfile_job("add-pyfile", file_path)

    def upload_file(self, file_path):
        """
        Upload a file to be passed to the Spark application.

        Parameters
        ----------
        file_path : string
            File path of the local file to be uploaded.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.upload_file("/test_upload.txt")

        >>> # Example job using the file uploaded using upload_file function
        >>> def upload_file_job(context):
        >>>    from pyspark import SparkFiles
        >>>    def func(iterator):
        >>>        with open(SparkFiles.get("test_upload.txt")) as testFile:
        >>>        fileVal = int(testFile.readline())
        >>>        return [x * fileVal for x in iterator]
        >>>    return context.sc.parallelize([1, 2, 3, 4])
        >>>               .mapPartitions(func).collect()

        >>> client.submit(add_file_job)
        """
        return self._upload_file_or_pyfile(
            "upload-file", open(file_path, 'rb'))

    def upload_pyfile(self, file_path):
        """
        Upload a .py or .zip dependency to be passed to the Spark application.

        Parameters
        ----------
        file_path : string
            File path of the local file to be uploaded.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.upload_pyfile("/test_package.egg")

        >>> # Example job using the file uploaded using upload_pyfile function
        >>> def upload_pyfile_job(context):
        >>>    # Importing module from test_package.egg
        >>>    from test.pyfile_test import TestClass
        >>>    test_class = TestClass()
        >>>    return test_class.say_hello()

        >>> client.submit(upload_pyfile_job)
        """
        return self._upload_file_or_pyfile(
            "upload-pyfile", open(file_path, 'rb'))

    def stop(self, shutdown_context):
        """
        Stops the remote context.
        The function will return immediately and will not wait for the pending
        jobs to get completed

        Parameters
        ----------
        shutdown_context : Boolean
            Whether to shutdown the underlying Spark context. If false, the
            context will keep running and it's still possible to send commands
            to it, if the backend being used supports it.
        """
        with self.lock:
            if not self._stopped:
                self._executor.shutdown(wait=False)
                try:
                    if shutdown_context:
                        session_uri = "/" + str(self._session_id)
                        headers = {'X-Requested-By': 'livy'}
                        self._conn.send_request(
                            "DELETE", session_uri, headers=headers)
                except:
                    raise Exception(traceback.format_exc())
                self._stopped = True

    def _set_uri(self, uri):
        if uri is not None and uri.scheme in ('http', 'https'):
            self._config.set(self._CONFIG_SECTION, 'livy.uri', uri.geturl())
        else:
            url_exception = uri.geturl if uri is not None else None
            raise ValueError(
                'Cannot create client - Uri not supported - ', url_exception)

    def _set_conf(self, key, value):
        if value is not None:
            self._config.set(self._CONFIG_SECTION, key, value)
        else:
            self._delete_conf(key)

    def _delete_conf(self, key):
        self._config.remove_option(self._CONFIG_SECTION, key)

    def _set_multiple_conf(self, conf_dict):
        for key, value in conf_dict.iteritems():
            self._set_conf(key, value)

    def _load_config(self, load_defaults, conf_dict):
        self._config.add_section(self._CONFIG_SECTION)
        if load_defaults:
            self._load_default_config()
        if conf_dict is not None and len(conf_dict) > 0:
            self._set_multiple_conf(conf_dict)

    def _load_default_config(self):
        config_dir = os.environ.get(self._LIVY_CLIENT_CONF_DIR)
        if config_dir is not None:
            config_files = os.listdir(config_dir)
            default_conf_files = ['spark-defaults.conf', 'livy-client.conf']
            for default_conf_file in default_conf_files:
                if default_conf_file in config_files:
                    self._load_config_from_file(config_dir, default_conf_file)

    def _load_config_from_file(self, config_dir, config_file):
        path = os.path.join(config_dir, config_file)
        data = "[" + self._CONFIG_SECTION + "]\n" + open(path).read()
        self._config.readfp(StringIO(data.decode('utf8')))

    def _create_new_session(self, session_conf_dict):
        data = {'kind': 'pyspark', 'conf': session_conf_dict}
        response = self._conn.send_request(
            'POST', "/", headers=self._conn._JSON_HEADERS, data=data)
        return response

    def _reconnect_to_existing_session(self):
        reconnect_uri = "/" + str(self._session_id) + "/connect"
        self._conn.send_request(
            'POST', reconnect_uri, headers=self._conn._JSON_HEADERS)

    def _send_job(self, command, job):
        pickled_job = cloudpickle.dumps(job)
        base64_pickled_job = base64.b64encode(pickled_job).decode('utf-8')
        base64_pickled_job_data = {'job': base64_pickled_job}
        job_handle = JobHandle(self._conn, self._session_id, self._executor)
        job_handle._start(command, base64_pickled_job_data)
        return job_handle

    def _add_file_or_pyfile_job(self, command, file_uri):
        data = {'uri': file_uri}
        suffix_url = "/" + str(self._session_id) + "/" + command
        return self._executor.submit(
            self._add_or_upload_resource, suffix_url, data=data,
            headers=self._conn._JSON_HEADERS)

    def _upload_file_or_pyfile(self, command, open_file):
        files = {'file': open_file}
        suffix_url = "/" + str(self._session_id) + "/" + command
        return self._executor.submit(
            self._add_or_upload_resource, suffix_url, files=files)

    def _add_or_upload_resource(self, suffix_url, files=None, data=None,
                                headers={}):
        return self._conn.send_request(
            'POST', suffix_url, files=files, data=data,
            headers=headers).content


class _LivyConnection(object):

    _SESSIONS_URI = '/sessions'
    # Timeout in seconds
    _TIMEOUT = 10
    _JSON_HEADERS = {'Content-Type': 'application/json',
                     'Accept': 'application/json'}

    def __init__(self, uri):
        self._server_url_prefix = uri.geturl() + self._SESSIONS_URI
        self._requests = requests
        self.lock = threading.Lock()

    def send_request(
            self, method, suffix_url, headers={}, files=None, data=None):
        try:
            with self.lock:
                headers['X-Requested-By'] = 'livy'
                request_url = self._server_url_prefix + suffix_url
                return self._requests.request(
                    method, request_url, timeout=self._TIMEOUT,
                    headers=headers, files=files, json=data)
        finally:
            if files is not None:
                files.clear()
