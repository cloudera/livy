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

from ConfigParser import SafeConfigParser
from StringIO import StringIO
import os
import codecs

"""A http based client for submitting Spark-based jobs to a Livy backend."""
class HttpClient(object):

    __CONFIG_SECTION = 'env'
    __LIVY_CLIENT_CONF_DIR = "LIVY_CLIENT_CONF_DIR"

    def __init__(self, uri, load_defaults=True, conf_dict = dict()):
        """
         Loads the provided config and creates a http-based client
         :param uri: Livy server uri generated from urlparse lib
         :type uri: ParseResult generated from urlparse lib

         >>> from urlparse import urlparse
         >>> uri = urlparse("http://test")

         :param boolean load_defaults: Default value is true. If true, loads the
             livy client configuration from the files 'livy-client.conf' and
             'spark-defaults.conf'. The path to the files should be defined in the
             'LIVY_CLIENT_CONF_DIR' environment variable
         :param dict: Default value is empty dict. The key-value pairs in the dict
             will be loaded to the config
         :returns An instance of thsi class
        """
        self.__config = SafeConfigParser()
        self.__load_config(load_defaults, conf_dict)
        self.__set_uri(uri)

    def submit(self, job):
        """
        Submits a job for execution to the spark cluster.
        :param job: A function that has JobContext as the parameter. Spark jobs can
            be created with the help of JobContext, which exposes the Spark libraries
        :type job: A named or anonymous function that has type JobContext as the parameter
        :returns A future to monitor the status of the job
        """
        pass

    def run(self, job):
        """
        Asks the remote context to run a job immediately.

        Normally, the remote context will queue jobs and execute them based on how many worker
        threads have been configured. This method will run the submitted job in the same thread
        processing the RPC message, so that queueing does not apply.

        It's recommended that this method only be used to run code that finishes quickly. This
        avoids interfering with the normal operation of the context.
        :param job: A function that has JobContext as the parameter. Spark jobs can
            be created with the help of JobContext, which exposes the Spark libraries
        :type job: A named or anonymous function that has type JobContext as the parameter
        :returns A future to monitor the status of the job
        """
        pass

    def add_file(self, file_uri):
        """
        Adds a file to the running remote context.

        Note that the URL should be reachable by the Spark driver process. If running the driver
        in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
        on that node (and not on the client machine).

        :param file_uri: Representation of the path to a file using the filesystem encoding
        :type file_uri: PurePath from pathlib or pathlib2(preferred)

        >>> import pathlib2
        >>> pathlib2.Path("/test.txt").asuri()

        :returns A future to monitor the status of the job
        """
        pass

    def add_pyfile(self, file_uri):
        """
        Adds a .py or .zip to the running remote context.

        Note that the URL should be reachable by the Spark driver process. If running the driver
        in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
        on that node (and not on the client machine).

        :param file_uri: Representation of the path to a file using the filesystem encoding
        :type file_uri: PurePath from pathlib or pathlib2(preferred)

        >>> import pathlib2
        >>> pathlib2.Path("/test.txt").asuri()

        :returns A future to monitor the status of the job
        """
        pass

    def upload_file(self, open_file):
        """
        Upload a file to be passed to the Spark application
        :param file open_file: The local file to be uploaded
        :returns A future that can be used to monitor this operation
        """
        pass

    def upload_pyfile(self, open_file):
        """
        Upload a .py or .zip dependency to be passed to the Spark application
        :param file open_file: The local file to be uploaded
        :returns A future that can be used to monitor this operation
        """
        pass

    def __set_uri(self, uri):
        if uri is not None and ('http' == uri.scheme or 'https' == uri.scheme):
            self.__config.set(self.__CONFIG_SECTION, 'livy.uri', uri.geturl())
        else:
            url_exception = uri.geturl if uri is not None else None
            raise Exception('Cannot create client - Uri not supported - ', url_exception)

    def __set_conf(self, key, value):
        if value is not None:
            self.__config.set(self.__CONFIG_SECTION, key, value)
        else:
            self.__config.remove_option('env', key)

    def __set_multiple_conf(self, conf_dict):
        if conf_dict is not None:
            for key, value in conf_dict.iteritems():
                self.set_conf(self.__CONFIG_SECTION, key, value)

    def __load_config(self, load_defaults, conf_dict):
        self.__config.add_section(self.__CONFIG_SECTION)
        if load_defaults is True:
            self.__handle_default_config_loading()
        self.__set_multiple_conf(conf_dict)

    def __handle_default_config_loading(self):
        has_config_dir = os.environ.has_key(self.__LIVY_CLIENT_CONF_DIR)
        if has_config_dir:
            config_dir = os.environ.get(self.__LIVY_CLIENT_CONF_DIR)
            config_files = os.listdir(config_dir)
            default_conf_files = ['livy-client.conf', 'spark-defaults.conf']
            for config_file in config_files:
                if config_file in default_conf_files:
                    path = os.path.join(config_dir, config_file)
                    data = "[" + self.__CONFIG_SECTION + "]\n" + open(path).read()
                    self.__config.readfp(StringIO(codecs.decode(data, 'utf-8')))


