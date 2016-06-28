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

from abc import ABCMeta, abstractmethod

"""A client for submitting Spark-based jobs to a Livy backend."""
class Client:
    __metaclass__ = ABCMeta

    @abstractmethod
    def submit(self, job):
        """
        Submits a job for execution to the spark cluster.
        :param job: A function that has JobContext as the parameter. Spark jobs can
            be created with the help of JobContext, which exposes the Spark libraries
        :type job: A named or anonymous function that has type JobContext as the parameter
        :returns A future to monitor the status of the job
        """
        pass

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
    def upload_file(self, open_file):
        """
        Upload a file to be passed to the Spark application
        :param file open_file: The local file to be uploaded
        :returns A future that can be used to monitor this operation
        """
        pass

    @abstractmethod
    def upload_pyfile(self, open_file):
        """
        Upload a .py or .zip dependency to be passed to the Spark application
        :param file open_file: The local file to be uploaded
        :returns A future that can be used to monitor this operation
        """
        pass
