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


class JobContext:
    """
    Holds runtime information about the job execution context.

    An instance of this class is kept on the node hosting a remote Spark context and is made
    available to jobs being executed via RemoteSparkContext#submit().
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def sc(self):
        """
        The shared SparkContext instance.
        :returns SparkContext instance
        """
        pass

    @abstractmethod
    def sql_ctx(self):
        """
        The shared SqlContext instance.
        :returns SqlContext instance
        """
        pass

    @abstractmethod
    def hive_ctx(self):
        """
        The shared HiveContext instance.
        :returns HiveContext instance
        """
        pass

    @abstractmethod
    def streaming_ctx(self):
        """
        The shared SparkStreamingContext instance that has already been created.
        :returns StreamingContext instance
        """
        pass

    @abstractmethod
    def create_streaming_ctx(self, batch_duration):
        """
        Creates the SparkStreaming context.
        :param seconds batch_duration Time interval at which streaming data will
            be divided into batches
        """
        pass

    @abstractmethod
    def stop_streaming_ctx(self):
        """
        Stops the SparkStreaming context.
        """
        pass

    @abstractmethod
    def get_temp_file(self):
        """"
        :returns Returns a local tmp dir specific to the context
        """
        pass

