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
from abc import ABCMeta, abstractproperty, abstractmethod


class JobContext:
    """
    An abstract class that holds runtime information about the job execution
    context.

    An instance of this class is kept on the node hosting a remote Spark
    context and is made available to jobs being executed via
    RemoteSparkContext#submit().

    """

    __metaclass__ = ABCMeta

    @abstractproperty
    def sc(self):
        """
        The shared SparkContext instance.

        Returns
        -------
        sc : pyspark.context.SparkContext
            A SparkContext instance

        Examples
         -------

        >>> def simple_spark_job(context):
        >>>     elements = [10, 20, 30]
        >>>     sc = context.sc
        >>>     return sc.parallelize(elements, 2).count()
        """
        pass

    @abstractproperty
    def sql_ctx(self):
        """
        The shared SQLContext instance.

        Returns
        -------
        sql_ctx : pyspark.sql.SQLContext
            A SQLContext instance

        Examples
         -------

        >>> def simple_spark_sql_job(context):
        >>>     sql_ctx = context.sql_ctx
        >>>     df1 = sql_ctx.read.json("/sample.json")
        >>>     return df1.dTypes()
        """
        pass

    @abstractproperty
    def hive_ctx(self):
        """
        The shared HiveContext instance.

        Returns
        -------
        hive_ctx : pyspark.sql.HiveContext
            A HiveContext instance

        Examples
         -------

        >>> def simple_spark_hive_job(context):
        >>>     hive_ctx = context.hive_ctx
        >>>     df1 = hive_ctx.read.json("/sample.json")
        >>>     return df1.dTypes()
        """
        pass

    @abstractproperty
    def streaming_ctx(self):
        """
        The shared SparkStreamingContext instance that has already been created

        Returns
        -------
        streaming_ctx : pyspark.streaming.StreamingContext
            A StreamingContext instance

        Raises
        -------
        ValueError
            If the streaming_ctx is not already created using the function
            create_streaming_ctx(batch_duration)

        Examples
         -------

        >>> def simple_spark_streaming_job(context):
        >>>     context.create_streaming_ctx(30)
        >>>     streaming_ctx = context.streaming_ctx
        >>>     lines = streaming_ctx.socketTextStream('localhost', '8080')
        >>>     filtered_lines = lines.filter(lambda line: 'spark' in line)
        >>>     filtered_lines.pprint()
        """
        pass

    @abstractmethod
    def create_streaming_ctx(self, batch_duration):
        """
        Creates the SparkStreaming context.

        Raises
        -------
        ValueError
            If the streaming_ctx has already been initialized

        Examples
        -------
        See usage in JobContext.streaming_ctx

        """
        pass

    @abstractmethod
    def stop_streaming_ctx(self):
        """
        Stops the SparkStreaming context.
        """
        pass

    @abstractproperty
    def local_tmp_dir_path(self):
        """"
        Returns
        -------
        local_tmp_dir_path : string
            Returns a local tmp dir path specific to the context
        """
        pass

    @abstractproperty
    def spark_session(self):
        """
        The shared SparkSession instance.

        Returns
        -------
        sc : pyspark.sql.SparkSession
            A SparkSession instance

        Examples
         -------

        >>> def simple_spark_job(context):
        >>>     session = context.spark_session
        >>>     df1 = session.read.json('/sample.json')
        >>>     return df1.dTypes()
        """
        pass
