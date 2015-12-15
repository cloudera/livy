/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.client.local;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.livy.client.local.counter.SparkCounters;

import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Holds runtime information about the job execution context.
 *
 * An instance of this class is kept on the node hosting a remote Spark context and is made
 * available to jobs being executed via RemoteSparkContext#submit().
 */
public interface JobContext {

  /** The shared SparkContext instance. */
  JavaSparkContext sc();

  /** The shared SQLContext inststance. */
  SQLContext sqlctx();

  /** The shared HiveContext inststance. */
  HiveContext hivectx();

  /** Returns the JavaStreamingContext which has already been created. */
  JavaStreamingContext streamingctx();

  /**
   * Creates the SparkStreaming Context using the batchDuration provided.
   *
   * @param batchDuration is time interval at which streaming data will be divided into batches
   */
  void createStreamingContext(long batchDuration);

  /** Stops the SparkStreaming Context. */
  void stopStreamingCtx();

  /**
   * Monitor a job. This allows job-related information (such as metrics) to be communicated
   * back to the client.
   *
   * @return The job (unmodified).
   */
  <T> JavaFutureAction<T> monitor(
    JavaFutureAction<T> job, SparkCounters sparkCounters, Set<Integer> cachedRDDIds);

  /**
   * Return a map from client job Id to corresponding JavaFutureActions.
   */
  Map<String, List<JavaFutureAction<?>>> getMonitoredJobs();

  /**
   * Return all added jar path which added through AddJarJob.
   */
  Set<String> getAddedJars();

  /**
   * Returns a local tmp dir specific to the context
   */
  File getLocalTmpDir();

}
