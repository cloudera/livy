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

package com.cloudera.livy;

import java.io.File;
import java.util.NoSuchElementException;

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

  /** The shared SQLContext instance. */
  SQLContext sqlctx();

  /** The shared HiveContext instance. */
  HiveContext hivectx();

  /** Returns the JavaStreamingContext which has already been created. */
  JavaStreamingContext streamingctx();

  /** Get shared object */
  <E> E getSharedObject(String name) throws NoSuchElementException;

  /** Set shared object, it will replace the old one if already existed */
  <E> void setSharedObject(String name, E object);

  /** Remove shared object from cache */
  <E> E removeSharedObject(String name);

  /**
   * Creates the SparkStreaming context.
   *
   * @param batchDuration Time interval at which streaming data will be divided into batches,
   *                      in milliseconds.
   */
  void createStreamingContext(long batchDuration);

  /** Stops the SparkStreaming context. */
  void stopStreamingCtx();

  /**
   * Returns a local tmp dir specific to the context
   */
  File getLocalTmpDir();

  /**
   * Returns SparkSession if it existed, otherwise throws Exception.
   */
  <E> E sparkSession() throws Exception;
}
