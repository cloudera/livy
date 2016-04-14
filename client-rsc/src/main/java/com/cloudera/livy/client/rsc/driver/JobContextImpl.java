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

package com.cloudera.livy.client.rsc.driver;

import java.io.File;

import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkState;

import com.cloudera.livy.JobContext;

class JobContextImpl implements JobContext {

  private static final Logger LOG = LoggerFactory.getLogger(JobContextImpl.class);

  private final JavaSparkContext sc;
  private final File localTmpDir;
  private volatile SQLContext sqlctx;
  private volatile HiveContext hivectx;
  private volatile JavaStreamingContext streamingctx;

  public JobContextImpl(JavaSparkContext sc, File localTmpDir) {
    this.sc = sc;
    this.localTmpDir = localTmpDir;
  }

  @Override
  public JavaSparkContext sc() {
    return sc;
  }

  @Override
  public SQLContext sqlctx() {
    if (sqlctx == null) {
      synchronized (this) {
        if (sqlctx == null) {
          sqlctx = new SQLContext(sc);
        }
      }
    }
    return sqlctx;
  }

  @Override
  public HiveContext hivectx() {
    if (hivectx == null) {
      synchronized (this) {
        if (hivectx == null) {
          hivectx = new HiveContext(sc.sc());
        }
      }
    }
    return hivectx;
  }

  @Override
  public synchronized JavaStreamingContext streamingctx(){
    checkState(streamingctx != null, "method createStreamingContext must be called first.");
    return streamingctx;
  }

  @Override
  public synchronized void createStreamingContext(long batchDuration) {
    checkState(streamingctx == null, "Streaming context is not null.");
    streamingctx = new JavaStreamingContext(sc, new Duration(batchDuration));
  }

  @Override
  public synchronized void stopStreamingCtx() {
    checkState(streamingctx != null, "Streaming Context is null");
    streamingctx.stop();
    streamingctx = null;
  }

  @Override
  public File getLocalTmpDir() {
    return localTmpDir;
  }

  public synchronized void stop() {
    if (streamingctx != null) {
      stopStreamingCtx();
    }
    if (sc != null) {
      sc.stop();
    }
  }

}
