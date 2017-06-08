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

package com.cloudera.livy.rsc.driver;

import java.io.File;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.JobContext;
import com.cloudera.livy.rsc.RSCConf;
import com.cloudera.livy.rsc.Utils;

class JobContextImpl implements JobContext {

  private static final Logger LOG = LoggerFactory.getLogger(JobContextImpl.class);

  private final JavaSparkContext sc;
  private final File localTmpDir;
  private volatile SQLContext sqlctx;
  private volatile HiveContext hivectx;
  private volatile JavaStreamingContext streamingctx;
  private final RSCDriver driver;
  private volatile Object sparksession;

  // Map to store shared variables across different jobs.
  private final LinkedHashMap<String, Object> sharedVariables;

  public JobContextImpl(JavaSparkContext sc, File localTmpDir, RSCDriver driver) {
    this.sc = sc;
    this.localTmpDir = localTmpDir;
    this.driver = driver;
    final int retainedVariables = driver.livyConf.getInt(RSCConf.Entry.RETAINED_SHARE_VARIABLES);
    this.sharedVariables = new LinkedHashMap<String, Object>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
        return size() > retainedVariables;
      }
    };
  }

  @Override
  public JavaSparkContext sc() {
    return sc;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object sparkSession() throws Exception {
    if (sparksession == null) {
      synchronized (this) {
        if (sparksession == null) {
          try {
            Class<?> clz = Class.forName("org.apache.spark.sql.SparkSession$");
            Object spark = clz.getField("MODULE$").get(null);
            Method m = clz.getMethod("builder");
            Object builder = m.invoke(spark);
            builder.getClass().getMethod("sparkContext", SparkContext.class)
              .invoke(builder, sc.sc());
            sparksession = builder.getClass().getMethod("getOrCreate").invoke(builder);
          } catch (Exception e) {
            LOG.warn("SparkSession is not supported", e);
            throw e;
          }
        }
      }
    }

    return sparksession;
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
    Utils.checkState(streamingctx != null, "method createStreamingContext must be called first.");
    return streamingctx;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object getSharedObject(String name) throws NoSuchElementException {
    Object obj;
    synchronized (sharedVariables) {
      // Remove the entry and insert again to achieve LRU.
      obj = sharedVariables.remove(name);
      if (obj == null) {
        throw new NoSuchElementException("Cannot find shared variable named " + name);
      }
      sharedVariables.put(name, obj);
    }

    return obj;

  }

  @Override
  public void setSharedObject(String name, Object object) {
    synchronized (sharedVariables) {
      sharedVariables.put(name, object);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object removeSharedObject(String name) {
    Object obj;
    synchronized (sharedVariables) {
      obj = sharedVariables.remove(name);
    }

    return obj;
  }

  @Override
  public synchronized void createStreamingContext(long batchDuration) {
    Utils.checkState(streamingctx == null, "Streaming context is not null.");
    streamingctx = new JavaStreamingContext(sc, new Duration(batchDuration));
  }

  @Override
  public synchronized void stopStreamingCtx() {
    Utils.checkState(streamingctx != null, "Streaming Context is null");
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

  public void addFile(String path) {
    driver.addFile(path);
  }

  public void addJarOrPyFile(String path) throws Exception {
    driver.addJarOrPyFile(path);
  }
}
