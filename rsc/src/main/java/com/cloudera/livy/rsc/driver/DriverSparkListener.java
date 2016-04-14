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

import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.spark.JavaSparkListener;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

class DriverSparkListener extends JavaSparkListener {

  private final Map<Integer, Integer> stageToJobId = Maps.newHashMap();
  private final RSCDriver driver;

  DriverSparkListener(RSCDriver driver) {
    this.driver = driver;
  }

  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    synchronized (stageToJobId) {
      for (int i = 0; i < jobStart.stageIds().length(); i++) {
        stageToJobId.put((Integer) jobStart.stageIds().apply(i), jobStart.jobId());
      }
    }
  }

  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    synchronized (stageToJobId) {
      for (Iterator<Map.Entry<Integer, Integer>> it = stageToJobId.entrySet().iterator();
          it.hasNext();) {
        Map.Entry<Integer, Integer> e = it.next();
        if (e.getValue() == jobEnd.jobId()) {
          it.remove();
        }
      }
    }

    JobWrapper<?> job = getWrapper(jobEnd.jobId());
    if (job != null) {
      job.jobDone();
    }
  }

  /**
   * Returns the client job ID for the given Spark job ID.
   *
   * This will only work for jobs monitored via JobContext#monitor(). Other jobs won't be
   * matched, and this method will return `None`.
   */
  private JobWrapper<?> getWrapper(Integer sparkJobId) {
    for (JobWrapper<?> job : driver.activeJobs.values()) {
      if (job.hasSparkJobId(sparkJobId)) {
        return job;
      }
    }
    return null;
  }

}
