/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.test.jobs.spark2;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class SparkSessionTest implements Job<Long> {

  @Override
  public Long call(JobContext jc) throws Exception {
    // Make sure SparkSession and SparkContext is callable
    SparkSession session = jc.sparkSession();

    JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
    return sc.parallelize(Arrays.asList(1, 2, 3)).count();
  }
}
