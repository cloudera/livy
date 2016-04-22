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

package com.cloudera.livy.test.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class SmallCount implements Job<Long> {

  private final int count;

  public SmallCount(int count) {
    this.count = count;
  }

  @Override
  public Long call(JobContext jc) {
    Random r = new Random();
    int partitions = Math.min(r.nextInt(10) + 1, count);

    List<Integer> elements = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      elements.add(r.nextInt());
    }

    return jc.sc().parallelize(elements, partitions).count();
  }

}
