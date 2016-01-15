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

import com.google.common.base.Throwables;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.MetricsCollection;
import com.cloudera.livy.client.local.rpc.RpcDispatcher;

public class BypassJobStatus {

  public final JobHandle.State state;
  public final byte[] result;
  public final String error;
  public final MetricsCollection metrics;

  public BypassJobStatus(JobHandle.State state, byte[] result, String error,
      MetricsCollection metrics) {
    this.state = state;
    this.result = result;
    this.error = error;
    this.metrics = metrics;
  }

  BypassJobStatus() {
    this(null, null, null, null);
  }

}
