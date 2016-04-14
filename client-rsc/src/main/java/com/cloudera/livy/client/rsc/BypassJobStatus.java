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

package com.cloudera.livy.client.rsc;

import java.util.List;

import com.cloudera.livy.JobHandle;

public class BypassJobStatus {

  public final JobHandle.State state;
  public final byte[] result;
  public final String error;

  public BypassJobStatus(JobHandle.State state, byte[] result, String error) {
    this.state = state;
    this.result = result;
    this.error = error;
  }

  BypassJobStatus() {
    this(null, null, null);
  }

}
