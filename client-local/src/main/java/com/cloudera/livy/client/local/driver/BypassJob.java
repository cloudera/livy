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

package com.cloudera.livy.client.local.driver;

import java.nio.ByteBuffer;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import com.cloudera.livy.client.common.BufferUtils;
import com.cloudera.livy.client.common.Serializer;

class BypassJob implements Job<byte[]> {

  private final Serializer serializer;
  private final byte[] serializedJob;

  BypassJob(Serializer serializer, byte[] serializedJob) {
    this.serializer = serializer;
    this.serializedJob = serializedJob;
  }

  @Override
  public byte[] call(JobContext jc) throws Exception {
    Job<?> job = (Job<?>) serializer.deserialize(ByteBuffer.wrap(serializedJob));
    Object result = job.call(jc);
    byte[] serializedResult;
    if (result != null) {
      ByteBuffer data = serializer.serialize(result);
      serializedResult = BufferUtils.toByteArray(data);
    } else {
      serializedResult = null;
    }
    return serializedResult;
  }

}
