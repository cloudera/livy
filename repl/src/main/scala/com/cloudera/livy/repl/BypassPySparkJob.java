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
package com.cloudera.livy.repl;

import java.nio.charset.Charset;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class BypassPySparkJob implements Job<byte[]> {

    private final byte[] serializedJob;
    private PySparkJobProcessor pySparkJobProcessor;

    public BypassPySparkJob(byte[] serializedJob, PySparkJobProcessor pySparkJobProcessor) {
        this.serializedJob = serializedJob;
        this.pySparkJobProcessor = pySparkJobProcessor;
    }

    @Override
    public byte[] call(JobContext jc) throws Exception {
        String value = pySparkJobProcessor.processByPassJob(serializedJob);
        return value != null ? value.getBytes(Charset.forName("UTF-8")) : null;
    }
}
