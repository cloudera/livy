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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.Function;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class FileReader implements Job<String> {

  private final boolean isResource;
  private final String fileName;

  public FileReader(String fileName, boolean isResource) {
    this.fileName = fileName;
    this.isResource = isResource;
  }

  @Override
  public String call(JobContext jc) {
    return jc.sc().parallelize(Arrays.asList(1)).map(new Reader()).collect().get(0);
  }

  private class Reader implements Function<Integer, String> {

    @Override
    public String call(Integer i) throws Exception {
      InputStream in;
      if (isResource) {
        ClassLoader ccl = Thread.currentThread().getContextClassLoader();
        in = ccl.getResourceAsStream(fileName);
        if (in == null) {
          throw new IOException("Resource not found: " + fileName);
        }
      } else {
        in = new FileInputStream(SparkFiles.get(fileName));
      }
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        int read;
        while ((read = in.read(buf)) >= 0) {
          out.write(buf, 0, read);
        }
        byte[] bytes = out.toByteArray();
        return new String(bytes, 0, bytes.length, UTF_8);
      } finally {
        in.close();
      }
    }

  }

}
