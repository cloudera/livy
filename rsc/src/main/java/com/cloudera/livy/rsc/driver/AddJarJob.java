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
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class AddJarJob implements Job<Object> {

  private final String path;

  // For serialization.
  private AddJarJob() {
    this(null);
  }

  public AddJarJob(String path) {
    this.path = path;
  }

  @Override
  public Object call(JobContext jc) throws Exception {
    File localCopyDir = new File(jc.getLocalTmpDir(), "__livy__");
    synchronized (jc) {
      if (!localCopyDir.isDirectory() && !localCopyDir.mkdir()) {
        throw new IOException("Failed to create directory for downloaded jars.");
      }
    }

    URI uri = new URI(path);
    String name = uri.getFragment() != null ? uri.getFragment() : uri.getPath();
    name = new File(name).getName();
    File localCopy = new File(localCopyDir, name);

    if (localCopy.exists()) {
      throw new IOException(String.format("A file with name %s has already been uploaded.", name));
    }

    Configuration conf = jc.sc().sc().hadoopConfiguration();
    FileSystem fs = FileSystem.get(uri, conf);
    fs.copyToLocalFile(new Path(uri), new Path(localCopy.toURI()));

    MutableClassLoader cl = (MutableClassLoader) Thread.currentThread().getContextClassLoader();
    cl.addURL(localCopy.toURI().toURL());

    jc.sc().addJar(path);
    return null;
  }

}