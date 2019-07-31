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

package com.cloudera.livy.examples;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.cloudera.livy.*;

class PiJob implements
    Job<Double>,
    Function<Integer, Integer>,
    Function2<Integer, Integer, Integer> {

  private final int slices;
  private final int samples;

  public PiJob(int slices) {
    this.slices = slices;
    this.samples = (int) Math.min(100000L * slices, Integer.MAX_VALUE);
  }

  @Override
  public Double call(JobContext ctx) throws Exception {
    List<Integer> sampleList = new ArrayList<>();
    for (int i = 0; i < samples; i++) {
      sampleList.add(i);
    }

    return 4.0d * ctx.sc().parallelize(sampleList, slices).map(this).reduce(this) / samples;
  }

  @Override
  public Integer call(Integer v1) {
    double x = Math.random() * 2 - 1;
    double y = Math.random() * 2 - 1;
    return (x * x + y * y < 1) ? 1 : 0;
  }

  @Override
  public Integer call(Integer v1, Integer v2) {
    return v1 + v2;
  }
}

/**
 * Example execution:
 * java -cp /pathTo/spark-core_2.10-*version*.jar:/pathTo/livy-api-*version*.jar:
 * /pathTo/livy-client-http-*version*.jar:/pathTo/livy-examples-*version*.jar
 * com.cloudera.livy.examples.PiApp http://livy-host:8998 2
 */
public class PiApp {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: PiJob <livy url> <slices>");
      System.exit(-1);
    }

    LivyClient client = new LivyClientBuilder()
      .setURI(new URI(args[0]))
      .build();

    try {
      System.out.println("Uploading livy-example jar to the SparkContext...");
      for (String s : System.getProperty("java.class.path").split(File.pathSeparator)) {
        if (new File(s).getName().startsWith("livy-examples")) {
          client.uploadJar(new File(s)).get();
          break;
        }
      }

      final int slices = Integer.parseInt(args[1]);
      double pi = client.submit(new PiJob(slices)).get();

      System.out.println("Pi is roughly " + pi);
    } finally {
      client.stop(true);
    }
  }
}

