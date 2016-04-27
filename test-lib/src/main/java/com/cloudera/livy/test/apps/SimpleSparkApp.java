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

package com.cloudera.livy.test.apps;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

public class SimpleSparkApp {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Missing output path.");
    }
    String output = args[0];

    JavaSparkContext sc = new JavaSparkContext();
    try {
      List<String> data = Arrays.asList("the", "quick", "brown", "fox", "jumped", "over", "the",
        "lazy", "dog");

      JavaPairRDD<String, Integer> rdd = sc.parallelize(data, 3)
        .mapToPair(new Counter());
      rdd.saveAsTextFile(output);
    } finally {
      sc.close();
    }
  }

  private static class Counter implements PairFunction<String, String, Integer> {

    @Override
    public Tuple2<String, Integer> call(String s) throws Exception {
      return new Tuple2<>(s, s.length());
    }

  }

}
