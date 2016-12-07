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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class DatasetTest implements Job<Long> {

  @Override
  public Long call(JobContext jc) throws Exception {
    SparkSession spark = jc.sparkSession();

    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Row> rdd = sc.parallelize(Arrays.asList(1, 2, 3)).map(
      new Function<Integer, Row>() {
      public Row call(Integer integer) throws Exception {
        return RowFactory.create(integer);
      }
    });
    StructType schema = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("value", DataTypes.IntegerType, false)
    });

    Dataset<Row> ds = spark.createDataFrame(rdd, schema);

    return ds.filter(new FilterFunction<Row>() {
      @Override
      public boolean call(Row row) throws Exception {
        return row.getInt(0) >= 2;
      }
    }).count();
  }
}
