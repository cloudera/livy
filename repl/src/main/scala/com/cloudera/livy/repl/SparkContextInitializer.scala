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

package com.cloudera.livy.repl

import org.apache.spark.{SparkConf, SparkContext}

import com.cloudera.livy.Logging

/**
 * A mixin trait for Spark entry point creation. This trait exists two different code path
 * separately for Spark1 and Spark2, depends on whether SparkSession exists or not.
 */
trait SparkContextInitializer extends Logging {
  self: SparkInterpreter =>

  def createSparkContext(conf: SparkConf): Unit = {
    if (isSparkSessionPresent()) {
      spark2CreateContext(conf)
    } else {
      spark1CreateContext(conf)
    }
  }

  private def spark1CreateContext(conf: SparkConf): Unit = {
    sparkContext = SparkContext.getOrCreate(conf)
    var sqlContext: Object = null

    if (conf.getBoolean("spark.repl.enableHiveContext", false)) {
      try {
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(getClass.getClassLoader)
        if (loader.getResource("hive-site.xml") == null) {
          warn("livy.repl.enable-hive-context is true but no hive-site.xml found on classpath.")
        }

        sqlContext = Class.forName("org.apache.spark.sql.hive.HiveContext")
          .getConstructor(classOf[SparkContext]).newInstance(sparkContext).asInstanceOf[Object]
        info("Created sql context (with Hive support).")
      } catch {
        case _: NoClassDefFoundError =>
          sqlContext = Class.forName("org.apache.spark.sql.SQLContext")
            .getConstructor(classOf[SparkContext]).newInstance(sparkContext).asInstanceOf[Object]
          info("Created sql context.")
      }
    } else {
      sqlContext = Class.forName("org.apache.spark.sql.SQLContext")
        .getConstructor(classOf[SparkContext]).newInstance(sparkContext).asInstanceOf[Object]
      info("Created sql context.")
    }

    bind("sc", "org.apache.spark.SparkContext", sparkContext, List("""@transient"""))
    bind("sqlContext", sqlContext.getClass.getCanonicalName, sqlContext, List("""@transient"""))

    execute("import org.apache.spark.SparkContext._")
    execute("import sqlContext.implicits._")
    execute("import sqlContext.sql")
    execute("import org.apache.spark.sql.functions._")
  }

  private def spark2CreateContext(conf: SparkConf): Unit = {
    val sparkClz = Class.forName("org.apache.spark.sql.SparkSession$")
    val sparkObj = sparkClz.getField("MODULE$").get(null)

    val builderMethod = sparkClz.getMethod("builder")
    val builder = builderMethod.invoke(sparkObj)
    builder.getClass.getMethod("config", classOf[SparkConf]).invoke(builder, conf)

    var spark: Object = null
    if (conf.get("spark.sql.catalogImplementation", "in-memory").toLowerCase == "hive") {
      if (sparkClz.getMethod("hiveClassesArePresent").invoke(sparkObj).asInstanceOf[Boolean]) {
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(getClass.getClassLoader)
        if (loader.getResource("hive-site.xml") == null) {
          warn("livy.repl.enable-hive-context is true but no hive-site.xml found on classpath.")
        }

        builder.getClass.getMethod("enableHiveSupport").invoke(builder)
        spark = builder.getClass.getMethod("getOrCreate").invoke(builder)
        info("Created Spark session (with Hive support).")
      } else {
        spark = builder.getClass.getMethod("getOrCreate").invoke(builder)
        info("Created Spark session.")
      }
    } else {
      spark = builder.getClass.getMethod("getOrCreate").invoke(builder)
      info("Created Spark session.")
    }

    sparkContext = spark.getClass.getMethod("sparkContext").invoke(spark)
      .asInstanceOf[SparkContext]

    bind("spark", spark.getClass.getCanonicalName, spark, List("""@transient"""))
    bind("sc", "org.apache.spark.SparkContext", sparkContext, List("""@transient"""))

    execute("import org.apache.spark.SparkContext._")
    execute("import spark.implicits._")
    execute("import spark.sql")
    execute("import org.apache.spark.sql.functions._")
  }

  private def isSparkSessionPresent(): Boolean = {
    try {
      Class.forName("org.apache.spark.sql.SparkSession")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }
}
