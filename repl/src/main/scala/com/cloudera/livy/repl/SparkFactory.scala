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
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

import com.cloudera.livy.Logging


/**
  * SparkContext/SQLContext are created in this object, so that we can share the
  * SparkContext/SQLContext in one jvm.
  */
object SparkFactory extends Logging {

  private var sparkContext: SparkContext = _

  private var sqlContext: SQLContext = _

  private var conf: SparkConf = _


  def setSparkConf(conf: SparkConf): Unit = {
    this.conf = conf
  }

  def getSparkConf(): SparkConf = this.conf

  def getOrCreateSparkContext(): SparkContext = {
    synchronized {
      if (sparkContext == null) {
        sparkContext = SparkContext.getOrCreate(conf)
      }
      sparkContext
    }
  }

  def close(): Unit = {
    synchronized {
      if (sparkContext != null) {
        sparkContext.stop()
        sparkContext = null
      }
      sqlContext = null
    }
  }

  def getJavaSparkContext(): JavaSparkContext = new JavaSparkContext(getOrCreateSparkContext())

  def getOrCreateSQLContext(): SQLContext = {
    synchronized {
      if (sqlContext == null) {
        if (conf.getBoolean("spark.repl.enableHiveContext", false)) {
          try {
            val loader = Option(Thread.currentThread().getContextClassLoader)
              .getOrElse(getClass.getClassLoader)
            if (loader.getResource("hive-site.xml") == null) {
              warn("livy.repl.enableHiveContext is true but no hive-site.xml found on classpath.")
            }
            info("Created sql context (with Hive support).")
            sqlContext = new HiveContext(sparkContext)

          } catch {
            case _: java.lang.NoClassDefFoundError =>
              info("Created sql context.")
              sqlContext = new SQLContext(sparkContext)
          }
        } else {
          info("Created sql context.")
          sqlContext = new SQLContext(sparkContext)
        }
      }
      sqlContext
    }
  }
}
