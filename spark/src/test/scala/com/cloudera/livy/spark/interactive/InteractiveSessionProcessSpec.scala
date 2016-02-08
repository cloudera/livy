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

package com.cloudera.livy.spark.interactive

import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.{BaseInteractiveSessionSpec, PySpark}
import com.cloudera.livy.sessions.interactive.InteractiveSession
import com.cloudera.livy.spark.SparkProcessBuilderFactory

class InteractiveSessionProcessSpec extends BaseInteractiveSessionSpec {

  val livyConf = new LivyConf()
  livyConf.set("livy.repl.driverClassPath", sys.props("java.class.path"))
  livyConf.set(InteractiveSessionFactory.LivyReplJars, "")

  def createSession(): InteractiveSession = {
    assume(sys.env.get("SPARK_HOME").isDefined, "SPARK_HOME is not set.")
    val processFactory = new SparkProcessBuilderFactory(livyConf)
    val interactiveFactory = new InteractiveSessionProcessFactory(processFactory)

    val req = new CreateInteractiveRequest()
    req.kind = PySpark()
    interactiveFactory.create(0, null, req)
  }
}
