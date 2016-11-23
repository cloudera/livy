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

package com.cloudera.livy.server.interactive

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.launcher.SparkLauncher

import com.cloudera.livy.LivyConf
import com.cloudera.livy.rsc.RSCConf
import com.cloudera.livy.server.BaseSessionServletSpec
import com.cloudera.livy.sessions.{Kind, SessionKindModule, Spark}

abstract class BaseInteractiveServletSpec
  extends BaseSessionServletSpec[InteractiveSession, InteractiveRecoveryMetadata] {

  mapper.registerModule(new SessionKindModule())

  protected var tempDir: File = _

  override def afterAll(): Unit = {
    super.afterAll()
    if (tempDir != null) {
      scala.util.Try(FileUtils.deleteDirectory(tempDir))
      tempDir = null
    }
  }

  override protected def createConf(): LivyConf = synchronized {
    if (tempDir == null) {
      tempDir = Files.createTempDirectory("client-test").toFile()
    }
    val dummyJar =
      Files.createTempFile(Paths.get(sys.props("java.io.tmpdir")), "dummy", "jar").toFile
    super.createConf()
      .set(LivyConf.SESSION_STAGING_DIR, tempDir.toURI().toString())
      .set(InteractiveSession.LIVY_REPL_JARS, dummyJar.getAbsolutePath)
      .set(LivyConf.LIVY_SPARK_VERSION, "1.6.0")
      .set(LivyConf.LIVY_SPARK_SCALA_VERSION, "2.10.5")
  }

  protected def createRequest(
      inProcess: Boolean = true,
      extraConf: Map[String, String] = Map(),
      kind: Kind = Spark()): CreateInteractiveRequest = {
    val classpath = sys.props("java.class.path")
    val request = new CreateInteractiveRequest()
    request.kind = kind
    request.conf = extraConf ++ Map(
      RSCConf.Entry.LIVY_JARS.key() -> "",
      RSCConf.Entry.CLIENT_IN_PROCESS.key() -> inProcess.toString,
      SparkLauncher.SPARK_MASTER -> "local",
      SparkLauncher.DRIVER_EXTRA_CLASSPATH -> classpath,
      SparkLauncher.EXECUTOR_EXTRA_CLASSPATH -> classpath
    )
    request
  }

}
