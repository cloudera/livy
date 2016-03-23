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
import java.lang.ProcessBuilder.Redirect
import java.net.{ConnectException, URI, URL}
import java.nio.file.{Files, Paths}
import java.util.{HashMap => JHashMap}
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{Future, _}
import scala.concurrent.duration.Duration

import org.apache.spark.launcher.SparkLauncher
import org.json4s._
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JsonAST.{JNull, JString}
import org.json4s.jackson.JsonMethods._

import com.cloudera.livy._
import com.cloudera.livy.client.local.{LocalClient, LocalConf}
import com.cloudera.livy.sessions._
import com.cloudera.livy.utils.SparkProcessBuilder
import com.cloudera.livy.Utils

object InteractiveSession {
  val LivyReplDriverClassPath = "livy.repl.driverClassPath"
  val LivyReplJars = "livy.repl.jars"
  val SparkSubmitPyFiles = "spark.submit.pyFiles"
  val SparkYarnIsPython = "spark.yarn.isPython"
}

class InteractiveSession(
    id: Int,
    owner: String,
    _proxyUser: Option[String],
    livyConf: LivyConf,
    request: CreateInteractiveRequest)
  extends Session(id, owner) with Logging {

  import InteractiveSession._

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global
  protected implicit def jsonFormats: Formats = DefaultFormats

  protected[this] var _state: SessionState = SessionState.Starting()

  private val client = {
    info(s"Creating LivyClient for sessionId: $id")
    val builder = new LivyClientBuilder()
      .setConf("spark.app.name", s"livy-session-$id")
      .setConf("spark.master", "yarn-cluster")
      .setURI(new URI("local:spark"))
      .setAll(Option(request.conf).map(_.asJava).getOrElse(new JHashMap()))
      .setConf("livy.client.sessionId", id.toString)
      .setConf(LocalConf.Entry.CLIENT_REPL_MODE.key(), "true")

    request.kind match {
      case PySpark() | PySpark3() =>
        val pySparkFiles = if (!LivyConf.TEST_MODE) findPySparkArchives() else Nil
        builder.setConf(SparkYarnIsPython, "true")
        builder.setConf(SparkSubmitPyFiles, (pySparkFiles ++ request.pyFiles).mkString(","))

      case _ =>
    }
    builder.setConf("session.kind", request.kind.toString)

    sys.env.get("LIVY_REPL_JAVA_OPTS").foreach { opts =>
      val userOpts = request.conf.get(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)
      val newOpts = userOpts.toSeq ++ Seq(opts)
      builder.setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, newOpts.mkString(" "))
    }

    Option(livyConf.get(LivyReplDriverClassPath)).foreach { cp =>
      val userCp = request.conf.get(SparkLauncher.DRIVER_EXTRA_CLASSPATH)
      val newCp = Seq(cp) ++ userCp.toSeq
      builder.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, newCp.mkString(File.pathSeparator))
    }

    val allJars = livyJars(livyConf) ++ request.jars

    def listToConf(lst: List[String]): Option[String] = {
      if (lst.size > 0) Some(lst.mkString(",")) else None
    }

    val userOpts: Map[Option[String], String] = Map(
      listToConf(request.archives) -> "spark.yarn.dist.archives",
      listToConf(request.files) -> "spark.files",
      listToConf(allJars) -> "spark.jars",
      request.driverCores.map(_.toString) -> "spark.driver.cores",
      request.driverMemory.map(_.toString + "b") -> SparkLauncher.DRIVER_MEMORY,
      request.executorCores.map(_.toString) -> SparkLauncher.EXECUTOR_CORES,
      request.executorMemory.map(_.toString) -> SparkLauncher.EXECUTOR_MEMORY,
      request.numExecutors.map(_.toString) -> "spark.dynamicAllocation.maxExecutors"
    )

    userOpts.foreach { case (opt, configKey) =>
      opt.foreach { value => builder.setConf(configKey, value) }
    }

    proxyUser.foreach(builder.setConf(LocalConf.Entry.PROXY_USER.key(), _))
    builder.build()
  }.asInstanceOf[LocalClient]

  // Client is ready to receive commands now.
  _state = SessionState.Idle()

  private[this] var _executedStatements = 0
  private[this] var _statements = IndexedSeq[Statement]()

  override def logLines(): IndexedSeq[String] = IndexedSeq()

  override def state: SessionState = _state

  override def stop(): Future[Unit] = {
    Future {
      _state = SessionState.ShuttingDown()
      client.stop(true)
      _state = SessionState.Dead()
    }
  }

  def kind: Kind = request.kind

  def proxyUser: Option[String] = _proxyUser

  def statements: IndexedSeq[Statement] = _statements

  def interrupt(): Future[Unit] = {
    stop()
  }

  def executeStatement(content: ExecuteRequest): Statement = {
    ensureRunning {
      _state = SessionState.Busy()
      recordActivity()

      val future = Future {
        val id = client.submitReplCode(content.code)
        waitForStatement(id)
      }

      val statement = new Statement(_executedStatements, content, future)

      _executedStatements += 1
      _statements = _statements :+ statement

      statement
    }
  }

  @tailrec
  private def waitForStatement(id: String): JValue = {
    val response = client.getReplJobResult(id).get()
    if (response != null) {
      val result = parse(response)
      // If the response errored out, it's possible it took down the interpreter. Check if
      // it's still running.
      result \ "status" match {
        case JString("error") =>
          val state = client.getReplState().get() match {
            case "error" => SessionState.Error()
            case _ => SessionState.Idle()
          }
          transition(state)
        case _ => transition(SessionState.Idle())
      }
      result
    } else {
      Thread.sleep(1000)
      waitForStatement(id)
    }
  }

  private def livyJars(livyConf: LivyConf): List[String] = {
    Option(livyConf.get(LivyReplJars)).map(_.split(",").toList).getOrElse {
      val home = sys.env("LIVY_HOME")
      val jars = Option(new File(home, "repl-jars"))
        .filter(_.isDirectory())
        .getOrElse(new File(home, "com/cloudera/livy/repl/target/jars"))
      require(jars.isDirectory(), "Cannot find Livy REPL jars.")
      jars.listFiles().map(_.getAbsolutePath()).toList
    }
  }

  private def findPySparkArchives(): Seq[String] = {
    sys.env.get("PYSPARK_ARCHIVES_PATH")
      .map(_.split(",").toSeq)
      .getOrElse {
        sys.env.get("SPARK_HOME") .map { case sparkHome =>
          val pyLibPath = Seq(sparkHome, "python", "lib").mkString(File.separator)
          val pyArchivesFile = new File(pyLibPath, "pyspark.zip")
          require(pyArchivesFile.exists(),
            "pyspark.zip not found; cannot run pyspark application in YARN mode.")

          val py4jFile = Files.newDirectoryStream(Paths.get(pyLibPath), "py4j-*-src.zip")
            .iterator()
            .next()
            .toFile

          require(py4jFile.exists(),
            "py4j-*-src.zip not found; cannot run pyspark application in YARN mode.")
          Seq(pyArchivesFile.getAbsolutePath, py4jFile.getAbsolutePath)
        }.getOrElse(Seq())
      }
  }


  private def transition(state: SessionState) = synchronized {
    _state = state
  }

  private def ensureState[A](state: SessionState, f: => A) = {
    synchronized {
      if (_state == state) {
        f
      } else {
        throw new IllegalStateException("Session is in state %s" format _state)
      }
    }
  }

  private def ensureRunning[A](f: => A) = {
    synchronized {
      _state match {
        case SessionState.Idle() | SessionState.Busy() =>
          f
        case _ =>
          throw new IllegalStateException("Session is in state %s" format _state)
      }
    }
  }

}
