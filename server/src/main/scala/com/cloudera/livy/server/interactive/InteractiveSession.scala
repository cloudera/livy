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

import java.io.{File, InputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.{HashMap => JHashMap}
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Future, _}
import scala.util.{Failure, Success, Try}

import org.apache.spark.launcher.SparkLauncher
import org.json4s._
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods._

import com.cloudera.livy._
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.rsc.{PingJob, RSCClient, RSCConf}
import com.cloudera.livy.sessions._

object InteractiveSession {
  val LivyReplJars = "livy.repl.jars"
  val SparkYarnIsPython = "spark.yarn.isPython"
}

class InteractiveSession(
    id: Int,
    owner: String,
    override val proxyUser: Option[String],
    livyConf: LivyConf,
    request: CreateInteractiveRequest)
  extends Session(id, owner, livyConf) {

  import InteractiveSession._

  private implicit def jsonFormats: Formats = DefaultFormats

  private var _state: SessionState = SessionState.Starting()

  private val operations = mutable.Map[Long, String]()
  private val operationCounter = new AtomicLong(0)

  val kind = request.kind

  private val client = {
    val conf = prepareConf(request.conf, request.jars, request.files, request.archives,
      request.pyFiles)

    info(s"Creating LivyClient for sessionId: $id")
    val builder = new LivyClientBuilder()
      .setConf("spark.app.name", s"livy-session-$id")
      .setAll(conf.asJava)
      .setConf("livy.client.sessionId", id.toString)
      .setConf(RSCConf.Entry.DRIVER_CLASS.key(), "com.cloudera.livy.repl.ReplDriver")
      .setURI(new URI("rsc:/"))

    def mergeConfList(list: Seq[String], key: String): Unit = {
      if (list.nonEmpty) {
        val newList = (list ++ conf.get(key)).mkString(",")
        builder.setConf(key, newList)
      }
    }

    kind match {
      case PySpark() =>
        val pySparkFiles = if (!LivyConf.TEST_MODE) findPySparkArchives() else Nil
        mergeConfList(pySparkFiles, LivyConf.SPARK_PY_FILES)
        builder.setConf(SparkYarnIsPython, "true")
      case SparkR() =>
        val sparkRArchive = if (!LivyConf.TEST_MODE) findSparkRArchive() else None
        sparkRArchive.foreach { archive =>
          builder.setConf(RSCConf.Entry.SPARKR_PACKAGE.key(), archive)
        }
      case _ =>
    }
    builder.setConf(RSCConf.Entry.SESSION_KIND.key, kind.toString)

    mergeConfList(livyJars(livyConf), LivyConf.SPARK_JARS)

    val userOpts: Map[String, Option[String]] = Map(
      "spark.driver.cores" -> request.driverCores.map(_.toString),
      SparkLauncher.DRIVER_MEMORY -> request.driverMemory.map(_.toString + "b"),
      SparkLauncher.EXECUTOR_CORES -> request.executorCores.map(_.toString),
      SparkLauncher.EXECUTOR_MEMORY -> request.executorMemory.map(_.toString),
      "spark.dynamicAllocation.maxExecutors" -> request.numExecutors.map(_.toString)
    )

    userOpts.foreach { case (key, opt) =>
      opt.foreach { value => builder.setConf(key, value) }
    }

    builder.setConf(RSCConf.Entry.PROXY_USER.key(), proxyUser.orNull)
    builder.build()
  }.asInstanceOf[RSCClient]

  // Send a dummy job that will return once the client is ready to be used, and set the
  // state to "idle" at that point.
  client.submit(new PingJob()).addListener(new JobHandle.Listener[Void]() {
    override def onJobQueued(job: JobHandle[Void]): Unit = { }
    override def onJobStarted(job: JobHandle[Void]): Unit = { }

    override def onJobCancelled(job: JobHandle[Void]): Unit = {
      transition(SessionState.Error())
      stop()
    }

    override def onJobFailed(job: JobHandle[Void], cause: Throwable): Unit = {
      transition(SessionState.Error())
      stop()
    }

    override def onJobSucceeded(job: JobHandle[Void], result: Void): Unit = {
      transition(SessionState.Idle())
    }
  })


  private[this] var _executedStatements = 0
  private[this] var _statements = IndexedSeq[Statement]()

  override def logLines(): IndexedSeq[String] = IndexedSeq()

  override def state: SessionState = _state

  override def stopSession(): Unit = {
    transition(SessionState.ShuttingDown())
    client.stop(true)
    transition(SessionState.Dead())
  }

  def statements: IndexedSeq[Statement] = _statements

  def interrupt(): Future[Unit] = {
    stop()
  }

  def executeStatement(content: ExecuteRequest): Statement = {
    ensureRunning()
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

  def runJob(job: Array[Byte]): Long = {
    performOperation(job, true)
  }

  def submitJob(job: Array[Byte]): Long = {
    performOperation(job, false)
  }

  def addFile(fileStream: InputStream, fileName: String): Unit = {
    addFile(copyResourceToHDFS(fileStream, fileName))
  }

  def addJar(jarStream: InputStream, jarName: String): Unit = {
    addJar(copyResourceToHDFS(jarStream, jarName))
  }

  def addFile(uri: URI): Unit = {
    recordActivity()
    client.addFile(resolveURI(uri)).get()
  }

  def addJar(uri: URI): Unit = {
    recordActivity()
    client.addJar(resolveURI(uri)).get()
  }

  def jobStatus(id: Long): Any = {
    val clientJobId = operations(id)
    recordActivity()
    // TODO: don't block indefinitely?
    val status = client.getBypassJobStatus(clientJobId).get()
    new JobStatus(id, status.state, status.result, status.error)
  }

  def cancelJob(id: Long): Unit = {
    recordActivity()
    operations.remove(id).foreach { client.cancel }
  }

  @tailrec
  private def waitForStatement(id: String): JValue = {
    Try(client.getReplJobResult(id).get()) match {
      case Success(null) =>
        Thread.sleep(1000)
        waitForStatement(id)

      case Success(response) =>
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


      case Failure(err) =>
        // If any other error occurs, it probably means the session died. Transition to
        // the error state.
        transition(SessionState.Error())
        throw err
    }
  }

  private def livyJars(livyConf: LivyConf): List[String] = {
    Option(livyConf.get(LivyReplJars)).map(_.split(",").toList).getOrElse {
      val home = sys.env("LIVY_HOME")
      val jars = Option(new File(home, "repl-jars"))
        .filter(_.isDirectory())
        .getOrElse(new File(home, "repl/target/jars"))
      require(jars.isDirectory(), "Cannot find Livy REPL jars.")
      jars.listFiles().map(_.getAbsolutePath()).toList
    }
  }

  private def findSparkRArchive(): Option[String] = {
    Option(livyConf.get(RSCConf.Entry.SPARKR_PACKAGE.key())).orElse {
      sys.env.get("SPARK_HOME").map { case sparkHome =>
        val path = Seq(sparkHome, "R", "lib", "sparkr.zip").mkString(File.separator)
        val rArchivesFile = new File(path)
        require(rArchivesFile.exists(), "sparkr.zip not found; cannot run sparkr application.")
        rArchivesFile.getAbsolutePath()
      }
    }
  }

  private def findPySparkArchives(): Seq[String] = {
    Option(livyConf.get(RSCConf.Entry.PYSPARK_ARCHIVES))
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

  private def ensureRunning(): Unit = synchronized {
    _state match {
      case SessionState.Idle() | SessionState.Busy() =>
      case _ =>
        throw new IllegalStateException("Session is in state %s" format _state)
    }
  }

  private def performOperation(job: Array[Byte], sync: Boolean): Long = {
    ensureRunning()
    recordActivity()
    val future = client.bypass(ByteBuffer.wrap(job), sync)
    val opId = operationCounter.incrementAndGet()
    operations(opId) = future
    opId
   }

}
