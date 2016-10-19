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
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Future, _}
import scala.util.{Failure, Random, Success, Try}

import org.apache.spark.launcher.SparkLauncher
import org.json4s._
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods._

import com.cloudera.livy._
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.rsc.{PingJob, RSCClient, RSCConf}
import com.cloudera.livy.sessions._
import com.cloudera.livy.utils.{AppInfo, LivySparkUtils, SparkApp, SparkAppListener}

object InteractiveSession {
  val LivyReplJars = "livy.repl.jars"
  val SparkYarnIsPython = "spark.yarn.isPython"
}

class InteractiveSession(
    id: Int,
    owner: String,
    override val proxyUser: Option[String],
    livyConf: LivyConf,
    request: CreateInteractiveRequest,
    mockApp: Option[SparkApp] = None) // For unit test.
  extends Session(id, owner, livyConf)
  with SparkAppListener {

  import Session._
  import InteractiveSession._

  private implicit def jsonFormats: Formats = DefaultFormats

  private var _state: SessionState = SessionState.Starting()

  private val operations = mutable.Map[Long, String]()
  private val operationCounter = new AtomicLong(0)

  val kind = request.kind

  private val (client: RSCClient, app: Option[SparkApp]) = {
    val uniqueAppTag = s"livy-session-$id-${Random.alphanumeric.take(8).mkString}"

    val conf = SparkApp.prepareSparkConf(uniqueAppTag, livyConf, prepareConf(
      request.conf, request.jars, request.files, request.archives, request.pyFiles, livyConf))

    val builderProperties = mutable.Map[String, String]()
    builderProperties ++= conf

    def mergeConfList(list: Seq[String], key: String): Unit = {
      if (list.nonEmpty) {
        builderProperties.get(key) match {
          case None =>
            builderProperties.put(key, list.mkString(","))
          case Some(oldList) =>
            val newList = (oldList :: list.toList).mkString(",")
            builderProperties.put(key, newList)
        }
      }
    }

    def mergeHiveSiteAndHiveDeps(sparkMajorVersion: Int): Unit = {
      val sparkFiles = conf.get("spark.files").map(_.split(",")).getOrElse(Array.empty[String])
      hiveSiteFile(sparkFiles, livyConf) match {
        case (_, true) =>
          debug("Enable HiveContext because hive-site.xml is found in user request.")
          mergeConfList(datanucleusJars(livyConf, sparkMajorVersion), LivyConf.SPARK_JARS)
        case (Some(file), false) =>
          debug("Enable HiveContext because hive-site.xml is found under classpath, "
            + file.getAbsolutePath)
          mergeConfList(List(file.getAbsolutePath), LivyConf.SPARK_FILES)
          mergeConfList(datanucleusJars(livyConf, sparkMajorVersion), LivyConf.SPARK_JARS)
        case (None, false) =>
          warn("Enable HiveContext but no hive-site.xml found under" +
            " classpath or user request.")
      }
    }

    kind match {
      case PySpark() | PySpark3() =>
        val pySparkFiles = if (!LivyConf.TEST_MODE) findPySparkArchives() else Nil
        mergeConfList(pySparkFiles, LivyConf.SPARK_PY_FILES)
        builderProperties.put(SparkYarnIsPython, "true")
      case SparkR() =>
        val sparkRArchive = if (!LivyConf.TEST_MODE) findSparkRArchive() else None
        sparkRArchive.foreach { archive =>
          builderProperties.put(RSCConf.Entry.SPARKR_PACKAGE.key(), archive + "#sparkr")
        }
      case _ =>
    }
    builderProperties.put(RSCConf.Entry.SESSION_KIND.key, kind.toString)

    mergeConfList(livyJars(livyConf), LivyConf.SPARK_JARS)
    val enableHiveContext = livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT)
    val sparkMajorVersion =
      LivySparkUtils.formatSparkVersion(LivySparkUtils.sparkSubmitVersion(livyConf))._1
    // pass spark.major_version to driver
    builderProperties.put("spark.major_version", sparkMajorVersion.toString)
    if (sparkMajorVersion <= 1) {
      builderProperties.put("spark.repl.enableHiveContext",
        livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT).toString)
    } else {
      val confVal = if (enableHiveContext) "hive" else "in-memory"
      builderProperties.put("spark.sql.catalogImplementation", confVal)
    }

    if (enableHiveContext) {
      mergeHiveSiteAndHiveDeps(sparkMajorVersion)
    }

    val userOpts: Map[String, Option[String]] = Map(
      "spark.driver.cores" -> request.driverCores.map(_.toString),
      SparkLauncher.DRIVER_MEMORY -> request.driverMemory.map(_.toString),
      SparkLauncher.EXECUTOR_CORES -> request.executorCores.map(_.toString),
      SparkLauncher.EXECUTOR_MEMORY -> request.executorMemory.map(_.toString),
      "spark.executor.instances" -> request.numExecutors.map(_.toString)
    )

    userOpts.foreach { case (key, opt) =>
      opt.foreach { value => builderProperties.put(key, value) }
    }

    info(s"Creating LivyClient for sessionId: $id")
    val builder = new LivyClientBuilder()
      .setAll(builderProperties.asJava)
      .setConf("spark.app.name", s"livy-session-$id")
      .setConf("livy.client.sessionId", id.toString)
      .setConf(RSCConf.Entry.DRIVER_CLASS.key(), "com.cloudera.livy.repl.ReplDriver")
      .setConf(RSCConf.Entry.PROXY_USER.key(), proxyUser.orNull)
      .setURI(new URI("rsc:/"))
    val client = builder.build().asInstanceOf[RSCClient]

    val app = mockApp.orElse {
      if (livyConf.isRunningOnYarn()) {
        // When Livy is running with YARN, SparkYarnApp can provide better YARN integration.
        // (e.g. Reflect YARN application state to session state).
        Option(SparkApp.create(uniqueAppTag, None, None, livyConf, Some(this)))
      } else {
        // When Livy is running with other cluster manager, SparkApp doesn't provide any additional
        // benefit over controlling RSCDriver using RSCClient. Don't use it.
        None
      }
    }
    (client, app)
  }

  // Send a dummy job that will return once the client is ready to be used, and set the
  // state to "idle" at that point.
  client.submit(new PingJob()).addListener(new JobHandle.Listener[Void]() {
    override def onJobQueued(job: JobHandle[Void]): Unit = { }
    override def onJobStarted(job: JobHandle[Void]): Unit = { }

    override def onJobCancelled(job: JobHandle[Void]): Unit = errorOut()

    override def onJobFailed(job: JobHandle[Void], cause: Throwable): Unit = errorOut()

    override def onJobSucceeded(job: JobHandle[Void], result: Void): Unit = {
      transition(SessionState.Idle())
    }

    private def errorOut(): Unit = {
      // Other code might call stop() to close the RPC channel. When RPC channel is closing,
      // this callback might be triggered. Check and don't call stop() to avoid nested called
      // if the session is already shutting down.
      if (_state != SessionState.ShuttingDown()) {
        transition(SessionState.Error())
        stop()
      }
    }
  })


  private[this] var _executedStatements = 0
  private[this] var _statements = IndexedSeq[Statement]()

  override def logLines(): IndexedSeq[String] = app.map(_.log()).getOrElse(IndexedSeq.empty)

  override def recoveryMetadata: RecoveryMetadata = {
    throw new NotImplementedError("TODO")
  }

  override def state: SessionState = _state

  override def stopSession(): Unit = {
    try {
      transition(SessionState.ShuttingDown())
      client.stop(true)
    } catch {
      case _: Exception =>
        app.foreach {
          warn(s"Failed to stop RSCDriver. Killing it...")
          _.kill()
        }
    } finally {
      transition(SessionState.Dead())
    }
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
    client.addFile(resolveURI(uri, livyConf)).get()
  }

  def addJar(uri: URI): Unit = {
    recordActivity()
    client.addJar(resolveURI(uri, livyConf)).get()
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
        .getOrElse(new File(home, "repl/scala-2.10/target/jars"))
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

  private def datanucleusJars(livyConf: LivyConf, sparkMajorVersion: Int): Seq[String] = {
    if (sys.env.getOrElse("LIVY_INTEGRATION_TEST", "false").toBoolean) {
      // datanucleus jars has already been in classpath in integration test
      Seq.empty
    } else {
      val sparkHome = livyConf.sparkHome().get
      val libdir = sparkMajorVersion match {
        case 1 =>
          if (new File(sparkHome, "RELEASE").isFile) {
            new File(sparkHome, "lib")
          } else {
            new File(sparkHome, "lib_managed/jars")
          }
        case 2 =>
          if (new File(sparkHome, "RELEASE").isFile) {
            new File(sparkHome, "jars")
          } else if (new File(sparkHome, "assembly/target/scala-2.11/jars").isDirectory) {
            new File(sparkHome, "assembly/target/scala-2.11/jars")
          } else {
            new File(sparkHome, "assembly/target/scala-2.10/jars")
          }
        case v => throw new RuntimeException("Unsupported spark major version:" + sparkMajorVersion)
      }
      val jars = if (!libdir.isDirectory) {
          Seq.empty[String]
        } else {
          libdir.listFiles().filter(_.getName.startsWith("datanucleus-"))
            .map(_.getAbsolutePath).toSeq
        }
      if (jars.isEmpty) {
        warn("datanucleus jars can not be found")
      }
      jars
    }
  }

  /**
   * Look for hive-site.xml (for now just ignore spark.files defined in spark-defaults.conf)
   * 1. First look for hive-site.xml in user request
   * 2. Then look for that under classpath
   * @param livyConf
   * @return  (hive-site.xml path, whether it is provided by user)
   */
  private def hiveSiteFile(sparkFiles: Array[String],
                           livyConf: LivyConf): (Option[File], Boolean) = {
    if (sparkFiles.exists(_.split("/").last == "hive-site.xml")) {
      (None, true)
    } else {
      val hiveSiteURL = getClass.getResource("/hive-site.xml")
      if (hiveSiteURL != null && hiveSiteURL.getProtocol == "file") {
        (Some(new File(hiveSiteURL.toURI)), false)
      } else {
        (None, false)
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
    // When a statement returns an error, the session should transit to error state.
    // If the session crashed because of the error, the session should instead go to dead state.
    // Since these 2 transitions are triggered by different threads, there's a race condition.
    // Make sure we won't transit from dead to error state.
    if (!_state.isInstanceOf[SessionState.Dead] || !state.isInstanceOf[SessionState.Error]) {
      debug(s"$this session state change from ${_state} to $state")
      _state = state
    }
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

  override def appIdKnown(appId: String): Unit = {
    _appId = Option(appId)
  }

  override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
    synchronized {
      debug(s"$this app state changed from $oldState to $newState")
      newState match {
        case SparkApp.State.FINISHED | SparkApp.State.KILLED | SparkApp.State.FAILED =>
          transition(SessionState.Dead())
        case _ =>
      }
    }
  }

  override def infoChanged(appInfo: AppInfo): Unit = { this.appInfo = appInfo }
}
