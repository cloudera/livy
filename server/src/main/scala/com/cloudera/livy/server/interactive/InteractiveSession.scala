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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.fs.Path
import org.apache.spark.launcher.SparkLauncher

import com.cloudera.livy._
import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.rsc.{PingJob, RSCClient, RSCConf}
import com.cloudera.livy.rsc.driver.Statement
import com.cloudera.livy.server.recovery.SessionStore
import com.cloudera.livy.sessions._
import com.cloudera.livy.sessions.Session._
import com.cloudera.livy.sessions.SessionState.Dead
import com.cloudera.livy.utils._

@JsonIgnoreProperties(ignoreUnknown = true)
case class InteractiveRecoveryMetadata(
    id: Int,
    appId: Option[String],
    appTag: String,
    kind: Kind,
    heartbeatTimeoutS: Int,
    owner: String,
    proxyUser: Option[String],
    rscDriverUri: Option[URI],
    version: Int = 1)
  extends RecoveryMetadata

object InteractiveSession extends Logging {
  val RECOVERY_SESSION_TYPE = "interactive"

  def create(
      id: Int,
      owner: String,
      proxyUser: Option[String],
      livyConf: LivyConf,
      request: CreateInteractiveRequest,
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None,
      mockClient: Option[RSCClient] = None): InteractiveSession = {
    val appTag = s"livy-session-$id-${Random.alphanumeric.take(8).mkString}"
    val sparkEnv = SparkEnvironment.getSparkEnv(livyConf, request.sparkEnv)

    val client = mockClient.orElse {
      val conf = SparkApp.prepareSparkConf(appTag, livyConf, prepareConf(
        request.conf, request.jars, request.files, request.archives, request.pyFiles, livyConf))

      val builderProperties = prepareBuilderProp(conf, request.kind, livyConf, sparkEnv)

      val userOpts: Map[String, Option[String]] = Map(
        "spark.driver.cores" -> request.driverCores.map(_.toString),
        SparkLauncher.DRIVER_MEMORY -> request.driverMemory.map(_.toString),
        SparkLauncher.EXECUTOR_CORES -> request.executorCores.map(_.toString),
        SparkLauncher.EXECUTOR_MEMORY -> request.executorMemory.map(_.toString),
        "spark.executor.instances" -> request.numExecutors.map(_.toString),
        "spark.app.name" -> request.name.map(_.toString),
        "spark.yarn.queue" -> request.queue
      )

      userOpts.foreach { case (key, opt) =>
        opt.foreach { value => builderProperties.put(key, value) }
      }

      builderProperties.getOrElseUpdate("spark.app.name", s"livy-session-$id")

      info(s"Creating Interactive session $id: [owner: $owner, request: $request]")
      val builder = new LivyClientBuilder()
        .setAll(builderProperties.asJava)
        .setConf("livy.client.session-id", id.toString)
        .setConf(RSCConf.Entry.DRIVER_CLASS.key(), "com.cloudera.livy.repl.ReplDriver")
        .setConf(RSCConf.Entry.PROXY_USER.key(), proxyUser.orNull)
        .setURI(new URI("rsc:/"))

      Option(builder.build().asInstanceOf[RSCClient])
    }

    new InteractiveSession(
      id,
      None,
      appTag,
      client,
      SessionState.Starting(),
      request.kind,
      request.heartbeatTimeoutInSecond,
      livyConf,
      owner,
      proxyUser,
      sessionStore,
      mockApp)
  }

  def recover(
      metadata: InteractiveRecoveryMetadata,
      livyConf: LivyConf,
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None,
      mockClient: Option[RSCClient] = None): InteractiveSession = {
    val client = mockClient.orElse(metadata.rscDriverUri.map { uri =>
      val builder = new LivyClientBuilder().setURI(uri)
      builder.build().asInstanceOf[RSCClient]
    })

    new InteractiveSession(
      metadata.id,
      metadata.appId,
      metadata.appTag,
      client,
      SessionState.Recovering(),
      metadata.kind,
      metadata.heartbeatTimeoutS,
      livyConf,
      metadata.owner,
      metadata.proxyUser,
      sessionStore,
      mockApp)
  }

  @VisibleForTesting
  private[interactive] def prepareBuilderProp(
    conf: Map[String, String],
    kind: Kind,
    livyConf: LivyConf,
    sparkEnv: SparkEnvironment): mutable.Map[String, String] = {

    val builderProperties = mutable.Map[String, String]()
    builderProperties ++= conf

    def livyJars(livyConf: LivyConf, scalaVersion: String): List[String] = {
      Option(livyConf.get(LivyConf.REPL_JARS)).map { jars =>
        val regex = """[\w-]+_(\d\.\d\d).*\.jar""".r
        jars.split(",").filter { name => new Path(name).getName match {
            // Filter out unmatched scala jars
            case regex(ver) => ver == scalaVersion
            // Keep all the java jars end with ".jar"
            case _ => name.endsWith(".jar")
          }
        }.toList
      }.getOrElse {
        val home = sys.env("LIVY_HOME")
        val jars = Option(new File(home, s"repl_$scalaVersion-jars"))
          .filter(_.isDirectory())
          .getOrElse(new File(home, s"repl/scala-$scalaVersion/target/jars"))
        require(jars.isDirectory(), "Cannot find Livy REPL jars.")
        jars.listFiles().map(_.getAbsolutePath()).toList
      }
    }

    /**
     * Look for hive-site.xml (for now just ignore spark.files defined in spark-defaults.conf)
     * 1. First look for hive-site.xml in user request
     * 2. Then look for that under classpath
     * @return  (hive-site.xml path, whether it is provided by user)
     */
    def hiveSiteFile(sparkFiles: Array[String]): (Option[File], Boolean) = {
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
      val sparkFiles = conf.get(SparkEnvironment.SPARK_FILES)
        .map(_.split(","))
        .getOrElse(Array.empty[String])
      hiveSiteFile(sparkFiles) match {
        case (_, true) =>
          debug("Enable HiveContext because hive-site.xml is found in user request.")
          mergeConfList(sparkEnv.datanucleusJars(), SparkEnvironment.SPARK_JARS)
        case (Some(file), false) =>
          debug("Enable HiveContext because hive-site.xml is found under classpath, "
            + file.getAbsolutePath)
          mergeConfList(List(file.getAbsolutePath), SparkEnvironment.SPARK_FILES)
          mergeConfList(sparkEnv.datanucleusJars(), SparkEnvironment.SPARK_JARS)
        case (None, false) =>
          warn("Enable HiveContext but no hive-site.xml found under" +
            " classpath or user request.")
      }
    }

    kind match {
      case PySpark() | PySpark3() =>
        val pySparkFiles = if (!LivyConf.TEST_MODE) sparkEnv.findPySparkArchives() else Nil
        mergeConfList(pySparkFiles, RSCConf.Entry.PYSPARK_ARCHIVES.key())
        builderProperties.put(SparkEnvironment.SPARK_YARN_IS_PYTHON, "true")
      case SparkR() =>
        val sparkRArchive = if (!LivyConf.TEST_MODE) Some(sparkEnv.findSparkRArchive()) else None
        sparkRArchive.foreach { archive =>
          mergeConfList(List(archive + "#sparkr"), SparkEnvironment.SPARK_ARCHIVES)
        }
      case _ =>
    }
    builderProperties.put(RSCConf.Entry.SESSION_KIND.key, kind.toString)
    builderProperties.put(RSCConf.Entry.SPARK_HOME.key, sparkEnv.sparkHome())
    builderProperties.put(RSCConf.Entry.SPARK_CONF_DIR.key, sparkEnv.sparkConfDir())

    // Set Livy.rsc.jars from livy conf to rsc conf, RSC conf will take precedence if both are set.
    Option(livyConf.get(LivyConf.RSC_JARS)).foreach(
      builderProperties.getOrElseUpdate(RSCConf.Entry.LIVY_JARS.key(), _))

    mergeConfList(livyJars(livyConf, sparkEnv.scalaVersion()), SparkEnvironment.SPARK_JARS)

    val enableHiveContext = sparkEnv.getBoolean(SparkEnvironment.ENABLE_HIVE_CONTEXT)
    // pass spark.livy.spark_major_version to driver
    val sparkMajorVersion = sparkEnv.sparkVersion()._1
    builderProperties.put(SparkEnvironment.SPARK_MAJOR_VERSION, sparkMajorVersion.toString)

    if (sparkMajorVersion <= 1) {
      builderProperties.put(SparkEnvironment.SPARK_ENABLE_HIVE_CONTEXT, enableHiveContext.toString)
    } else {
      val confVal = if (enableHiveContext) "hive" else "in-memory"
      builderProperties.put(SparkEnvironment.SPARK2_ENABLE_HIVE_CONTEXT, confVal)
    }

    if (enableHiveContext) {
      mergeHiveSiteAndHiveDeps(sparkMajorVersion)
    }

    builderProperties
  }
}

class InteractiveSession(
    id: Int,
    appIdHint: Option[String],
    appTag: String,
    client: Option[RSCClient],
    initialState: SessionState,
    val kind: Kind,
    heartbeatTimeoutS: Int,
    livyConf: LivyConf,
    owner: String,
    override val proxyUser: Option[String],
    sessionStore: SessionStore,
    mockApp: Option[SparkApp]) // For unit test.
  extends Session(id, owner, livyConf)
  with SessionHeartbeat
  with SparkAppListener {

  import InteractiveSession._

  private var serverSideState: SessionState = initialState

  override protected val heartbeatTimeout: FiniteDuration = {
    val heartbeatTimeoutInSecond = heartbeatTimeoutS
    Duration(heartbeatTimeoutInSecond, TimeUnit.SECONDS)
  }
  private val operations = mutable.Map[Long, String]()
  private val operationCounter = new AtomicLong(0)
  private var rscDriverUri: Option[URI] = None
  private var sessionLog: IndexedSeq[String] = IndexedSeq.empty
  private val sessionSaveLock = new Object()

  _appId = appIdHint
  sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
  heartbeat()

  private val app = mockApp.orElse {
    if (livyConf.isRunningOnYarn()) {
      val driverProcess = client.flatMap { c => Option(c.getDriverProcess) }
        .map(new LineBufferedProcess(_))
      // When Livy is running with YARN, SparkYarnApp can provide better YARN integration.
      // (e.g. Reflect YARN application state to session state).
      Option(SparkApp.create(appTag, appId, driverProcess, livyConf, Some(this)))
    } else {
      // When Livy is running with other cluster manager, SparkApp doesn't provide any
      // additional benefit over controlling RSCDriver using RSCClient. Don't use it.
      None
    }
  }

  if (client.isEmpty) {
    transition(Dead())
    val msg = s"Cannot recover interactive session $id because its RSCDriver URI is unknown."
    info(msg)
    sessionLog = IndexedSeq(msg)
  } else {
    val uriFuture = Future { client.get.getServerUri.get() }

    uriFuture onSuccess { case url =>
      rscDriverUri = Option(url)
      sessionSaveLock.synchronized {
        sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
      }
    }
    uriFuture onFailure { case e => warn("Fail to get rsc uri", e) }

    // Send a dummy job that will return once the client is ready to be used, and set the
    // state to "idle" at that point.
    client.get.submit(new PingJob()).addListener(new JobHandle.Listener[Void]() {
      override def onJobQueued(job: JobHandle[Void]): Unit = { }
      override def onJobStarted(job: JobHandle[Void]): Unit = { }

      override def onJobCancelled(job: JobHandle[Void]): Unit = errorOut()

      override def onJobFailed(job: JobHandle[Void], cause: Throwable): Unit = errorOut()

      override def onJobSucceeded(job: JobHandle[Void], result: Void): Unit = {
        transition(SessionState.Running())
        info(s"Interactive session $id created [appid: ${appId.orNull}, owner: $owner, proxyUser:" +
          s" $proxyUser, state: ${state.toString}, kind: ${kind.toString}, " +
          s"info: ${appInfo.asJavaMap}]")
      }

      private def errorOut(): Unit = {
        // Other code might call stop() to close the RPC channel. When RPC channel is closing,
        // this callback might be triggered. Check and don't call stop() to avoid nested called
        // if the session is already shutting down.
        if (serverSideState != SessionState.ShuttingDown()) {
          transition(SessionState.Error())
          stop()
          app.foreach { a =>
            info(s"Failed to ping RSC driver for session $id. Killing application.")
            a.kill()
          }
        }
      }
    })
  }

  override def logLines(): IndexedSeq[String] = app.map(_.log()).getOrElse(sessionLog)

  override def recoveryMetadata: RecoveryMetadata =
    InteractiveRecoveryMetadata(
      id, appId, appTag, kind, heartbeatTimeout.toSeconds.toInt, owner, proxyUser, rscDriverUri)

  override def state: SessionState = {
    if (serverSideState.isInstanceOf[SessionState.Running]) {
      // If session is in running state, return the repl state from RSCClient.
      client
        .flatMap(s => Option(s.getReplState))
        .map(SessionState(_))
        .getOrElse(SessionState.Busy()) // If repl state is unknown, assume repl is busy.
    } else {
      serverSideState
    }
  }

  override def stopSession(): Unit = {
    try {
      transition(SessionState.ShuttingDown())
      sessionStore.remove(RECOVERY_SESSION_TYPE, id)
      client.foreach { _.stop(true) }
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

  def statements: IndexedSeq[Statement] = {
    ensureActive()
    val r = client.get.getReplJobResults().get()
    r.statements.toIndexedSeq
  }

  def getStatement(stmtId: Int): Option[Statement] = {
    ensureActive()
    val r = client.get.getReplJobResults(stmtId, 1).get()
    if (r.statements.length < 1) {
      None
    } else {
      Option(r.statements(0))
    }
  }

  def interrupt(): Future[Unit] = {
    stop()
  }

  def executeStatement(content: ExecuteRequest): Statement = {
    ensureRunning()
    recordActivity()

    val id = client.get.submitReplCode(content.code).get
    client.get.getReplJobResults(id, 1).get().statements(0)
  }

  def cancelStatement(statementId: Int): Unit = {
    ensureRunning()
    recordActivity()
    client.get.cancelReplCode(statementId)
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
    ensureActive()
    recordActivity()
    client.get.addFile(resolveURI(uri, livyConf)).get()
  }

  def addJar(uri: URI): Unit = {
    ensureActive()
    recordActivity()
    client.get.addJar(resolveURI(uri, livyConf)).get()
  }

  def jobStatus(id: Long): Any = {
    ensureActive()
    val clientJobId = operations(id)
    recordActivity()
    // TODO: don't block indefinitely?
    val status = client.get.getBypassJobStatus(clientJobId).get()
    new JobStatus(id, status.state, status.result, status.error)
  }

  def cancelJob(id: Long): Unit = {
    ensureActive()
    recordActivity()
    operations.remove(id).foreach { client.get.cancel }
  }

  private def transition(newState: SessionState) = synchronized {
    // When a statement returns an error, the session should transit to error state.
    // If the session crashed because of the error, the session should instead go to dead state.
    // Since these 2 transitions are triggered by different threads, there's a race condition.
    // Make sure we won't transit from dead to error state.
    val areSameStates = serverSideState.getClass() == newState.getClass()
    val transitFromInactiveToActive = !serverSideState.isActive && newState.isActive
    if (!areSameStates && !transitFromInactiveToActive) {
      debug(s"$this session state change from ${serverSideState} to $newState")
      serverSideState = newState
    }
  }

  private def ensureActive(): Unit = synchronized {
    require(serverSideState.isActive, "Session isn't active.")
    require(client.isDefined, "Session is active but client hasn't been created.")
  }

  private def ensureRunning(): Unit = synchronized {
    serverSideState match {
      case SessionState.Running() =>
      case _ =>
        throw new IllegalStateException("Session is in state %s" format serverSideState)
    }
  }

  private def performOperation(job: Array[Byte], sync: Boolean): Long = {
    ensureActive()
    recordActivity()
    val future = client.get.bypass(ByteBuffer.wrap(job), sync)
    val opId = operationCounter.incrementAndGet()
    operations(opId) = future
    opId
   }

  override def appIdKnown(appId: String): Unit = {
    _appId = Option(appId)
    sessionSaveLock.synchronized {
      sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
    }
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
