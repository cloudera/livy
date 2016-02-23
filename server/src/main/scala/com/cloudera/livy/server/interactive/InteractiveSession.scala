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
import java.net.{ConnectException, URL}
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.concurrent.{Future, _}
import scala.concurrent.duration.Duration

import dispatch._
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JsonAST.{JNull, JString}
import org.json4s.jackson.Serialization.write

import com.cloudera.livy.{ExecuteRequest, LivyConf, Utils}
import com.cloudera.livy.sessions._
import com.cloudera.livy.sessions.interactive.Statement
import com.cloudera.livy.utils.SparkProcessBuilder

object InteractiveSession {
  val LivyReplDriverClassPath = "livy.repl.driverClassPath"
  val LivyReplJars = "livy.repl.jars"
  val LivyServerUrl = "livy.server.serverUrl"
  val SparkDriverExtraJavaOptions = "spark.driver.extraJavaOptions"
  val SparkLivyCallbackUrl = "spark.livy.callbackUrl"
  val SparkLivyPort = "spark.livy.port"
  val SparkSubmitPyFiles = "spark.submit.pyFiles"
  val SparkYarnIsPython = "spark.yarn.isPython"
}

class InteractiveSession(
    id: Int,
    owner: String,
    _proxyUser: Option[String],
    livyConf: LivyConf,
    request: CreateInteractiveRequest)
  extends Session(id, owner) {

  import InteractiveSession._

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global
  protected implicit def jsonFormats: Formats = DefaultFormats

  protected[this] var _state: SessionState = SessionState.Starting()

  private[this] var _url: Option[URL] = None

  private[this] var _executedStatements = 0
  private[this] var _statements = IndexedSeq[Statement]()

  private val process = {
    val builder = new SparkProcessBuilder(livyConf)
    builder.className("com.cloudera.livy.repl.Main")
    builder.conf(request.conf)
    request.archives.foreach(builder.archive)
    request.driverCores.foreach(builder.driverCores)
    request.driverMemory.foreach(builder.driverMemory)
    request.executorCores.foreach(builder.executorCores)
    request.executorMemory.foreach(builder.executorMemory)
    request.numExecutors.foreach(builder.numExecutors)
    request.files.foreach(builder.file)

    val jars = request.jars ++ livyJars(livyConf)
    jars.foreach(builder.jar)

    _proxyUser.foreach(builder.proxyUser)
    request.queue.foreach(builder.queue)
    request.name.foreach(builder.name)

    request.kind match {
      case PySpark() =>
        builder.conf(SparkYarnIsPython, "true", admin = true)

        // FIXME: Spark-1.4 seems to require us to manually upload the PySpark support files.
        // We should only do this for Spark 1.4.x
        val pySparkFiles = if (!LivyConf.TEST_MODE) findPySparkArchives() else Nil
        builder.files(pySparkFiles)

        // We can't actually use `builder.pyFiles`, because livy-repl is a Jar, and
        // spark-submit will reject it because it isn't a Python file. Instead we'll pass it
        // through a special property that the livy-repl will use to expose these libraries in
        // the Python shell.
        builder.files(request.pyFiles)

        builder.conf(SparkSubmitPyFiles, (pySparkFiles ++ request.pyFiles).mkString(","),
          admin = true)
      case _ =>
    }

    sys.env.get("LIVY_REPL_JAVA_OPTS").foreach { replJavaOpts =>
      val javaOpts = builder.conf(SparkDriverExtraJavaOptions) match {
        case Some(javaOptions) => f"$javaOptions $replJavaOpts"
        case None => replJavaOpts
      }
      builder.conf(SparkDriverExtraJavaOptions, javaOpts, admin = true)
    }

    Option(livyConf.get(LivyReplDriverClassPath))
      .foreach(builder.driverClassPath)

    sys.props.get(LivyServerUrl).foreach { serverUrl =>
      val callbackUrl = f"$serverUrl/sessions/$id/callback"
      builder.conf(SparkLivyCallbackUrl, callbackUrl, admin = true)
    }

    builder.conf(SparkLivyPort, "0", admin = true)

    builder.redirectOutput(Redirect.PIPE)
    builder.redirectErrorStream(true)
    builder.start(None, List(kind.toString))
  }

  private val stdoutThread = new Thread {
    override def run() = {
      val regex = """Starting livy-repl on (https?://.*)""".r

      val lines = process.inputIterator

      // Loop until we find the ip address to talk to livy-repl.
      @tailrec
      def readUntilURL(): Unit = {
        if (lines.hasNext) {
          val line = lines.next()

          line match {
            case regex(url_) => url = new URL(url_)
            case _ => readUntilURL()
          }
        }
      }

      readUntilURL()
    }
  }

  stdoutThread.setName("process session stdout reader")
  stdoutThread.setDaemon(true)
  stdoutThread.start()

  override def logLines(): IndexedSeq[String] = process.inputLines

  override def state: SessionState = _state

  override def stop(): Future[Unit] = {
    val future: Future[Unit] = synchronized {
      _state match {
        case SessionState.Idle() =>
          _state = SessionState.Busy()

          Http(svc.DELETE OK as.String).either() match {
            case (Right(_) | Left(_: ConnectException)) =>
              // Make sure to eat any connection errors because the repl shut down before it sent
              // out an OK.
              synchronized {
                _state = SessionState.Dead()
              }

              Future.successful(())

            case Left(t: Throwable) =>
              Future.failed(t)
          }
        case SessionState.NotStarted() =>
          Future {
            waitForStateChange(SessionState.NotStarted(), Duration(10, TimeUnit.SECONDS))
            stop()
          }
        case SessionState.Starting() =>
          Future {
            waitForStateChange(SessionState.Starting(), Duration(10, TimeUnit.SECONDS))
            stop()
          }
        case SessionState.Busy() | SessionState.Running() =>
          Future {
            waitForStateChange(SessionState.Busy(), Duration(10, TimeUnit.SECONDS))
            stop()
          }
        case SessionState.ShuttingDown() =>
          Future {
            waitForStateChange(SessionState.ShuttingDown(), Duration(10, TimeUnit.SECONDS))
            stop()
          }
        case SessionState.Error(_) | SessionState.Dead(_) | SessionState.Success(_) =>
          if (process.isAlive) {
            Future {
              process.destroy()
            }
          } else {
            Future.successful(Unit)
          }
      }
    }

    future.andThen { case r =>
      process.waitFor()
      stdoutThread.join()
      r
    }
  }

  def kind: Kind = request.kind

  def proxyUser: Option[String] = _proxyUser

  def url: Option[URL] = _url

  def url_=(url: URL): Unit = {
    ensureState(SessionState.Starting(), {
      _state = SessionState.Idle()
      _url = Some(url)
    })
  }

  def statements: IndexedSeq[Statement] = _statements

  def interrupt(): Future[Unit] = {
    stop()
  }

  def executeStatement(content: ExecuteRequest): Statement = {
    ensureRunning {
      _state = SessionState.Busy()
      recordActivity()

      val req = (svc / "execute").setContentType("application/json", "UTF-8") << write(content)

      val future = Http(req OK as.json4s.Json).map { case resp: JValue =>
        parseResponse(resp).getOrElse {
          // The result isn't ready yet. Loop until it is.
          val id = (resp \ "id").extract[Int]
          waitForStatement(id)
        }
      }

      val statement = new Statement(_executedStatements, content, future)

      _executedStatements += 1
      _statements = _statements :+ statement

      statement
    }
  }

  def waitForStateChange(oldState: SessionState, atMost: Duration): Unit = {
    Utils.waitUntil({ () => state != oldState }, atMost)
  }

  private def svc = {
    val url = _url.head
    dispatch.url(url.toString)
  }

  @tailrec
  private def waitForStatement(id: Int): JValue = {
    val req = (svc / "history" / id).setContentType("application/json", "UTF-8")
    val resp = Await.result(Http(req OK as.json4s.Json), Duration.Inf)

    parseResponse(resp) match {
      case Some(result) => result
      case None =>
        Thread.sleep(1000)
        waitForStatement(id)
    }
  }

  private def parseResponse(response: JValue): Option[JValue] = {
    response \ "result" match {
      case JNull => None
      case result =>
        // If the response errored out, it's possible it took down the interpreter. Check if
        // it's still running.
        result \ "status" match {
          case JString("error") =>
            if (replErroredOut()) {
              transition(SessionState.Error())
            } else {
              transition(SessionState.Idle())
            }
          case _ => transition(SessionState.Idle())
        }

        Some(result)
    }
  }

  private def replErroredOut() = {
    val req = svc.setContentType("application/json", "UTF-8")
    val response = Await.result(Http(req OK as.json4s.Json), Duration.Inf)

    response \ "state" match {
      case JString("error") => true
      case _ => false
    }
  }

  private def livyJars(livyConf: LivyConf): Seq[String] = {
    Option(livyConf.get(LivyReplJars)).map(_.split(",").toSeq).getOrElse {
      val home = sys.env("LIVY_HOME")
      val jars = Option(new File(home, "repl-jars"))
        .filter(_.isDirectory())
        .getOrElse(new File(home, "repl/target/jars"))
      require(jars.isDirectory(), "Cannot find Livy REPL jars.")
      jars.listFiles().map(_.getAbsolutePath()).toSeq
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

  // Error out the job if the process errors out.
  Future {
    if (process.waitFor() == 0) {
      // Set the state to done if the session shut down before contacting us.
      _state match {
        case (SessionState.Dead(_) | SessionState.Error(_) | SessionState.Success(_)) =>
        case _ =>
          _state = SessionState.Success()
      }
    } else {
      _state = SessionState.Error()
    }
  }
}
