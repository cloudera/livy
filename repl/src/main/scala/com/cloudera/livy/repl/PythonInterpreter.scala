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

import java.io._
import java.lang.ProcessBuilder.Redirect
import java.lang.reflect.Proxy
import java.nio.file.{Files, Paths}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonParseException
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import py4j._
import py4j.reflection.PythonProxyHandler

import com.cloudera.livy.Logging
import com.cloudera.livy.client.common.ClientConf
import com.cloudera.livy.rsc.BaseProtocol
import com.cloudera.livy.rsc.driver.BypassJobWrapper
import com.cloudera.livy.sessions._

// scalastyle:off println
object PythonInterpreter extends Logging {

  def apply(conf: SparkConf, kind: Kind): Interpreter = {
    val pythonExec = kind match {
        case PySpark() => sys.env.getOrElse("PYSPARK_PYTHON", "python")
        case PySpark3() => sys.env.getOrElse("PYSPARK3_PYTHON", "python3")
        case _ => throw new IllegalArgumentException(s"Unknown kind: $kind")
    }

    val gatewayServer = new GatewayServer(null, 0)
    gatewayServer.start()

    val builder = new ProcessBuilder(Seq(pythonExec, createFakeShell().toString).asJava)

    val env = builder.environment()

    val pythonPath = sys.env.getOrElse("PYTHONPATH", "")
      .split(File.pathSeparator)
      .++(if (!ClientConf.TEST_MODE) findPySparkArchives() else Nil)
      .++(if (!ClientConf.TEST_MODE) findPyFiles() else Nil)

    env.put("PYSPARK_PYTHON", pythonExec)
    env.put("PYTHONPATH", pythonPath.mkString(File.pathSeparator))
    env.put("PYTHONUNBUFFERED", "YES")
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)
    env.put("SPARK_HOME", sys.env.getOrElse("SPARK_HOME", "."))
    env.put("LIVY_SPARK_MAJOR_VERSION", conf.get("spark.livy.spark_major_version", "1"))
    builder.redirectError(Redirect.PIPE)
    val process = builder.start()
    new PythonInterpreter(process, gatewayServer, kind.toString)
  }

  private def findPySparkArchives(): Seq[String] = {
    sys.env.get("PYSPARK_ARCHIVES_PATH")
      .map(_.split(",").toSeq)
      .getOrElse {
        sys.env.get("SPARK_HOME").map { sparkHome =>
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

  private def findPyFiles(): Seq[String] = {
    val pyFiles = sys.props.getOrElse("spark.submit.pyFiles", "").split(",")

    if (sys.env.getOrElse("SPARK_YARN_MODE", "") == "true") {
      // In spark mode, these files have been localized into the current directory.
      pyFiles.map { file =>
        val name = new File(file).getName
        new File(name).getAbsolutePath
      }
    } else {
      pyFiles
    }
  }

  private def createFakeShell(): File = {
    val source: InputStream = getClass.getClassLoader.getResourceAsStream("fake_shell.py")

    val file = Files.createTempFile("", "").toFile
    file.deleteOnExit()

    val sink = new FileOutputStream(file)
    val buf = new Array[Byte](1024)
    var n = source.read(buf)

    while (n > 0) {
      sink.write(buf, 0, n)
      n = source.read(buf)
    }

    source.close()
    sink.close()

    file
  }

  private def initiatePy4jCallbackGateway(server: GatewayServer): PySparkJobProcessor = {
    val f = server.getClass.getDeclaredField("gateway")
    f.setAccessible(true)
    val gateway = f.get(server).asInstanceOf[Gateway]
    val command: String = "f" + Protocol.ENTRY_POINT_OBJECT_ID + ";" +
      "com.cloudera.livy.repl.PySparkJobProcessor"
    getPythonProxy(command, gateway).asInstanceOf[PySparkJobProcessor]
  }

  // This method is a hack to get around the classLoader issues faced in py4j 0.8.2.1 for
  // dynamically adding jars to the driver. The change is to use the context classLoader instead
  // of the system classLoader when initiating a new Proxy instance
  // ISSUE - https://issues.apache.org/jira/browse/SPARK-6047
  // FIX - https://github.com/bartdag/py4j/pull/196
  private def getPythonProxy(commandPart: String, gateway: Gateway): Any = {
    val proxyString = commandPart.substring(1, commandPart.length)
    val parts = proxyString.split(";")
    val length: Int = parts.length
    val interfaces = ArrayBuffer.fill[Class[_]](length - 1){ null }
    if (length < 2) {
      throw new Py4JException("Invalid Python Proxy.")
    }
    else {
      var proxy: Int = 1
      while (proxy < length) {
        try {
          interfaces(proxy - 1) = Class.forName(parts(proxy))
          if (!interfaces(proxy - 1).isInterface) {
            throw new Py4JException("This class " + parts(proxy) +
              " is not an interface and cannot be used as a Python Proxy.")
          }
        } catch {
          case exception: ClassNotFoundException => {
            throw new Py4JException("Invalid interface name: " + parts(proxy))
          }
        }
        proxy += 1
      }

      val pythonProxyHandler = try {
        classOf[PythonProxyHandler].getConstructor(classOf[String], classOf[Gateway])
          .newInstance(parts(0), gateway)
      } catch {
        case NonFatal(e) =>
          classOf[PythonProxyHandler].getConstructor(classOf[String],
            Class.forName("py4j.CallbackClient"), classOf[Gateway])
            .newInstance(parts(0), gateway.getCallbackClient, gateway)
      }

      Proxy.newProxyInstance(Thread.currentThread.getContextClassLoader,
        interfaces.toArray, pythonProxyHandler.asInstanceOf[PythonProxyHandler])
    }
  }
}

private class PythonInterpreter(
    process: Process,
    gatewayServer: GatewayServer,
    pyKind: String)
  extends ProcessInterpreter(process)
  with Logging
{
  implicit val formats = DefaultFormats

  override def kind: String = pyKind

  private[repl] val pysparkJobProcessor =
    PythonInterpreter.initiatePy4jCallbackGateway(gatewayServer)

  override def close(): Unit = {
    try {
      super.close()
    } finally {
      gatewayServer.shutdown()
    }
  }

  @tailrec
  final override protected def waitUntilReady(): Unit = {
    val READY_REGEX = "READY\\(port=([0-9]+)\\)".r
    stdout.readLine() match {
      case null =>
      case READY_REGEX(port) => updatePythonGatewayPort(port.toInt)
      case _ => waitUntilReady()
    }
  }

  override protected def sendExecuteRequest(code: String): Interpreter.ExecuteResponse = {
    sendRequest(Map("msg_type" -> "execute_request", "content" -> Map("code" -> code))) match {
      case Some(response) =>
        assert((response \ "msg_type").extract[String] == "execute_reply")

        val content = response \ "content"

        (content \ "status").extract[String] match {
          case "ok" =>
            Interpreter.ExecuteSuccess((content \ "data").extract[JObject])
          case "error" =>
            val ename = (content \ "ename").extract[String]
            val evalue = (content \ "evalue").extract[String]
            val traceback = (content \ "traceback").extract[Seq[String]]

            Interpreter.ExecuteError(ename, evalue, traceback)
          case status =>
            Interpreter.ExecuteError("Internal Error", f"Unknown status $status")
        }
      case None =>
        Interpreter.ExecuteAborted(takeErrorLines())
    }
  }

  override protected def sendShutdownRequest(): Unit = {
    sendRequest(Map(
      "msg_type" -> "shutdown_request",
      "content" -> ()
    )).foreach { case rep =>
      warn(f"process failed to shut down while returning $rep")
    }
  }

  private def sendRequest(request: Map[String, Any]): Option[JValue] = {
    stdin.println(write(request))
    stdin.flush()

    Option(stdout.readLine()).map { case line =>
      try {
        parse(line)
      }
      catch {
        // LIVY-322 - If a statement puts raw text in the stdout without fake_shell parsing it,
        // retry the readLine up to 100 times in an attempt to find the next parsable line of JSON.
        case e: JsonParseException => retryRead(100)
      }
    }
  }

  // LIVY-322 - Method for retrying reads from stdout if a JsonParseException is encountered
  // due to non-JSON formatted text in the stdout. Accepts a param of the number of times
  // to retry the read, and recurses until either it finds a parsable line of JSON, or
  // exhausts the maximum level of retries.  If the maximum number of retries is exhausted,
  // an Exception is thrown.
  private def retryRead(maxRetries: Int): JValue = {
    if (maxRetries > 0) {
      try {
         parse(stdout.readLine())
      } catch {
        case e: JsonParseException =>
          retryRead(maxRetries - 1)
      }
    }
    else {
      throw new Exception(
         "Livy is unable to find valid JSON in the response from fake_shell. " +
         "Please recreate the session.")
    }
  }

  def addFile(path: String): Unit = {
    pysparkJobProcessor.addFile(path)
  }

  def addPyFile(driver: ReplDriver, conf: SparkConf, path: String): Unit = {
    val localCopyDir = new File(pysparkJobProcessor.getLocalTmpDirPath)
    val localCopyFile = driver.copyFileToLocal(localCopyDir, path, SparkContext.getOrCreate(conf))
    pysparkJobProcessor.addPyFile(localCopyFile.getPath)
    if (path.endsWith(".jar")) {
      driver.addLocalFileToClassLoader(localCopyFile)
    }
  }

  private def updatePythonGatewayPort(port: Int): Unit = {
    // The python gateway port can be 0 only when LivyConf.TEST_MODE is true
    // Py4j 0.10 has different API signature for "getCallbackClient", use reflection to handle it.
    if (port != 0) {
      val callbackClient = gatewayServer.getClass
        .getMethod("getCallbackClient")
        .invoke(gatewayServer)

      val field = Class.forName("py4j.CallbackClient").getDeclaredField("port")
      field.setAccessible(true)
      field.setInt(callbackClient, port.toInt)
    }
  }
}

case class PythonJobException(message: String) extends Exception(message) {}

// scalastyle:on println
