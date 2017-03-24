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

import java.io.{File, FileOutputStream}
import java.lang.ProcessBuilder.Redirect
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Semaphore, TimeUnit}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}
import org.json4s._
import org.json4s.JsonDSL._

import com.cloudera.livy.client.common.ClientConf
import com.cloudera.livy.rsc.RSCConf

private case class RequestResponse(content: String, error: Boolean)

// scalastyle:off println
object SparkRInterpreter {
  private val LIVY_END_MARKER = "----LIVY_END_OF_COMMAND----"
  private val LIVY_ERROR_MARKER = "----LIVY_END_OF_ERROR----"
  private val PRINT_MARKER = f"""print("$LIVY_END_MARKER")"""
  private val EXPECTED_OUTPUT = f"""[1] "$LIVY_END_MARKER""""

  private val PLOT_REGEX = (
    "(" +
      "(?:bagplot)|" +
      "(?:barplot)|" +
      "(?:boxplot)|" +
      "(?:dotchart)|" +
      "(?:hist)|" +
      "(?:lines)|" +
      "(?:pie)|" +
      "(?:pie3D)|" +
      "(?:plot)|" +
      "(?:qqline)|" +
      "(?:qqnorm)|" +
      "(?:scatterplot)|" +
      "(?:scatterplot3d)|" +
      "(?:scatterplot\\.matrix)|" +
      "(?:splom)|" +
      "(?:stripchart)|" +
      "(?:vioplot)" +
    ")"
    ).r.unanchored

  def apply(conf: SparkConf, listener: StatementProgressListener): SparkRInterpreter = {
    val backendTimeout = sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val sparkRBackendClass = mirror.classLoader.loadClass("org.apache.spark.api.r.RBackend")
    val backendInstance = sparkRBackendClass.getDeclaredConstructor().newInstance()

    var sparkRBackendPort = 0
    val initialized = new Semaphore(0)
    // Launch a SparkR backend server for the R process to connect to
    val backendThread = new Thread("SparkR backend") {
      override def run(): Unit = {
        sparkRBackendPort = sparkRBackendClass.getMethod("init").invoke(backendInstance)
          .asInstanceOf[Int]

        initialized.release()
        sparkRBackendClass.getMethod("run").invoke(backendInstance)
      }
    }

    backendThread.setDaemon(true)
    backendThread.start()
    try {
      // Wait for RBackend initialization to finish
      initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)
      val rExec = conf.getOption("spark.r.shell.command")
        .orElse(sys.env.get("SPARKR_DRIVER_R"))
        .getOrElse("R")

      var packageDir = ""
      if (sys.env.getOrElse("SPARK_YARN_MODE", "") == "true") {
        packageDir = "./sparkr"
      } else {
        // local mode
        val rLibPath = new File(sys.env.getOrElse("SPARKR_PACKAGE_DIR",
          Seq(sys.env.getOrElse("SPARK_HOME", "."), "R", "lib").mkString(File.separator)))
        if (!ClientConf.TEST_MODE) {
          require(rLibPath.exists(), "Cannot find sparkr package directory.")
          packageDir = rLibPath.getAbsolutePath()
        }
      }

      val builder = new ProcessBuilder(Seq(rExec, "--slave @").asJava)
      val env = builder.environment()
      env.put("SPARK_HOME", sys.env.getOrElse("SPARK_HOME", "."))
      env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
      env.put("SPARKR_PACKAGE_DIR", packageDir)
      env.put("R_PROFILE_USER",
        Seq(packageDir, "SparkR", "profile", "general.R").mkString(File.separator))

      builder.redirectErrorStream(true)
      val process = builder.start()
      new SparkRInterpreter(process, backendInstance, backendThread,
        conf.get("spark.livy.spark_major_version", "1"),
        conf.getBoolean("spark.repl.enableHiveContext", false),
        listener)
    } catch {
      case e: Exception =>
        if (backendThread != null) {
          backendThread.interrupt()
        }
        throw e
    }
  }
}

class SparkRInterpreter(process: Process,
    backendInstance: Any,
    backendThread: Thread,
    val sparkMajorVersion: String,
    hiveEnabled: Boolean,
    statementProgressListener: StatementProgressListener)
  extends ProcessInterpreter(process, statementProgressListener) {
  import SparkRInterpreter._

  implicit val formats = DefaultFormats

  private[this] var executionCount = 0
  override def kind: String = "sparkr"
  private[this] val isStarted = new CountDownLatch(1)

  final override protected def waitUntilReady(): Unit = {
    // Set the option to catch and ignore errors instead of halting.
    sendRequest("options(error = dump.frames)")
    if (!ClientConf.TEST_MODE) {
      sendRequest("library(SparkR)")
      if (sparkMajorVersion >= "2") {
        if (hiveEnabled) {
          sendRequest("spark <- SparkR::sparkR.session()")
        } else {
          sendRequest("spark <- SparkR::sparkR.session(enableHiveSupport=FALSE)")
        }
        sendRequest(
          """sc <- SparkR:::callJStatic("org.apache.spark.sql.api.r.SQLUtils",
            "getJavaSparkContext", spark)""")
      } else {
        sendRequest("sc <- sparkR.init()")
        if (hiveEnabled) {
          sendRequest("sqlContext <- sparkRHive.init(sc)")
        } else {
          sendRequest("sqlContext <- sparkRSQL.init(sc)")
        }
      }
    }

    isStarted.countDown()
    executionCount = 0
  }

  override protected def sendExecuteRequest(command: String): Interpreter.ExecuteResponse = {
    isStarted.await()
    var code = command

    // Create a image file if this command is trying to plot.
    val tempFile = PLOT_REGEX.findFirstIn(code).map { case _ =>
      val tempFile = Files.createTempFile("", ".png")
      val tempFileString = tempFile.toAbsolutePath

      code = f"""png("$tempFileString")\n$code\ndev.off()"""

      tempFile
    }

    try {
      val response = sendRequest(code)

      if (response.error) {
        Interpreter.ExecuteError("Error", response.content)
      } else {
        var content: JObject = TEXT_PLAIN -> response.content

        // If we rendered anything, pass along the last image.
        tempFile.foreach { case file =>
          val bytes = Files.readAllBytes(file)
          if (bytes.nonEmpty) {
            val image = Base64.encodeBase64String(bytes)
            content = content ~ (IMAGE_PNG -> image)
          }
        }

        Interpreter.ExecuteSuccess(content)
      }

    } catch {
      case e: Error =>
        Interpreter.ExecuteError("Error", e.output)
      case e: Exited =>
        Interpreter.ExecuteAborted(e.getMessage)
    } finally {
      tempFile.foreach(Files.delete)
    }

  }

  private def sendRequest(code: String): RequestResponse = {
    stdin.println(s"""tryCatch(eval(parse(text="${StringEscapeUtils.escapeJava(code)}"))
                     |,error = function(e) sprintf("%s%s", e, "${LIVY_ERROR_MARKER}"))
                  """.stripMargin)
    stdin.flush()

    stdin.println(PRINT_MARKER)
    stdin.flush()

    readTo(EXPECTED_OUTPUT, LIVY_ERROR_MARKER)
  }

  override protected def sendShutdownRequest() = {
    stdin.println("q()")
    stdin.flush()

    while (stdout.readLine() != null) {}
  }

  override def close(): Unit = {
    try {
      val closeMethod = backendInstance.getClass().getMethod("close")
      closeMethod.setAccessible(true)
      closeMethod.invoke(backendInstance)

      backendThread.interrupt()
      backendThread.join()
    } finally {
      super.close()
    }
  }

  @tailrec
  private def readTo(
      marker: String,
      errorMarker: String,
      output: StringBuilder = StringBuilder.newBuilder): RequestResponse = {
    var char = readChar(output)

    // Remove any ANSI color codes which match the pattern "\u001b\\[[0-9;]*[mG]".
    // It would be easier to do this with a regex, but unfortunately I don't see an easy way to do
    // without copying the StringBuilder into a string for each character.
    if (char == '\u001b') {
      if (readChar(output) == '[') {
        char = readDigits(output)

        if (char == 'm' || char == 'G') {
          output.delete(output.lastIndexOf('\u001b'), output.length)
        }
      }
    }

    if (output.endsWith(marker)) {
      var result = stripMarker(output.toString(), marker)

      if (result.endsWith(errorMarker + "\"")) {
        result = stripMarker(result, "\\n" + errorMarker)
        RequestResponse(result, error = true)
      } else {
        RequestResponse(result, error = false)
      }
    } else {
      readTo(marker, errorMarker, output)
    }
  }

  private def stripMarker(result: String, marker: String): String = {
    result.replace(marker, "")
      .stripPrefix("\n")
      .stripSuffix("\n")
  }

  private def readChar(output: StringBuilder): Char = {
    val byte = stdout.read()
    if (byte == -1) {
      throw new Exited(output.toString())
    } else {
      val char = byte.toChar
      output.append(char)
      char
    }
  }

  @tailrec
  private def readDigits(output: StringBuilder): Char = {
    val byte = stdout.read()
    if (byte == -1) {
      throw new Exited(output.toString())
    }

    val char = byte.toChar

    if (('0' to '9').contains(char)) {
      output.append(char)
      readDigits(output)
    } else {
      char
    }
  }

  private class Exited(val output: String) extends Exception {}
  private class Error(val output: String) extends Exception {}
}
// scalastyle:on println
