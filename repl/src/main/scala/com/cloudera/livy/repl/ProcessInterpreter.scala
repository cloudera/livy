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

import java.io.{BufferedReader, InputStreamReader, IOException, PrintWriter}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.Promise
import scala.io.Source

import org.apache.spark.SparkContext
import org.json4s.JValue

import com.cloudera.livy.{Logging, Utils}

private sealed trait Request
private case class ExecuteRequest(code: String, promise: Promise[JValue]) extends Request
private case class ShutdownRequest(promise: Promise[Unit]) extends Request

/**
 * Abstract class that describes an interpreter that is running in a separate process.
 *
 * This type is not thread safe, so must be protected by a mutex.
 *
 * @param process
 */
abstract class ProcessInterpreter(process: Process)
  extends Interpreter
  with Logging
{
  protected[this] val stdin = new PrintWriter(process.getOutputStream)
  protected[this] val stdout = new BufferedReader(new InputStreamReader(process.getInputStream), 1)

  override def start(): SparkContext = {
    waitUntilReady()

    // At this point there should be an already active SparkContext that can be retrieved
    // using SparkContext.getOrCreate. But we don't really support running "pre-compiled"
    // jobs against pyspark or sparkr, so just return null here.
    null
  }
  override def execute(code: String): Interpreter.ExecuteResponse = {
    try {
      sendExecuteRequest(code)
    } catch {
      case e: Throwable =>
        Interpreter.ExecuteError(e.getClass.getName, e.getMessage)
    }
  }

  override def close(): Unit = {
    if (Utils.isProcessAlive(process)) {
      logger.info("Shutting down process")
      sendShutdownRequest()

      try {
        process.getInputStream.close()
        process.getOutputStream.close()
      } catch {
        case _: IOException =>
      }
      try {
        process.destroy()
      } finally {
        logger.info("process has been shut down")
      }
    }
  }

  protected def sendExecuteRequest(request: String): Interpreter.ExecuteResponse

  protected def sendShutdownRequest(): Unit = {}

  protected def waitUntilReady(): Unit

  private[this] val stderrLock = new ReentrantLock()
  private[this] var stderrLines = Seq[String]()

  protected def takeErrorLines(): String = {
    stderrLock.lock()
    try {
      val lines = stderrLines
      stderrLines = Seq()
      lines.mkString("\n")
    } finally {
      stderrLock.unlock()
    }
  }

  private[this] val stderrThread = new Thread("process stderr thread") {
    override def run() = {
      val lines = Source.fromInputStream(process.getErrorStream).getLines()

      for (line <- lines) {
        stderrLock.lock()
        try {
          stderrLines :+= line
        } finally {
          stderrLock.unlock()
        }
      }
    }
  }

  stderrThread.setDaemon(true)
  stderrThread.start()

  private[this] val processWatcherThread = new Thread("process watcher thread") {
    override def run() = {
      val exitCode = process.waitFor()
      if (exitCode != 0) {
        error(f"Process has died with $exitCode, " + stderrLines.mkString("\n"))
      }
    }
  }

  processWatcherThread.setDaemon(true)
  processWatcherThread.start()
}
