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

package com.cloudera.livy

import java.io.{Closeable, File, InputStreamReader}
import java.net.URL
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException
import scala.concurrent.duration.Duration
import scala.util.control.Exception._

object Utils {
  def getPropertiesFromFile(file: File): Map[String, String] = {
    loadProperties(file.toURL())
  }

  def loadProperties(url: URL): Map[String, String] = {
    val inReader = new InputStreamReader(url.openStream(), UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map { k =>
        (k, properties.getProperty(k).trim())
      }.toMap
    } finally {
      inReader.close()
    }
  }

  /**
   * Checks if event has occurred during some time period. This performs an exponential backoff
   * to limit the poll calls.
   *
   * @param checkForEvent
   * @param atMost
   * @throws java.util.concurrent.TimeoutException
   * @throws java.lang.InterruptedException
   * @return
   */
  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  final def waitUntil(checkForEvent: () => Boolean, atMost: Duration): Unit = {
    val endTime = System.currentTimeMillis() + atMost.toMillis

    @tailrec
    def aux(count: Int): Unit = {
      if (!checkForEvent()) {
        val now = System.currentTimeMillis()

        if (now < endTime) {
          val sleepTime = Math.max(10 * (2 << (count - 1)), 1000)
          Thread.sleep(sleepTime)
          aux(count + 1)
        } else {
          throw new TimeoutException
        }
      }
    }

    aux(1)
  }

  /** Returns if the process is still running */
  def isProcessAlive(process: Process): Boolean = {
    try {
      process.exitValue()
      false
    } catch {
      case _: IllegalThreadStateException =>
        true
    }
  }

  def startDaemonThread(name: String)(f: => Unit): Thread = {
    val thread = new Thread(name) {
      override def run(): Unit = f
    }
    thread.setDaemon(true)
    thread.start()
    thread
  }

  def usingResource[A <: Closeable, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }

}
