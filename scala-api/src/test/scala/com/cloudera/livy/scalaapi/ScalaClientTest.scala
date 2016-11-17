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

package com.cloudera.livy.scalaapi

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets._
import java.util._
import java.util.concurrent.CountDownLatch
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

import org.apache.spark.SparkFiles
import org.apache.spark.launcher.SparkLauncher
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.ScalaFutures

import com.cloudera.livy.LivyBaseUnitTestSuite
import com.cloudera.livy.rsc.RSCConf.Entry._

class ScalaClientTest extends FunSuite
  with ScalaFutures
  with BeforeAndAfter
  with LivyBaseUnitTestSuite {

  import com.cloudera.livy._

  private var client: LivyScalaClient = _

  after {
    if (client != null) {
      client.stop(true)
      client = null
    }
  }

  test("test Job Submission") {
    configureClient(true)
    val jobHandle = client.submit(ScalaClientTestUtils.helloJob)
    ScalaClientTestUtils.assertTestPassed(jobHandle, "hello")
  }

  test("test Simple Spark Job") {
    configureClient(true)
    val sFuture = client.submit(ScalaClientTestUtils.simpleSparkJob)
    ScalaClientTestUtils.assertTestPassed(sFuture, 5)
  }

  test("test Job Failure") {
    configureClient(true)
    val sFuture = client.submit(ScalaClientTestUtils.throwExceptionJob)
    val lock = new CountDownLatch(1)
    var testFailure : Option[String] = None
    sFuture onComplete {
      case Success(t) => {
        testFailure = Some("Test should have thrown CustomFailureException")
        lock.countDown()
      }
      case Failure(e) => {
        if (!e.getMessage.contains("CustomTestFailureException")) {
          testFailure = Some("Test did not throw expected exception - CustomFailureException")
        }
        lock.countDown()
      }
    }
    ScalaClientTestUtils.assertAwait(lock)
    testFailure.foreach(fail(_))
  }

  test("test Sync Rpc") {
    configureClient(true)
    val future = client.run(ScalaClientTestUtils.helloJob)
    ScalaClientTestUtils.assertTestPassed(future, "hello")
  }

  test("test Remote client") {
    configureClient(false)
    val sFuture = client.submit(ScalaClientTestUtils.simpleSparkJob)
    ScalaClientTestUtils.assertTestPassed(sFuture, 5)
  }

  test("test add file") {
    configureClient(true)
    val file = File.createTempFile("test", ".file")
    val fileStream = new FileOutputStream(file)
    fileStream.write("test file".getBytes("UTF-8"))
    fileStream.close
    val addFileFuture = client.addFile(new URI("file:" + file.getAbsolutePath()))
    Await.ready(addFileFuture, ScalaClientTestUtils.Timeout second)
    val sFuture = client.submit { context =>
      ScalaClientTest.fileOperation(false, file.getName, context)
    }
    ScalaClientTestUtils.assertTestPassed(sFuture, "test file")
  }

  test("test add jar") {
    configureClient(true)
    val jar = File.createTempFile("test", ".resource")
    val jarFile = new JarOutputStream(new FileOutputStream(jar))
    jarFile.putNextEntry(new ZipEntry("test.resource"))
    jarFile.write("test resource".getBytes("UTF-8"))
    jarFile.closeEntry()
    jarFile.close()
    val addJarFuture = client.addJar(new URI("file:" + jar.getAbsolutePath()))
    Await.ready(addJarFuture, ScalaClientTestUtils.Timeout second)
    val sFuture = client.submit { context =>
      ScalaClientTest.fileOperation(true, "test.resource", context)
    }
    ScalaClientTestUtils.assertTestPassed(sFuture, "test resource")
  }

  test("Successive onComplete callbacks") {
    var testFailure: Option[String] = None
    configureClient(true)
    val future = client.run(ScalaClientTestUtils.helloJob)
    val lock = new CountDownLatch(3)
    for (i <- 0 to 2) {
      future onComplete {
        case Success(t) => {
          if (!t.equals("hello")) testFailure = Some("Expected message not returned")
          lock.countDown()
        }
        case Failure(e) => {
          testFailure = Some("onComplete should not have triggered Failure callback")
          lock.countDown()
        }
      }
    }
    ScalaClientTestUtils.assertAwait(lock)
    testFailure.foreach(fail(_))
  }

  private def configureClient(local: Boolean) = {
    val conf = ScalaClientTest.createConf(local)
    val javaClient = new LivyClientBuilder(false).setURI(new URI("rsc:/")).setAll(conf).build()
    client = javaClient.asScalaClient
    pingJob()
  }

  private def pingJob() = {
    val future = client.submit { context =>
      null
    }
    ScalaClientTestUtils.assertTestPassed(future, null)
  }
}

class CustomTestFailureException extends RuntimeException {}

object ScalaClientTest {

  def createConf(local: Boolean): Properties = {
    val conf = new Properties
    if (local) {
      conf.put(CLIENT_IN_PROCESS.key, "true")
      conf.put(SparkLauncher.SPARK_MASTER, "local")
      conf.put("spark.app.name", "SparkClientSuite Local App")
    } else {
      val classpath: String = System.getProperty("java.class.path")
      conf.put("spark.app.name", "SparkClientSuite Remote App")
      conf.put(SparkLauncher.DRIVER_MEMORY, "512m")
      conf.put(SparkLauncher.DRIVER_EXTRA_CLASSPATH, classpath)
      conf.put(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, classpath)
    }
    conf.put(LIVY_JARS.key, "")
    conf.put(SPARK_HOME.key(), System.getenv("SPARK_HOME"))
    conf
  }

  def fileOperation(isResource: Boolean, fileName: String, context: ScalaJobContext): String = {
    val arr = Seq(1)
    val rdd = context.sc.parallelize(arr).map { value =>
      var inputStream: InputStream = null
      if (isResource) {
        val ccl = Thread.currentThread.getContextClassLoader
        inputStream = ccl.getResourceAsStream(fileName)
      } else {
        inputStream = new FileInputStream(SparkFiles.get(fileName))
      }
      try {
        val out = new ByteArrayOutputStream()
        val buffer = new Array[Byte](1024)
        var read = inputStream.read(buffer)
        while (read >= 0) {
          out.write(buffer, 0, read)
          read = inputStream.read(buffer)
        }
        val bytes = out.toByteArray
        new String(bytes, 0, bytes.length, UTF_8)
      } finally {
        inputStream.close()
      }
    }
    rdd.collect().head
  }
}
