/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry

import org.apache.spark.SparkFiles
import org.apache.spark.launcher.SparkLauncher
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.ScalaFutures

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.cloudera.livy.rsc.RSCConf.Entry._

class ScalaClientTest extends FunSuite with ScalaFutures with BeforeAndAfter {

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
    val jobHandle = client.submit(ScalaClientTest.helloJob)
    val result = Await.result(jobHandle, 10 second)
    assert(result === "hello")
  }

  test("test Simple Spark Job") {
    configureClient(true)
    val sFuture = client.submit(ScalaClientTest.simpleSparkJob)
    val result = Await.result(sFuture, 10 second)
    assert(result === 5)
  }

  test("test Job Failure") {
    configureClient(true)
    val sFuture = client.submit(ScalaClientTest.throwExceptionJob)
    Thread.sleep(5000)
    sFuture onComplete {
      case Success(t) => {
        fail("Should have thrown an exception")
      }
      case Failure(e) => {
        assert(e.getMessage.contains("CustomTestFailureException"))
      }
    }
  }

  test("test Sync Rpc") {
    configureClient(true)
    val future = client.run(ScalaClientTest.helloJob)
    val result = Await.result(future, 10 second)
    assert(result === "hello")
  }

  test("test Remote client") {
    configureClient(false)
    val sFuture = client.submit(ScalaClientTest.simpleSparkJob)
    val result = Await.result(sFuture, 10 second)
    assert(result === 5)
  }

  test("test add file") {
    configureClient(true)
    var file: File = null
    file = File.createTempFile("test", ".file")
    val fileStream = new FileOutputStream(file)
    fileStream.write("test file".getBytes("UTF-8"))
    fileStream.close
    client.addFile(new URI("file:" + file.getAbsolutePath()))
    Thread.sleep(5000)
    val sFuture = client.submit(
      context => ScalaClientTest.fileOperation(false, file.getName, context)
    )
    val output = Await.result(sFuture, 10 second)
    assert(output === "test file")
  }

  test("test add jar") {
    configureClient(true)
    var jar: File = null
    jar = File.createTempFile("test", ".resource")
    val jarFile = new JarOutputStream(new FileOutputStream(jar))
    jarFile.putNextEntry(new ZipEntry("test.resource"))
    jarFile.write("test resource".getBytes("UTF-8"))
    jarFile.closeEntry()
    jarFile.close()
    client.addJar(new URI("file:" + jar.getAbsolutePath()))
    Thread.sleep(5000)
    val sFuture = client.submit(
      context => ScalaClientTest.fileOperation(true, "test.resource", context)
    )
    val output = Await.result(sFuture, 10 second)
    assert(output === "test resource")
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
    val result = Await.result(future, 5 second)
    assert(result == null)
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
    conf
  }

  def fileOperation(isResource: Boolean, fileName: String, context: ScalaJobContext): String = {
    val arr = Seq(1)
    val rdd = context.sc.parallelize(arr).map(value => {
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
    })
    rdd.collect().head
  }

  def helloJob(context: ScalaJobContext): String = "hello"

  def throwExceptionJob(context: ScalaJobContext) = throw new CustomTestFailureException

  def simpleSparkJob(context: ScalaJobContext): Long = {
    val r = new Random
    val count = 5
    val partitions = Math.min(r.nextInt(10) + 1, count)
    val buffer = new ArrayBuffer[Int]()
    for (a <- 1 to count) {
      buffer += r.nextInt()
    }
    context.sc.parallelize(buffer, partitions).count()
  }
}
