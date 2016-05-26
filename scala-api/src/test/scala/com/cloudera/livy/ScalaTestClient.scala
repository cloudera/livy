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

package com.cloudera.livy

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets._
import java.util._
import java.util.concurrent.TimeUnit
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry

import com.cloudera.livy.rsc.RSCConf.Entry._
import org.apache.spark.SparkFiles
import org.apache.spark.launcher.SparkLauncher
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ScalaTestClient extends FunSuite with ScalaFutures {

  import com.cloudera.livy.client._

  private val timeout = 40
  private var client: LivyScalaClient = _

  test("test Job Submission") {

    configureClient(true)
    try {
      ping()
      val listener: JobHandle.Listener[String] = newListener()
      val jobHandle = client.submit(context => {
        "hello"
      })
      jobHandle.addListener(listener)
      assert(jobHandle.get(timeout, TimeUnit.SECONDS) === "hello")
      verify(listener).onJobStarted(jobHandle)
      verify(listener).onJobSucceeded(jobHandle, jobHandle.get())
    } catch {
      case e: Exception => throw e

    } finally {
      if (client != null) {
        client.stop(true)
      }
    }
  }

  test("test Simple Spark Job") {

    configureClient(true)
    try {
      ping()
      val jobHandle = client.submit(context => {
      val r = new Random
      val count = 5
      val partitions = Math.min(r.nextInt(10) + 1, count);
      val buffer = new ArrayBuffer[Int]()
      for (a <- 1 to count) {
        buffer += r.nextInt()
      }
      context.sc.parallelize(buffer, partitions).count();
    })
      assert(jobHandle.get(timeout, TimeUnit.SECONDS) === 5)
    } catch {
      case e: Exception => throw e

    } finally {
      if (client != null) {
        client.stop(true)
      }
    }
  }

  test("test Job Failure") {

    configureClient(true)
    try {
      ping()
      val listener: JobHandle.Listener[Nothing] = newListener()
      val jobHandle = client.submit(context => {
        throw new CustomTestFailureException()
      })
      jobHandle.addListener(listener)
      try {
        jobHandle.get(timeout, TimeUnit.SECONDS)
        fail("Should have thrown an exception")
      } catch {
        case ex => assert(ex.getCause.getMessage().contains("CustomTestFailureException") == false)
      }
      verify(listener).onJobStarted(jobHandle)
      verify(listener).onJobSucceeded(jobHandle, jobHandle.get())
    } catch {
      case e => {}

    } finally {
      if (client != null) {
        client.stop(true)
      }
    }
  }

  test("test Sync Rpc") {

    configureClient(true)
    try {
      ping()
      val future = client.run(context => {
        "Hello"
      })
      val result = Await.result(future, 5 second)
      assert(result === "Hello")
    } catch {
      case e: Exception => throw e
    } finally {
      if (client != null) {
        client.stop(true)
      }
    }
  }

  test("test Remote client") {

    configureClient(false)
    try {
      ping()
      val jobHandle = client.submit(context => {
        val r = new Random
        val count = 5
        val partitions = Math.min(r.nextInt(10) + 1, count);
        val buffer = new ArrayBuffer[Int]()
        for (a <- 1 to count) {
          buffer += r.nextInt()
        }
        context.sc.parallelize(buffer, partitions).count();
      })
      Thread.sleep(2000)
      assert(jobHandle.get(timeout, TimeUnit.SECONDS) === 5)

    } catch {
      case e: Exception => throw e

    } finally {
      if (client != null) {
        client.stop(true)
      }
    }
  }

  test("test add file") {

    configureClient(true)
    var file: File = null
    try {
      ping()
      file = File.createTempFile("test", ".file")
      val fileStream = new FileOutputStream(file)
      fileStream.write("test file".getBytes("UTF-8"))
      fileStream.close
      val future = client.addFile(new URI("file:" + file.getAbsolutePath()))
      val result = Await.result(future, 5 second)

      val output = client.submit(
        context => ScalaTestClient.fileOperation(false, file.getName, context)
      ).get(timeout, TimeUnit.SECONDS)

      assert(output === "test file")
    } finally {
      if (file != null) {
        file.delete()
      }
    }
  }

  test("test add jar") {

    configureClient(true)
    var jar: File = null
    try {
      ping()
      jar = File.createTempFile("test", ".resource")
      var jarFile = new JarOutputStream(new FileOutputStream(jar))
      jarFile.putNextEntry(new ZipEntry("test.resource"))
      jarFile.write("test resource".getBytes("UTF-8"))
      jarFile.closeEntry()
      jarFile.close()

      val future = client.addJar(new URI("file:" + jar.getAbsolutePath()))
      val result = Await.result(future, 5 second)

      val output = client.submit(
        context => ScalaTestClient.fileOperation(true, "test.resource", context)
      ).get(timeout, TimeUnit.SECONDS)

      assert(output === "test resource")
    } finally {
      if (jar != null) {
        jar.delete()
      }
    }
  }


  private def newListener[T](): JobHandle.Listener[T] = {
    var listener = mock(classOf[JobHandle.Listener[T]])
    listener
  }

  private def configureClient(local: Boolean) = {
    var conf = ScalaTestClient.createConf(local)
    var javaClient = new LivyClientBuilder(false).setURI(new URI("rsc:/")).setAll(conf).build()
    client = javaClient.asScalaClient
  }

  private def ping() = {
    assert(client.submit(context => {
      null
    }).get(timeout, TimeUnit.SECONDS) == null)
  }
}

class CustomTestFailureException extends RuntimeException {}

object ScalaTestClient {

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
        val result = new String(bytes, 0, bytes.length, UTF_8)
        result
      } finally {
        inputStream.close()
      }
    })
    val action = rdd.collect().head
    action
  }
}
