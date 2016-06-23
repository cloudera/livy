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
package com.cloudera.livy.apps

import java.io.{File, FileNotFoundException}
import java.net.URI
import java.util.Properties

import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.storage.StorageLevel
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import com.cloudera.livy.LivyClientBuilder
import com.cloudera.livy.scalaapi.{LivyScalaClient, ScalaJobHandle, _}

/**
  * A WordCount example using Scala-API which reads text from a stream and saves
  * it as data frames. The word with maximum count is the result
  */
object WordCountApp {

  var scalaClient: LivyScalaClient = null

  def init(url: String): Unit = {
    val conf = new Properties
    val classpath: String = System.getProperty("java.class.path")
    conf.put("spark.app.name", "ScalaWordCount app")
    conf.put(SparkLauncher.DRIVER_EXTRA_CLASSPATH, classpath)
    conf.put(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, classpath)
    scalaClient = new LivyClientBuilder(true).setURI(new URI(url)).build().asScalaClient
  }

  def uploadScalaAPIJar(): Unit = {
    var appTargetFolderPath = getClass().getProtectionDomain.getCodeSource.getLocation.getPath
    val index = appTargetFolderPath.lastIndexOf("/")
    appTargetFolderPath = appTargetFolderPath.substring(0, index-7)
    val targetFolderFiles = new File(appTargetFolderPath).listFiles()
    var appJarPath: String = ""
    targetFolderFiles.foreach { file =>
      if (!file.isDirectory && file.getAbsolutePath.contains("jar")) {
        appJarPath = file.getAbsolutePath
      }
    }
    if (appJarPath == "" || !appJarPath.contains("jar")) {
      throw new FileNotFoundException("Examples App jar not found in the target folder")
    }
    val scalaApiPath = scalaClient.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val scalaApiJarUploadFuture = scalaClient.uploadJar(new File(scalaApiPath))
    scalaApiJarUploadFuture onComplete {
      case Success(t) => println("Successfully uploaded jar scala-api")
      case Failure(e) => throw e
    }
    val appJarUploadFuture = scalaClient.uploadJar(new File(appJarPath))
    appJarUploadFuture onComplete {
      case Success(t) => println("Successfully uploaded jar examples::")
      case Failure(e) => throw e
    }
  }

  def processStreamingWordCount(host: String, port: Int, outputPath:String): Unit = {
    val handle = scalaClient.submit { context =>
      context.createStreamingContext(20000)
      var index = 1
      val ssc = context.streamingctx
      val sqlctx = context.sqlctx
      val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.filter(filterEmptyConent(_)).flatMap(tokenize(_))
      words.print()
      words.foreachRDD { rdd =>
        import sqlctx.implicits._
        val df = rdd.toDF("word")
        df.write.mode("append").json(outputPath)
      }
      if (index == 2) {
        ssc.stop(true, true)
      } else {
        index += 1
      }
      ssc.start()
      ssc.awaitTerminationOrTimeout(40000)
    }
  }

  def getWordWithMostCount(outputPath: String): ScalaJobHandle[Any] = {
    val handle = scalaClient.submit { context =>
      val sqlctx = context.sqlctx
      val rdd = sqlctx.read.json(outputPath)
      rdd.registerTempTable("words")
      val result = sqlctx.sql("select word, count(word) as word_count from words " +
        "group by word order by word_count desc limit 1")
      if (result != null && result.count() > 0) {
        result.collect()(0)
      }
    }
    handle
  }

  private def filterEmptyConent(text: String): Boolean = {
    if (text == null || text.isEmpty) false else true
  }

  private def tokenize(text : String) : Array[String] = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }

  /**
    * Main method of the WordCount App
    * STEPS FOR EXECUTION
    * 1) 'livy.file.local-dir-whitelist' config has to be set in livy.conf file for the jars to be
    * added to the session
    *
    * 2) Libraries to be added to classpath - livy-api-version-SNAPSHOT.jar,
    * livy-scala-api-version-SNAPSHOT.jar, livy-client-http-version-SNAPSHOT.jar
    *
    * @param args
    * arg(0) - Livy server url
    * arg(1) - Output path to save the text read from the stream
    */
  def main(args: Array[String]) {
    if (args.length < 2) {
      throw new IllegalArgumentException("Number of args is less")
    }
    val outputPath = args(1)
    val lock = new Object
    lock.synchronized {
      init(args(0))
      lock.wait(12000)
      uploadScalaAPIJar()
      processStreamingWordCount("localhost", 8086, outputPath)
      lock.wait(50000)
    }
    val handle = getWordWithMostCount(outputPath)
    println()
    println("result::" + Await.result(handle, 40 second))
  }
}
