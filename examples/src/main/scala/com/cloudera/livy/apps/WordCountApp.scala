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
import java.security.CodeSource
import java.util.logging.Logger

import org.apache.spark.storage.StorageLevel
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.cloudera.livy.LivyClientBuilder
import com.cloudera.livy.scalaapi.{LivyScalaClient, ScalaJobHandle, _}

/**
  * A WordCount example using Scala-API which reads text from a stream and saves
  * it as data frames. The word with maximum count is the result
  */
object WordCountApp {

  var scalaClient: LivyScalaClient = null
  val logger = Logger.getLogger("WordCountApp")

  /**
    * Initializes the scala client with the given url
    * @param url The livy server url
    */
  def init(url: String): Unit = {
    scalaClient = new LivyClientBuilder(true).setURI(new URI(url)).build().asScalaClient
  }

  /**
    * Uploads the scala-api Jar and the examples Jar from the target directory
    * @throws FileNotFoundException If either of scala-api Jar or examples Jar is not found
    */
  @throws(classOf[FileNotFoundException])
  def uploadRelevantJarsForJobExecution(): Unit = {
    val exampleAppJarPath = getPath(this)
    val scalaApiJarPath =  getPath(scalaClient)
    uploadJar(exampleAppJarPath)
    uploadJar(scalaApiJarPath)
  }

  @throws(classOf[FileNotFoundException])
  private def getSourceIfNotNull(obj: Object): CodeSource = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null) {
      source
    } else {
      throw new FileNotFoundException("Jar Source not found")
    }
  }

  @throws(classOf[FileNotFoundException])
  private def getPath(obj: Object): String = {
    val source = getSourceIfNotNull(obj)
    var path = ""
    if (obj.isInstanceOf[LivyScalaClient]) {
      path = source.getLocation.getPath
    } else if (obj == this) {
      val appTargetFolderPath = new File(source.getLocation.getPath).getParent
      val targetFolderFiles = new File(appTargetFolderPath).listFiles()
      if (targetFolderFiles == null || targetFolderFiles.size == 0) {
        throw new FileNotFoundException("Examples App jar not found in the target folder")
      }
      targetFolderFiles.foreach { file =>
        if (!file.isDirectory && file.getAbsolutePath.contains("jar")) {
          path = file.getAbsolutePath
        }
      }
    }
    if (path == "" || !path.contains("jar")) {
      throw new FileNotFoundException("Jar not found")
    }
    path
  }

  private def uploadJar(path: String) = {
    val file = new File(path)
    val uploadJarFuture = scalaClient.uploadJar(file)
    Await.ready(uploadJarFuture, 40 second) onComplete {
      case Success(t) => logger.info("Successfully uploaded " + file.getName)
      case Failure(e) => throw e
    }
  }

  /**
    * Submits a spark streaming job to the livy server
    * <p/>
    * The streaming job reads data from the given host and port. The data read
    * is saved in json format as data frames in the given output path. For simplicity,
    * the number of streaming batches are 2 with each batch for 20 seconds. The Timeout
    * of the streaming job is set to 40 seconds
    * @param host Hostname that Spark Streaming context has to connect for receiving data
    * @param port Port that Spark Streaming context has to connect for receiving data
    * @param outputPath Output path to save the processed data read by the Spark Streaming context
    */
  def processStreamingWordCount(host: String, port: Int, outputPath: String): ScalaJobHandle[Unit] = {
    val handle = scalaClient.submit { context =>
      context.createStreamingContext(20000)
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
      ssc.start()
      ssc.awaitTerminationOrTimeout(35000)
      ssc.stop(false, true)
    }
    handle
  }

  /**
    * Submits a spark sql job to the livy server
    * <p/>
    * The sql context job reads data frames from the given json path and executes
    * a sql query to get the word with max count on the temp table created with data frames
    * @param inputPath Input path to the json data containing the words
    */
  def getWordWithMostCount(inputPath: String): ScalaJobHandle[Any] = {
    val handle = scalaClient.submit { context =>
      val sqlctx = context.sqlctx
      val rdd = sqlctx.read.json(inputPath)
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
    init(args(0))
    uploadRelevantJarsForJobExecution()
    logger.info("Calling processStreamingWordCount")
    val handle1 = processStreamingWordCount("localhost", 8086, outputPath)
    Await.result(handle1, 120 second) match {
      case success: Unit => logger.info("Successfully read data from stream")
    }
    logger.info("Calling getWordWithMostCount")
    val handle = getWordWithMostCount(outputPath)
    logger.info("Word with max count::" + Await.result(handle, 40 second))
  }
}
