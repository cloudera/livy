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

// scalastyle:off println
package com.cloudera.livy.examples

import java.io.{File, FileNotFoundException, IOException}
import java.net.URI

import org.apache.spark.storage.StorageLevel
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import com.cloudera.livy.LivyClientBuilder
import com.cloudera.livy.scalaapi.{LivyScalaClient, ScalaJobHandle, _}

/**
 *  A WordCount example using Scala-API which reads text from a stream and saves
 *  it as data frames. The word with maximum count is the result
 */
object WordCountApp {

  var scalaClient: LivyScalaClient = null

  /**
   *  Initializes the scala client with the given url
   *  @param url The livy server url
   */
  def init(url: String): Unit = {
    scalaClient = new LivyClientBuilder(false).setURI(new URI(url)).build().asScalaClient
  }

  /**
   *  Uploads the scala-api Jar and the examples Jar from the target directory
   *  @throws FileNotFoundException If either of scala-api Jar or examples Jar is not found
   */
  @throws(classOf[FileNotFoundException])
  def uploadRelevantJarsForJobExecution(): Unit = {
    val exampleAppJarPath = getSourcePath(this)
    val scalaApiJarPath = getSourcePath(scalaClient)
    uploadJar(exampleAppJarPath)
    uploadJar(scalaApiJarPath)
  }

  @throws(classOf[FileNotFoundException])
  private def getSourcePath(obj: Object): String = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null && source.getLocation.getPath != "") {
      source.getLocation.getPath
    } else {
      throw new FileNotFoundException(s"Jar containing ${obj.getClass.getName} not found.")
    }
  }

  private def uploadJar(path: String) = {
    val file = new File(path)
    val uploadJarFuture = scalaClient.uploadJar(file)
    Await.result(uploadJarFuture, 40 second) match {
      case null => println("Successfully uploaded " + file.getName)
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
  def processStreamingWordCount(
      host: String,
      port: Int,
      outputPath: String): ScalaJobHandle[Unit] = {
    scalaClient.submit { context =>
      context.createStreamingContext(20000)
      val ssc = context.streamingctx
      val sqlctx = context.sqlctx
      val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.filter(filterEmptyContent(_)).flatMap(tokenize(_))
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
  }

  /**
   * Submits a spark sql job to the livy server
   * <p/>
   * The sql context job reads data frames from the given json path and executes
   * a sql query to get the word with max count on the temp table created with data frames
   * @param inputPath Input path to the json data containing the words
   */
  def getWordWithMostCount(inputPath: String): ScalaJobHandle[String] = {
    scalaClient.submit { context =>
      val sqlctx = context.sqlctx
      try {
        val rdd = sqlctx.read.json(inputPath)
        rdd.registerTempTable("words")
        val result = sqlctx.sql("select word, count(word) as word_count from words " +
          "group by word order by word_count desc limit 1")
        result.first().toString()
      } catch {
        case exception: IOException => {
          throw new Exception("The input path does not have any data frame")
        }
      }
    }
  }

  private def filterEmptyContent(text: String): Boolean = {
    text != null && !text.isEmpty
  }

  private def tokenize(text : String) : Array[String] = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }

  private def stopClient(shutdownContext: Boolean): Unit = {
    scalaClient.stop(shutdownContext)
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
    val Array(uri, outputPath) = args
    try {
      init(uri)
      uploadRelevantJarsForJobExecution()
      println("Calling processStreamingWordCount")
      val handle1 = processStreamingWordCount("localhost", 8086, outputPath)
      Await.result(handle1, 120 second)
      println("Calling getWordWithMostCount")
      val handle = getWordWithMostCount(outputPath)
      println("Word with max count::" + Await.result(handle, 40 second))
    } finally {
      stopClient(true)
    }
  }
}
// scalastyle:off println
