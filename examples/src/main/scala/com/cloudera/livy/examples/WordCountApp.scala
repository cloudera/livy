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
   *
   * The streaming job reads data from the given host and port. The data read
   * is saved in json format as data frames in the given output path file. If the file is present
   * it appends to it, else creates a new file. For simplicity, the number of streaming batches
   * are 2 with each batch for 20 seconds. The Timeout of the streaming job is set to 40 seconds
   * @param host Hostname that Spark Streaming context has to connect for receiving data
   * @param port Port that Spark Streaming context has to connect for receiving data
   * @param outputPath Output path to save the processed data read by the Spark Streaming context
   */
  def processStreamingWordCount(
      host: String,
      port: Int,
      outputPath: String): ScalaJobHandle[Unit] = {
    scalaClient.submit { context =>
      context.createStreamingContext(15000)
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
      ssc.awaitTerminationOrTimeout(12000)
      ssc.stop(false, true)
    }
  }

  /**
   * Submits a spark sql job to the livy server
   *
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
          throw new IOException("The input path does not have any data frame", exception)
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

  private def stopClient(): Unit = {
    if (scalaClient != null) {
      scalaClient.stop(true)
      scalaClient = null;
    }
  }

  /**
   * Main method of the WordCount App. This method does the following
   * - Validate the arguments
   * - Initializes the scala client of livy
   * - Uploads the required livy and app code jar files to the spark cluster needed during runtime
   * - Executes the streaming job that reads text-data from socket stream, tokenizes and saves
   *   them as dataframes in JSON format in the given output path
   * - Executes the sql-context job which reads the data frames from the given output path and
   * and returns the word with max count
   *
   * @param args
   *
   * REQUIRED ARGUMENTS
   * arg(0) - Livy server url
   * arg(1) - Output path to save the text read from the stream
   *
   * OPTIONAL ARGUMENTS(Default values: arg(2) = 'localhost', arg(3) = 8086)
   * arg(2) - Socket stream host that spark streaming will listen to
   * arg(3) - Socket stream port that spark streaming will listen to
   * Optional arguments need to be passed in a key=value way
   * Example:
   * host=examplehost where "host" is the required key for arg(2)
   * port=8080 where "port" is the required key for arg(3)
   *
   * STEPS FOR EXECUTION - To get accurate results for one execution:
   * 1) Delete if the file already exists in the given outputFilePath
   *
   * 2) Spark streaming will listen to the given or defaulted host and port. So textdata needs to be
   *    passed as socket stream during the run-time of the App. The streaming context reads 2
   *    batches of data with an interval of 20 seconds for each batch. All the data has to be
   *    fed before the streaming context completes the second batch. NOTE - Inorder to get accurate
   *    results for one execution, pass the textdata before the execution of the app so that all the
   *    data is read by the socket stream
   *    To pass data to localhost and port 8086 provide the following command
   *    nc -kl 8086
   *
   * 3) The text can be provided as paragraphs as the app will tokenize the data and filter spaces
   *
   * 4) Execute the application jar file with the required and optional arguments either using
   * mvn or scala.
   *
   * Example execution:
   * scala -cp /pathTo/livy-api-*version*-SNAPSHOT.jar:/pathTo/livy-client-http-*version*
   * -SNAPSHOT.jar:/pathTo/livy-examples-*version*-SNAPSHOT.jar:/pathTo/livy-scala-api-*version*
   * -SNAPSHOT.jar com.cloudera.livy.examples.WordCountApp http://livy-host:8998 /outputFilePath
   * host=myhost port=8080
   */
  def main(args: Array[String]) {

    var socketStreamHost: String = "localhost"
    var socketStreamPort: Int = 8086
    var url = ""
    var outputFilePath = ""
    def parseOptionalArg(arg: String): Unit = {
      val Array(argKey, argValue) = arg.split("=")
      if (argKey == "host") {
        socketStreamHost = argValue
      } else if (argKey == "port") {
        socketStreamPort = argValue.toInt
      }
    }
    def parseRequiredArgs(requiredArgs: Array[String]): Unit = {
      url = requiredArgs(0)
      outputFilePath = requiredArgs(1)
    }
    args.length match {
      case 2 =>
        parseRequiredArgs(args)
      case 3 =>
        parseRequiredArgs(args.slice(0, 2))
        parseOptionalArg(args(2))
      case 4 =>
        parseRequiredArgs(args.slice(0, 2))
        args.slice(2, 4).foreach(parseOptionalArg)
      case _ =>
        throw new IllegalArgumentException("Check the docs to provide required and optional " +
          "arguments")
    }
    try {
      init(url)
      uploadRelevantJarsForJobExecution()
      println("Calling processStreamingWordCount")
      val handle1 = processStreamingWordCount("localhost", 8086, outputFilePath)
      Await.result(handle1, 100 second)
      println("Calling getWordWithMostCount")
      val handle = getWordWithMostCount(outputFilePath)
      println("Word with max count::" + Await.result(handle, 100 second))
    } finally {
      stopClient()
    }
  }
}
// scalastyle:off println
