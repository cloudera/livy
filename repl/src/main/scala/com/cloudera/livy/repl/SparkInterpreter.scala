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

import java.io._
import java.net.URLClassLoader
import java.nio.file.Paths

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{JPrintWriter, Results}
import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.repl.SparkIMain
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.json4s.{DefaultFormats, Extraction}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import com.cloudera.livy.Logging

object SparkInterpreter {
  private val EXCEPTION_STACK_TRACE_REGEX = """(.+?)\n((?:\tat .+?\n?)*)""".r
  private val KEEP_NEWLINE_REGEX = """(?=\n)""".r
  private val MAGIC_REGEX = "^%(\\w+)\\W*(.*)".r
  val USER_CODE_FRAME_NAME = "<user code>"
}

/**
 * This represents a Spark interpreter. It is not thread safe.
 */
class SparkInterpreter(conf: SparkConf) extends Interpreter with Logging {
  import SparkInterpreter._

  private implicit def formats = DefaultFormats

  private val outputStream = new ByteArrayOutputStream()
  private var sparkIMain: SparkIMain = _
  private var sparkContext: SparkContext = _
  private var sqlContext: SQLContext = _

  def kind: String = "spark"

  override def start(): SparkContext = {
    require(sparkIMain == null && sparkContext == null)

    val settings = new Settings()
    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader())
    settings.usejavacp.value = true

    sparkIMain = new SparkIMain(settings, new JPrintWriter(outputStream, true))
    sparkIMain.initializeSynchronous()

    // Spark 1.6 does not have "classServerUri"; instead, the local directory where class files
    // are stored needs to be registered in SparkConf. See comment in
    // SparkILoop::createSparkContext().
    Try(sparkIMain.getClass().getMethod("classServerUri")) match {
      case Success(method) =>
        method.setAccessible(true)
        conf.set("spark.repl.class.uri", method.invoke(sparkIMain).asInstanceOf[String])

      case Failure(_) =>
        val outputDir = sparkIMain.getClass().getMethod("getClassOutputDirectory")
        outputDir.setAccessible(true)
        conf.set("spark.repl.class.outputDir",
          outputDir.invoke(sparkIMain).asInstanceOf[File].getAbsolutePath())
    }

    restoreContextClassLoader {
      // Call sparkIMain.setContextClassLoader() to make sure SparkContext and repl are using the
      // same ClassLoader. Otherwise if someone defined a new class in interactive shell,
      // SparkContext cannot see them and will result in job stage failure.
      val setContextClassLoaderMethod = sparkIMain.getClass().getMethod("setContextClassLoader")
      setContextClassLoaderMethod.setAccessible(true)
      setContextClassLoaderMethod.invoke(sparkIMain)

      // With usejavacp=true, the Scala interpreter looks for jars under System Classpath. But it
      // doesn't look for jars added to MutableURLClassLoader. Thus extra jars are not visible to
      // the interpreter. SparkContext can use them via JVM ClassLoaders but users cannot import
      // them using Scala import statement.
      //
      // For instance: If we import a package using SparkConf:
      // "spark.jars.packages": "com.databricks:spark-csv_2.10:1.4.0"
      // then "import com.databricks.spark.csv._" in the interpreter, it will throw an error.
      //
      // Adding them to the interpreter manually to fix this issue.
      var classLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        if (classLoader.getClass.getCanonicalName == "org.apache.spark.util.MutableURLClassLoader")
        {
          val extraJarPath = classLoader.asInstanceOf[URLClassLoader].getURLs()
            // Check if the file exists. Otherwise an exception will be thrown.
            .filter { u => u.getProtocol == "file" && new File(u.getPath).isFile }
            // Livy rsc and repl are also in the extra jars list. Filter them out.
            .filterNot { u => Paths.get(u.toURI).getFileName.toString.startsWith("livy-") }

          extraJarPath.foreach { p => debug(s"Adding $p to Scala interpreter's class path...") }
          sparkIMain.addUrlsToClassPath(extraJarPath: _*)
          classLoader = null
        } else {
          classLoader = classLoader.getParent
        }
      }

      sparkContext = SparkContext.getOrCreate(conf)
      if (conf.getBoolean("livy.repl.enableHiveContext", false)) {
        try {
          val loader = Option(Thread.currentThread().getContextClassLoader)
            .getOrElse(getClass.getClassLoader)
          if (loader.getResource("hive-site.xml") == null) {
            warn("livy.repl.enableHiveContext is true but no hive-site.xml found on classpath.")
          }
          sqlContext = new HiveContext(sparkContext)
          info("Created sql context (with Hive support).")
        } catch {
          case _: java.lang.NoClassDefFoundError =>
            sqlContext = new SQLContext(sparkContext)
            info("Created sql context.")
        }
      } else {
        sqlContext = new SQLContext(sparkContext)
        info("Created sql context.")
      }
      sparkIMain.beQuietDuring {
        sparkIMain.bind("sc", "org.apache.spark.SparkContext", sparkContext, List("""@transient"""))
      }
      sparkIMain.beQuietDuring {
        sparkIMain.bind("sqlContext", sqlContext.getClass.getCanonicalName,
          sqlContext, List("""@transient"""))
      }
      execute("import org.apache.spark.SparkContext._")
      execute("import sqlContext.implicits._")
      execute("import sqlContext.sql")
      execute("import org.apache.spark.sql.functions._")
    }
    sparkContext
  }

  override def execute(code: String): Interpreter.ExecuteResponse = restoreContextClassLoader {
    require(sparkIMain != null && sparkContext != null)

    executeLines(code.trim.split("\n").toList, Interpreter.ExecuteSuccess(JObject(
      (TEXT_PLAIN, JString(""))
    )))
  }

  override def close(): Unit = synchronized {
    if (sparkContext != null) {
      sparkContext.stop()
    }

    if (sparkIMain != null) {
      sparkIMain.close()
      sparkIMain = null
    }
    sqlContext = null
  }

  private def executeMagic(magic: String, rest: String): Interpreter.ExecuteResponse = {
    magic match {
      case "json" => executeJsonMagic(rest)
      case "table" => executeTableMagic(rest)
      case _ =>
        Interpreter.ExecuteError("UnknownMagic", f"Unknown magic command $magic")
    }
  }

  private def executeJsonMagic(name: String): Interpreter.ExecuteResponse = {
    try {
      val value = sparkIMain.valueOfTerm(name) match {
        case Some(obj: RDD[_]) => obj.asInstanceOf[RDD[_]].take(10)
        case Some(obj) => obj
        case None => return Interpreter.ExecuteError("NameError", f"Value $name does not exist")
      }

      Interpreter.ExecuteSuccess(JObject(
        (APPLICATION_JSON, Extraction.decompose(value))
      ))
    } catch {
      case _: Throwable =>
        Interpreter.ExecuteError("ValueError", "Failed to convert value into a JSON value")
    }
  }

  private class TypesDoNotMatch extends Exception

  private def convertTableType(value: JValue): String = {
    value match {
      case (JNothing | JNull) => "NULL_TYPE"
      case JBool(_) => "BOOLEAN_TYPE"
      case JString(_) => "STRING_TYPE"
      case JInt(_) => "BIGINT_TYPE"
      case JDouble(_) => "DOUBLE_TYPE"
      case JDecimal(_) => "DECIMAL_TYPE"
      case JArray(arr) =>
        if (allSameType(arr.iterator)) {
          "ARRAY_TYPE"
        } else {
          throw new TypesDoNotMatch
        }
      case JObject(obj) =>
        if (allSameType(obj.iterator.map(_._2))) {
          "MAP_TYPE"
        } else {
          throw new TypesDoNotMatch
        }
    }
  }

  private def allSameType(values: Iterator[JValue]): Boolean = {
    if (values.hasNext) {
      val type_name = convertTableType(values.next())
      values.forall { case value => type_name.equals(convertTableType(value)) }
    } else {
      true
    }
  }

  private def executeTableMagic(name: String): Interpreter.ExecuteResponse = {
    val value = sparkIMain.valueOfTerm(name) match {
      case Some(obj: RDD[_]) => obj.asInstanceOf[RDD[_]].take(10)
      case Some(obj) => obj
      case None => return Interpreter.ExecuteError("NameError", f"Value $name does not exist")
    }

    extractTableFromJValue(Extraction.decompose(value))
  }

  private def extractTableFromJValue(value: JValue): Interpreter.ExecuteResponse = {
    // Convert the value into JSON and map it to a table.
    val rows: List[JValue] = value match {
      case JArray(arr) => arr
      case _ => List(value)
    }

    try {
      val headers = scala.collection.mutable.Map[String, Map[String, String]]()

      val data = rows.map { case row =>
        val cols: List[JField] = row match {
          case JArray(arr: List[JValue]) =>
            arr.zipWithIndex.map { case (v, index) => JField(index.toString, v) }
          case JObject(obj) => obj.sortBy(_._1)
          case value: JValue => List(JField("0", value))
        }

        cols.map { case (k, v) =>
          val typeName = convertTableType(v)

          headers.get(k) match {
            case Some(header) =>
              if (header.get("type").get != typeName) {
                throw new TypesDoNotMatch
              }
            case None =>
              headers.put(k, Map(
                "type" -> typeName,
                "name" -> k
              ))
          }

          v
        }
      }

      Interpreter.ExecuteSuccess(
        APPLICATION_LIVY_TABLE_JSON -> (
          ("headers" -> headers.toSeq.sortBy(_._1).map(_._2)) ~ ("data" -> data)
        ))
    } catch {
      case _: TypesDoNotMatch =>
        Interpreter.ExecuteError("TypeError", "table rows have different types")
    }
  }

  private def executeLines(
      lines: List[String],
      resultFromLastLine: Interpreter.ExecuteResponse): Interpreter.ExecuteResponse = {
    lines match {
      case Nil => resultFromLastLine
      case head :: tail =>
        val result = executeLine(head)

        result match {
          case Interpreter.ExecuteIncomplete() =>
            tail match {
              case Nil =>
                // ExecuteIncomplete could be caused by an actual incomplete statements (e.g. "sc.")
                // or statements with just comments.
                // To distinguish them, reissue the same statement wrapped in { }.
                // If it is an actual incomplete statement, the interpreter will return an error.
                // If it is some comment, the interpreter will return success.
                executeLine(s"{\n$head\n}") match {
                  case Interpreter.ExecuteIncomplete() | Interpreter.ExecuteError(_, _, _) =>
                    // Return the original error so users won't get confusing error message.
                    result
                  case _ => resultFromLastLine
                }
              case next :: nextTail =>
                executeLines(head + "\n" + next :: nextTail, resultFromLastLine)
            }
          case Interpreter.ExecuteError(_, _, _) =>
            result

          case _ =>
            executeLines(tail, result)
        }
    }
  }

  private def executeLine(code: String): Interpreter.ExecuteResponse = {
    code match {
      case MAGIC_REGEX(magic, rest) =>
        executeMagic(magic, rest)
      case _ =>
        scala.Console.withOut(outputStream) {
          sparkIMain.interpret(code) match {
            case Results.Success =>
              Interpreter.ExecuteSuccess(
                TEXT_PLAIN -> readStdout()
              )
            case Results.Incomplete => Interpreter.ExecuteIncomplete()
            case Results.Error =>
              def parseStdout(stdout: String): (String, Seq[String]) = {
                stdout match {
                  case EXCEPTION_STACK_TRACE_REGEX(ename, tracebackLines) =>
                    var traceback = KEEP_NEWLINE_REGEX.pattern.split(tracebackLines)
                    val interpreterFrameIdx = traceback.indexWhere(_.contains("$iwC$$iwC.<init>"))
                    if (interpreterFrameIdx >= 0) {
                      traceback = traceback
                        // Remove Interpreter frames
                        .take(interpreterFrameIdx)
                        // Replace weird internal class name
                        .map(_.replace("$iwC$$iwC", "<user code>"))
                      // TODO Proper translate line number in stack trace for $iwC$$iwC.
                    }
                    (ename.trim, traceback)
                  case _ => (stdout, Seq.empty)
                }
              }
              val (ename, traceback) = parseStdout(readStdout())
              Interpreter.ExecuteError("Error", ename, traceback)
          }
        }
    }
  }

  private def restoreContextClassLoader[T](fn: => T): T = {
    val currentClassLoader = Thread.currentThread().getContextClassLoader()
    try {
      fn
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader)
    }
  }

  private def readStdout() = {
    val output = outputStream.toString("UTF-8").trim
    outputStream.reset()

    output
  }
}
