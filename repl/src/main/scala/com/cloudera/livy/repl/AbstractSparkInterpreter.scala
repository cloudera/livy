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

import java.io.ByteArrayOutputStream

import scala.tools.nsc.interpreter.Results

import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.Extraction
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import com.cloudera.livy.Logging

object AbstractSparkInterpreter {
  private val EXCEPTION_STACK_TRACE_REGEX = """(.+?)\n((?:[  |\t].+?\n?)*)""".r
  private val KEEP_NEWLINE_REGEX = """(?=\n)""".r
  private val MAGIC_REGEX = "^%(\\w+)\\W*(.*)".r
  val USER_CODE_FRAME_NAME = "<user code>"
}

abstract class AbstractSparkInterpreter extends Interpreter with Logging {
  import AbstractSparkInterpreter._

  private implicit def formats = DefaultFormats

  protected val outputStream = new ByteArrayOutputStream()

  final def kind: String = "spark"

  protected def isStarted(): Boolean

  protected def interpret(code: String): Results.Result

  protected def valueOfTerm(name: String): Option[Any]

  override def execute(code: String): Interpreter.ExecuteResponse = restoreContextClassLoader {
    require(isStarted())

    executeLines(code.trim.split("\n").toList, Interpreter.ExecuteSuccess(JObject(
      (TEXT_PLAIN, JString(""))
    )))
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
      val value = valueOfTerm(name) match {
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
    val value = valueOfTerm(name) match {
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
          interpret(code) match {
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
                        .map(_.replaceAll("(\\$iwC\\$)*\\$iwC", "<user code>"))
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

  protected def restoreContextClassLoader[T](fn: => T): T = {
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
