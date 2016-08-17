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

package com.cloudera.livy.sessions

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonToken}
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.module.SimpleModule

sealed trait Kind
case class Spark() extends Kind {
  override def toString: String = "spark"
}

case class PySpark() extends Kind {
  override def toString: String = "pyspark"
}

case class PySpark3() extends Kind {
  override def toString: String = "pyspark3"
}

case class SparkR() extends Kind {
  override def toString: String = "sparkr"
}

case class Shared() extends Kind {
  override def toString: String = "shared"
}

object Kind {

  def apply(kind: String): Kind = kind match {
    case "spark" | "scala" => Spark()
    case "pyspark" | "python" => PySpark()
    case "pyspark3" | "python3" => PySpark3()
    case "sparkr" | "r" => SparkR()
    case "shared" => Shared()
    case other => throw new IllegalArgumentException(s"Invalid kind: $other")
  }

}

class SessionKindModule extends SimpleModule("SessionKind") {

  addSerializer(classOf[Kind], new JsonSerializer[Kind]() {
    override def serialize(value: Kind, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      jgen.writeString(value.toString)
    }
  })

  addDeserializer(classOf[Kind], new JsonDeserializer[Kind]() {
    override def deserialize(jp: JsonParser, ctxt: DeserializationContext): Kind = {
      require(jp.getCurrentToken() == JsonToken.VALUE_STRING, "Kind should be a string.")
      Kind(jp.getText())
    }
  })

}
