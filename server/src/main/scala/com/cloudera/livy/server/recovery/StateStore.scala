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
package com.cloudera.livy.server.recovery

import scala.reflect._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.sessions.SessionKindModule
import com.cloudera.livy.sessions.SessionManager._

trait StateStoreCompanion {
  def create(livyConf: LivyConf): StateStore
}

protected trait JsonMapper {
  protected val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  def serializeToString(value: Object): String = mapper.writeValueAsString(value)

  def serializeToBytes(value: Object): Array[Byte] = mapper.writeValueAsBytes(value)

  def deserialize[T: ClassTag](json: String): T =
    mapper.readValue(json, classTag[T].runtimeClass.asInstanceOf[Class[T]])

  def deserialize[T: ClassTag](json: Array[Byte]): T =
    mapper.readValue(json, classTag[T].runtimeClass.asInstanceOf[Class[T]])
}

/**
 * Interface of a key-value pair storage for state storage.
 * It's responsible for de/serialization and retrieving/storing object.
 * It's the low level interface used by higher level classes like SessionStore.
 *
 * Hardcoded to use JSON serialization for now for easier ops. Will add better serialization later.
 */
abstract class StateStore extends JsonMapper {
  /**
   * Set a key-value pair to this state store. It overwrites existing value.
   * @throws Exception Throw when persisting the state store fails.
   */
  def set(key: String, value: Object): Unit

  /**
   * Get a key-value pair from this state store.
   * @return Value if the key exists. None if the key doesn't exist.
   * @throws Exception Throw when deserialization of the stored value fails.
   */
  def get[T: ClassTag](key: String): Option[T]

  /**
   * Treat keys in this state store as a directory tree and
   * return names of the direct children of the key.
   * @return List of names of the direct children of the key.
   *         Empty list if the key doesn't exist or have no child.
   */
  def getChildren(key: String): Seq[String]

  /**
   * Remove the key from this state store. Does not throw if the key doesn't exist.
   * @throws Exception Throw when persisting the state store fails.
   */
  def remove(key: String): Unit
}

/**
 * Factory to create the store chosen in LivyConf.
 */
object StateStore extends Logging {
  private[this] var stateStore: Option[StateStore] = None

  def init(livyConf: LivyConf): Unit = synchronized {
    val stateStoreName = pickStateStore(livyConf)
    initStateStore(stateStoreName, livyConf)
  }

  def get: StateStore = {
    assert(stateStore.isDefined, "StateStore hasn't been initialized.")
    stateStore.get
  }

  // Return the StateStore's companion object from its class name.
  // Using reflection here to avoid static dependency on Apache Curator.
  // scala.reflect.runtime.universe isn't thread safe. Play safe and use Manifest.
  // TODO Use scala.reflect.runtime.universe after migrating to Scala 2.11.
  private def getCompanionObject[T](name : String)(implicit man: Manifest[T]) : T =
    Class.forName(name).getField("MODULE$").get(man.runtimeClass).asInstanceOf[T]

  private[recovery] def pickStateStore(livyConf: LivyConf): String = {
    livyConf.get(LivyConf.RECOVERY_MODE) match {
      case SESSION_RECOVERY_MODE_OFF => BlackholeStateStore.getClass.getName
      case SESSION_RECOVERY_MODE_RECOVERY =>
        livyConf.get(LivyConf.RECOVERY_STATE_STORE) match {
          case "filesystem" => FileSystemStateStore.getClass.getName
          case "zookeeper" => ZooKeeperStateStore.getClass.getName
          case ss => throw new IllegalArgumentException(s"Unsupported state store $ss")
        }
      case rm => throw new IllegalArgumentException(s"Unsupported recovery mode $rm")
    }
  }

  private[recovery] def initStateStore(stateStoreClassName: String, livyConf: LivyConf): Unit = {
    if (stateStore.isEmpty) {
      try {
        val stateStoreCompanion = getCompanionObject[StateStoreCompanion](stateStoreClassName)
        stateStore = Option(stateStoreCompanion.create(livyConf))
        info(s"Using ${stateStore.get.getClass.getSimpleName} for recovery.")
      } catch {
        case e: Exception =>
          throw new Exception(s"Failed to create state store $stateStoreClassName", e)
      }
    }
  }
}
