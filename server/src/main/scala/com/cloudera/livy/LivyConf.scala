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

package com.cloudera.livy

import java.io.File
import java.lang.{Boolean => JBoolean, Long => JLong}
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration

import com.cloudera.livy.client.common.ClientConf
import com.cloudera.livy.client.common.ClientConf.ConfEntry

object LivyConf {

  case class Entry(override val key: String, override val dflt: AnyRef) extends ConfEntry

  object Entry {
    def apply(key: String, dflt: Boolean): Entry = Entry(key, dflt: JBoolean)
    def apply(key: String, dflt: Int): Entry = Entry(key, dflt: Integer)
    def apply(key: String, dflt: Long): Entry = Entry(key, dflt: JLong)
  }

  val TEST_MODE = ClientConf.TEST_MODE

  val SESSION_FACTORY = Entry("livy.server.session.factory", "process")
  val SPARK_HOME = Entry("livy.server.spark-home", null)
  val SPARK_SUBMIT_KEY = Entry("livy.server.spark-submit", null)
  val IMPERSONATION_ENABLED = Entry("livy.impersonation.enabled", false)
  val FILE_UPLOAD_MAX_SIZE = Entry("livy.file.upload.max.size", 100L * 1024 * 1024)
  val SUPERUSERS = Entry("livy.superusers", null)
  val SPARKR_PACKAGE = Entry("livy.repl.sparkr.package", null)
  val SESSION_STAGING_DIR = Entry("livy.session.staging-dir", null)
}

/**
 *
 * @param loadDefaults whether to also load values from the Java system properties
 */
class LivyConf(loadDefaults: Boolean) extends ClientConf[LivyConf](null) {

  import LivyConf._

  private lazy val _superusers = Option(get(SUPERUSERS)).map(_.split("[, ]+").toSeq).getOrElse(Nil)

  lazy val hadoopConf = new Configuration()

  /**
   * Create a LivyConf that loads defaults from the system properties and the classpath.
   * @return
   */
  def this() = this(true)

  if (loadDefaults) {
    loadFromMap(sys.props)
  }

  def loadFromFile(name: String): LivyConf = {
    getConfigFile(name)
      .map(Utils.getPropertiesFromFile)
      .foreach(loadFromMap)
    this
  }

  /** Return the location of the spark home directory */
  def sparkHome(): Option[String] = Option(get(SPARK_HOME)).orElse(sys.env.get("SPARK_HOME"))

  /** Return the path to the spark-submit executable. */
  def sparkSubmit(): String = {
    Option(get(SPARK_SUBMIT_KEY))
      .orElse { sparkHome().map { _ + File.separator + "bin" + File.separator + "spark-submit" } }
      .getOrElse("spark-submit")
  }

  /** Return the list of superusers. */
  def superusers(): Seq[String] = _superusers

  private val configDir: Option[File] = {
    sys.env.get("LIVY_CONF_DIR")
      .orElse(sys.env.get("LIVY_HOME").map(path => s"$path${File.separator}conf"))
      .map(new File(_))
      .filter(_.exists())
  }

  private def getConfigFile(name: String): Option[File] = {
    configDir.map(new File(_, name)).filter(_.exists())
  }

  private def loadFromMap(map: Iterable[(String, String)]): Unit = {
    map.foreach { case (k, v) =>
      if (k.startsWith("livy.")) {
        set(k, v)
      }
    }
  }

}
