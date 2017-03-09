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
import java.util.HashMap
import java.util.Map

import org.apache.hadoop.conf.Configuration

import com.cloudera.livy.client.common.ClientConf
import com.cloudera.livy.client.common.ClientConf.ConfEntry
import com.cloudera.livy.client.common.ClientConf.DeprecatedConf

object LivyConf {

  case class Entry(override val key: String, override val dflt: AnyRef) extends ConfEntry

  object Entry {
    def apply(key: String, dflt: Boolean): Entry = Entry(key, dflt: JBoolean)
    def apply(key: String, dflt: Int): Entry = Entry(key, dflt: Integer)
    def apply(key: String, dflt: Long): Entry = Entry(key, dflt: JLong)
  }

  val TEST_MODE = ClientConf.TEST_MODE

  val SPARK_HOME = Entry("livy.server.spark-home", null)
  val LIVY_SPARK_MASTER = Entry("livy.spark.master", "local")
  val LIVY_SPARK_DEPLOY_MODE = Entry("livy.spark.deployMode", null)

  // Two configurations to specify Spark and related Scala version. These are internal
  // configurations will be set by LivyServer and used in session creation. It is not required to
  // set usually unless running with unofficial Spark + Scala versions
  // (like Spark 2.0 + Scala 2.10, Spark 1.6 + Scala 2.11)
  val LIVY_SPARK_SCALA_VERSION = Entry("livy.spark.scalaVersion", null)
  val LIVY_SPARK_VERSION = Entry("livy.spark.version", null)

  val SESSION_STAGING_DIR = Entry("livy.session.staging-dir", null)
  val FILE_UPLOAD_MAX_SIZE = Entry("livy.file.upload.max.size", 100L * 1024 * 1024)
  val LOCAL_FS_WHITELIST = Entry("livy.file.local-dir-whitelist", null)
  val ENABLE_HIVE_CONTEXT = Entry("livy.repl.enableHiveContext", false)

  val ENVIRONMENT = Entry("livy.environment", "production")

  val SERVER_HOST = Entry("livy.server.host", "0.0.0.0")
  val SERVER_PORT = Entry("livy.server.port", 8998)
  val CSRF_PROTECTION = LivyConf.Entry("livy.server.csrf_protection.enabled", false)

  val IMPERSONATION_ENABLED = Entry("livy.impersonation.enabled", false)
  val SUPERUSERS = Entry("livy.superusers", null)

  val ACCESS_CONTROL_ENABLED = Entry("livy.server.access_control.enabled", false)
  val ACCESS_CONTROL_USERS = Entry("livy.server.access_control.users", null)

  val AUTH_TYPE = Entry("livy.server.auth.type", null)
  val AUTH_KERBEROS_PRINCIPAL = Entry("livy.server.auth.kerberos.principal", null)
  val AUTH_KERBEROS_KEYTAB = Entry("livy.server.auth.kerberos.keytab", null)
  val AUTH_KERBEROS_NAME_RULES = Entry("livy.server.auth.kerberos.name_rules", "DEFAULT")

  val HEARTBEAT_WATCHDOG_INTERVAL = Entry("livy.server.heartbeat-watchdog.interval", "1m")

  val LAUNCH_KERBEROS_PRINCIPAL =
    LivyConf.Entry("livy.server.launch.kerberos.principal", null)
  val LAUNCH_KERBEROS_KEYTAB =
    LivyConf.Entry("livy.server.launch.kerberos.keytab", null)
  val LAUNCH_KERBEROS_REFRESH_INTERVAL =
    LivyConf.Entry("livy.server.launch.kerberos.refresh_interval", "1h")
  val KINIT_FAIL_THRESHOLD =
    LivyConf.Entry("livy.server.launch.kerberos.kinit_fail_threshold", 5)

  /**
   * Recovery mode of Livy. Possible values:
   * off: Default. Turn off recovery. Every time Livy shuts down, it stops and forgets all sessions.
   * recovery: Livy persists session info to the state store. When Livy restarts, it recovers
   *   previous sessions from the state store.
   * Must set livy.server.recovery.state-store and livy.server.recovery.state-store.url to
   * configure the state store.
   */
  val RECOVERY_MODE = Entry("livy.server.recovery.mode", "off")
  /**
   * Where Livy should store state to for recovery. Possible values:
   * <empty>: Default. State store disabled.
   * filesystem: Store state on a file system.
   * zookeeper: Store state in a Zookeeper instance.
   */
  val RECOVERY_STATE_STORE = Entry("livy.server.recovery.state-store", null)
  /**
   * For filesystem state store, the path of the state store directory. Please don't use a
   * filesystem that doesn't support atomic rename (e.g. S3). e.g. file:///tmp/livy or hdfs:///.
   * For zookeeper, the address to the Zookeeper servers. e.g. host1:port1,host2:port2
   */
  val RECOVERY_STATE_STORE_URL = Entry("livy.server.recovery.state-store.url", "")

  // If Livy can't find the yarn app within this time, consider it lost.
  val YARN_APP_LOOKUP_TIMEOUT = Entry("livy.server.yarn.app-lookup-timeout", "60s")

  // How often Livy polls YARN to refresh YARN app state.
  val YARN_POLL_INTERVAL = Entry("livy.server.yarn.poll-interval", "5s")

  // Days to keep Livy server request logs.
  val REQUEST_LOG_RETAIN_DAYS = Entry("livy.server.request-log-retain.days", 5)

  // REPL related jars separated with comma.
  val REPL_JARS = Entry("livy.repl.jars", null)
  // RSC related jars separated with comma.
  val RSC_JARS = Entry("livy.rsc.jars", null)

  // How long to check livy session leakage
  val YARN_APP_LEAKAGE_CHECK_TIMEOUT = Entry("livy.server.yarn.app-leakage.check_timeout", "600s")
  // how often to check livy session leakage
  val YARN_APP_LEAKAGE_CHECK_INTERVAL = Entry("livy.server.yarn.app-leakage.check_interval", "60s")

  // Whether session timeout should be checked, by default it will be checked, which means inactive
  // session will be stopped after "livy.server.session.timeout"
  val SESSION_TIMEOUT_CHECK = Entry("livy.server.session.timeout-check", true)
  // How long will an inactive session be gc-ed.
  val SESSION_TIMEOUT = Entry("livy.server.session.timeout", "1h")
  // How long a finished session state will be kept in memory
  val SESSION_STATE_RETAIN_TIME = Entry("livy.server.session.state-retain.sec", "600s")

  val SPARK_MASTER = "spark.master"
  val SPARK_DEPLOY_MODE = "spark.submit.deployMode"
  val SPARK_JARS = "spark.jars"
  val SPARK_FILES = "spark.files"
  val SPARK_ARCHIVES = "spark.yarn.dist.archives"
  val SPARK_PY_FILES = "spark.submit.pyFiles"

  /**
   * These are Spark configurations that contain lists of files that the user can add to
   * their jobs in one way or another. Livy needs to pre-process these to make sure the
   * user can read them (in case they reference local files), and to provide correct URIs
   * to Spark based on the Livy config.
   *
   * The configuration allows adding new configurations in case we either forget something in
   * the hardcoded list, or new versions of Spark add new configs.
   */
  val SPARK_FILE_LISTS = Entry("livy.spark.file-list-configs", null)

  private val HARDCODED_SPARK_FILE_LISTS = Seq(
    SPARK_JARS,
    SPARK_FILES,
    SPARK_ARCHIVES,
    SPARK_PY_FILES,
    "spark.yarn.archive",
    "spark.yarn.dist.files",
    "spark.yarn.dist.jars",
    "spark.yarn.jar",
    "spark.yarn.jars"
  )

}

/**
 *
 * @param loadDefaults whether to also load values from the Java system properties
 */
class LivyConf(loadDefaults: Boolean) extends ClientConf[LivyConf](null) {

  import LivyConf._

  private lazy val _superusers = configToSeq(SUPERUSERS)
  private lazy val _allowedUsers = configToSeq(ACCESS_CONTROL_USERS).toSet

  lazy val hadoopConf = new Configuration()
  lazy val localFsWhitelist = configToSeq(LOCAL_FS_WHITELIST).map { path =>
    // Make sure the path ends with a single separator.
    path.stripSuffix("/") + "/"
  }

  lazy val sparkFileLists = HARDCODED_SPARK_FILE_LISTS ++ configToSeq(SPARK_FILE_LISTS)

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

  /** Return true if spark master starts with yarn. */
  def isRunningOnYarn(): Boolean = sparkMaster().startsWith("yarn")

  /** Return the spark deploy mode Livy sessions should use. */
  def sparkDeployMode(): Option[String] = Option(get(LIVY_SPARK_DEPLOY_MODE)).filterNot(_.isEmpty)

  /** Return the location of the spark home directory */
  def sparkHome(): Option[String] = Option(get(SPARK_HOME)).orElse(sys.env.get("SPARK_HOME"))

  /** Return the spark master Livy sessions should use. */
  def sparkMaster(): String = get(LIVY_SPARK_MASTER)

  /** Return the path to the spark-submit executable. */
  def sparkSubmit(): String = {
    sparkHome().map { _ + File.separator + "bin" + File.separator + "spark-submit" }.get
  }

  /** Return the list of superusers. */
  def superusers(): Seq[String] = _superusers

  /** Return the set of users allowed to use Livy via SPNEGO. */
  def allowedUsers(): Set[String] = _allowedUsers

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

  private def configToSeq(entry: LivyConf.Entry): Seq[String] = {
    Option(get(entry)).map(_.split("[, ]+").toSeq).getOrElse(Nil)
  }

  // TODO: Add Conf Deprecation
  def getConfigsWithAlternatives: Map[String, DeprecatedConf] = new HashMap[String, DeprecatedConf]
}
