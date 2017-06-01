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

package com.cloudera.livy.server

import java.io.File

import scala.collection.JavaConverters._

import org.eclipse.jetty.util.ssl.SslContextFactory

import com.cloudera.livy.LivyConf
import com.cloudera.livy.client.common.SSLOptions

private[livy] class ServerSSLOptions private(
  port: Int,
  keyStore: File,
  keyPassword: String,
  keyStorePassword: String,
  keyStoreType: String,
  needClientAuth: Boolean,
  trustStore: File,
  trustStorePassword: String,
  trustStoreType: String,
  protocol: String,
  enabledAlgorithms: Set[String])
extends SSLOptions(
  port,
  keyStore,
  keyPassword,
  keyStorePassword,
  keyStoreType,
  needClientAuth,
  trustStore,
  trustStorePassword,
  trustStoreType,
  protocol,
  enabledAlgorithms.asJava
) {

  def createJettySslContextFactory(): SslContextFactory = {
    val sslContextFactory = new SslContextFactory()

    sslContextFactory.setKeyStorePath(keyStore.getAbsolutePath)
    sslContextFactory.setKeyManagerPassword(keyPassword)
    sslContextFactory.setKeyStorePassword(keyStorePassword)
    Option(keyStoreType).foreach(sslContextFactory.setKeyStoreType)

    if (needClientAuth) {
      Option(trustStore).foreach(file => sslContextFactory.setTrustStorePath(file.getAbsolutePath))
      Option(trustStorePassword).foreach(sslContextFactory.setTrustStorePassword)
      Option(trustStoreType).foreach(sslContextFactory.setTrustStoreType)
    }

    Option(protocol).foreach(sslContextFactory.setProtocol)
    if (!supportedAlgorithms().isEmpty) {
      sslContextFactory.setIncludeCipherSuites(supportedAlgorithms().asScala.toSeq: _*)
    }

    sslContextFactory
  }
}

private[livy] object ServerSSLOptions {
  def parse(conf: LivyConf): ServerSSLOptions = {
    val port = conf.getInt(LivyConf.SSL_PORT)

    val keyStore = new File(conf.get(LivyConf.SSL_KEYSTORE))
    require(keyStore.exists(), "Cannot find key store file")

    val keyPassword = conf.get(LivyConf.SSL_KEY_PASSWORD)
    val keyStorePassword = conf.get(LivyConf.SSL_KEYSTORE_PASSWORD)
    require(keyPassword != null)
    require(keyStorePassword != null)

    val keyStoreType = conf.get(LivyConf.SSL_KEYSTORE_TYPE)

    val protocol = conf.get(LivyConf.SSL_PROTOCOL)
    val algos = Option(conf.get(LivyConf.SSL_ENABLED_ALGOS)).map { s =>
      s.split(",").map(_.trim).filter(_.nonEmpty).toSet
    }.getOrElse(Set.empty)

    val clientAuth = conf.getBoolean(LivyConf.SSL_CLIENT_AUTH)
    val trustStore = Option(conf.get(LivyConf.SSL_TRUSTSTORE)).map { t =>
      val f = new File(t)
      require(f.exists(), "Cannot find trust store file")
      f
    }.getOrElse(null.asInstanceOf[File])

    val trustStorePassword = conf.get(LivyConf.SSL_TRUSTSTORE_PASSWORD)
    if (trustStore != null) {
      require(trustStorePassword != null,
        "Trust store password cannot be null if trust store is provided")
    }

    val trustStoreType = conf.get(LivyConf.SSL_TRUSTSTORE_TYPE)

    new ServerSSLOptions(port, keyStore, keyPassword, keyStorePassword, keyStoreType,
      clientAuth, trustStore, trustStorePassword, trustStoreType, protocol, algos)
  }
}
