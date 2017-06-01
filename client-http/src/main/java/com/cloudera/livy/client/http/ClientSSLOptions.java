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

package com.cloudera.livy.client.http;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;

import com.cloudera.livy.client.common.SSLOptions;

class ClientSSLOptions extends SSLOptions {

  private ClientSSLOptions(
    File keyStore,
    String keyPassword,
    String keyStorePassword,
    String keyStoreType,
    boolean needClientAuth,
    File trustStore,
    String trustStorePassword,
    String trustStoreType,
    String protocol,
    Set<String> enabledAlgorithms
  ) throws Exception {
    super(-1, keyStore, keyPassword, keyStorePassword, keyStoreType, needClientAuth,
      trustStore, trustStorePassword, trustStoreType, protocol, enabledAlgorithms);
  }

  static ClientSSLOptions parse(HttpConf conf) throws Exception {
    File trustStore = null;
    if (conf.get(HttpConf.Entry.SSL_TRUSTSTORE) != null) {
      trustStore = new File(conf.get(HttpConf.Entry.SSL_TRUSTSTORE));
      if (!trustStore.exists()) {
        throw new IllegalStateException("Trust store file " + trustStore.getAbsolutePath() +
        " cannot be found");
      }
    }

    String trustStorePassword = conf.get(HttpConf.Entry.SSL_TRUSTSTORE_PASSWORD);
    if (trustStore != null && trustStorePassword == null) {
      throw new IllegalStateException("trust store password cannot be null if trust store is " +
       "provided");
    }

    String trustStoreType = conf.get(HttpConf.Entry.SSL_TRUSTSTORE_TYPE);

    boolean clientAuth = conf.getBoolean(HttpConf.Entry.SSL_CLIENT_AUTH);

    File keyStore = null;
    if (conf.get(HttpConf.Entry.SSL_KEYSTORE) != null) {
      keyStore = new File(conf.get(HttpConf.Entry.SSL_KEYSTORE));
      if (!keyStore.exists()) {
        throw new IllegalStateException("Key store file " + keyStore.getAbsolutePath() +
        " cannot be found");
      }
    }

    String keyPassword = conf.get(HttpConf.Entry.SSL_KEY_PASSWORD);
    String keyStorePassword = conf.get(HttpConf.Entry.SSL_KEYSTORE_PASSWORD);
    if (keyStore != null && (keyPassword == null || keyStorePassword == null)) {
      throw new IllegalStateException("Key password or key store password cannot be null if " +
       "key store is provided.");
    }

    String keyStoreType = conf.get(HttpConf.Entry.SSL_KEYSTORE_TYPE);

    String protocol = conf.get(HttpConf.Entry.SSL_PROTOCOL);

    String algorithms = conf.get(HttpConf.Entry.SSL_ENABLED_ALGOS);
    Set<String> enabledAlgorithms;
    if (algorithms != null) {
      enabledAlgorithms = new HashSet<>(Arrays.asList(algorithms.split(",")));
    } else {
      enabledAlgorithms = Collections.emptySet();
    }

    return new ClientSSLOptions(keyStore, keyPassword, keyStorePassword, keyStoreType, clientAuth,
      trustStore, trustStorePassword, trustStoreType, protocol, enabledAlgorithms);
  }

  SSLConnectionSocketFactory createSslConnectionSocketFactory() throws Exception {
    KeyStore trustStoreObj = null;
    if (trustStore != null) {
      if (trustStoreType != null) {
        trustStoreObj = KeyStore.getInstance(trustStoreType);
      } else {
        trustStoreObj = KeyStore.getInstance(KeyStore.getDefaultType());
      }

      FileInputStream ins = new FileInputStream(trustStore);
      try {
        trustStoreObj.load(ins, trustStorePassword.toCharArray());
      } finally {
        ins.close();
      }
    }

    KeyStore keyStoreObj = null;
    if (keyStore != null) {
      if (keyStoreType != null) {
        keyStoreObj = KeyStore.getInstance(keyStoreType);
      } else {
        keyStoreObj = KeyStore.getInstance(KeyStore.getDefaultType());
      }

      FileInputStream ins = new FileInputStream(keyStore);
      try {
        keyStoreObj.load(ins, keyStorePassword.toCharArray());
      } finally {
        ins.close();
      }
    }

    SSLContextBuilder builder = SSLContexts.custom();
    if (trustStoreObj != null) {
      builder.loadTrustMaterial(trustStoreObj, new TrustStrategy() {
        @Override
        public boolean isTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
          return true;
        }
      });
    }
    if (keyStoreObj != null) {
      builder.loadKeyMaterial(keyStoreObj, keyPassword.toCharArray());
    }

    if (protocol != null) {
      builder.useProtocol(protocol);
    }

    SSLContext sslContext = builder.build();
    return new SSLConnectionSocketFactory(
      sslContext,
      new String[] { protocol },
      supportedAlgorithms().toArray(new String[supportedAlgorithms().size()]),
      new DefaultHostnameVerifier());
  }
}
