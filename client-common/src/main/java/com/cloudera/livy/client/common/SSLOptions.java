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

package com.cloudera.livy.client.common;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to maintain all the SSL related configurations. This class will be inherited by Livy
 * Server and Client to create a Server / Client side SSL context.
 */
public abstract class SSLOptions {
  private static final Logger LOG = LoggerFactory.getLogger(SSLOptions.class);

  public final int port;
  protected final File keyStore;
  protected final String keyStorePassword;
  protected final String keyPassword;
  protected final String keyStoreType;

  protected final boolean needClientAuth;
  protected final File trustStore;
  protected final String trustStorePassword;
  protected final String trustStoreType;

  protected final String protocol;
  private final Set<String> enabledAlgorithms;

  protected SSLOptions(
      int port,
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
  ) {
    this.port = port;
    this.keyStore = keyStore;
    this.keyPassword = keyPassword;
    this.keyStorePassword = keyStorePassword;
    this.keyStoreType = keyStoreType;
    this.needClientAuth = needClientAuth;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
    this.trustStoreType = trustStoreType;
    this.protocol = protocol;
    this.enabledAlgorithms = enabledAlgorithms;
  }

  protected Set<String> supportedAlgorithms() throws Exception {
    if (enabledAlgorithms.isEmpty()) {
      return Collections.emptySet();
    } else {
      SSLContext context;
      try {
        context = SSLContext.getInstance(protocol);
        context.init(null, null, null);
      } catch (NullPointerException e) {
        context = SSLContext.getDefault();
      } catch (NoSuchAlgorithmException e) {
        context = SSLContext.getDefault();
      }

      Set<String> providerAlgorithms = new HashSet<>(Arrays.asList(
        context.getServerSocketFactory().getSupportedCipherSuites()));
      Set<String> supported = new HashSet<>();
      for (String algo : enabledAlgorithms) {
        if (providerAlgorithms.contains(algo)) {
          supported.add(algo);
        } else {
          LOG.debug("Discard SSL algorithm {} since it is not supported by Java", algo);
        }
      }

      if (supported.isEmpty()) {
        throw new IllegalStateException("No supported SSL algorithms found");
      }

      return supported;
    }
  }
}
