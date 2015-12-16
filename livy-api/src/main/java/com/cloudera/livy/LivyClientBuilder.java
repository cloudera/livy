/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A builder for Livy clients.
 */
public final class LivyClientBuilder {

  private static final Logger LOG = Logger.getLogger(LivyClientBuilder.class.getName());

  private final URI uri;
  private final Properties config;

  public LivyClientBuilder(URI uri) {
    if (uri == null) {
      throw new IllegalArgumentException("URI must be provided.");
    }
    this.uri = uri;
    this.config = new Properties();
  }

  public LivyClientBuilder setConf(String key, String value) {
    config.setProperty(key, value);
    return this;
  }

  public LivyClientBuilder setAll(Map<String, String> props) {
    config.putAll(props);
    return this;
  }

  public LivyClientBuilder setAll(Properties props) {
    config.putAll(props);
    return this;
  }

  public LivyClient build() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = getClass().getClassLoader();
    }

    LivyClient client = null;
    ServiceLoader<LivyClientFactory> loader = ServiceLoader.load(LivyClientFactory.class, cl);
    if (!loader.iterator().hasNext()) {
      throw new IllegalStateException("No LivyClientFactory implementation was found.");
    }

    Exception error = null;
    for (LivyClientFactory factory : loader) {
      try {
        client = factory.createClient(uri, config);
      } catch (Exception e) {
        // Keep the first error and re-throw it if no factories support the URI.
        if (error == null) {
          error = e;
        }
        LOG.log(Level.WARNING,
          String.format("Factory %s threw an exception.", factory.getClass().getName()), e);
      }

      if (client != null) {
        break;
      }
    }

    if (client == null) {
      throw new IllegalArgumentException(String.format(
        "URI '%s' is not supported by any registered client factories.", uri), error);
    }
    return client;
  }

}
