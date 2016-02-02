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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A builder for Livy clients.
 */
public final class LivyClientBuilder {

  public static final String LIVY_URI_KEY = "livy.uri";

  private static final Logger LOG = Logger.getLogger(LivyClientBuilder.class.getName());

  private final Properties config;

  /**
   * Creates a new builder that will automatically load the default Livy and Spark configuration
   * from the classpath.
   */
  public LivyClientBuilder() throws IOException {
    this(true);
  }

  /**
   * Creates a new builder that will optionally load the default Livy and Spark configuration
   * from the classpath.
   *
   * Livy client configuration is stored in a file called "livy-client.conf", and Spark client
   * configuration is stored in a file called "spark-defaults.conf", both in the root of the
   * application's classpath. Livy configuration takes precedence over Spark's (in case
   * configuration entries are duplicated), and configuration set in this builder object will
   * override the values in those files.
   */
  public LivyClientBuilder(boolean loadDefaults) throws IOException {
    this.config = new Properties();

    if (loadDefaults) {
      String[] confFiles = { "spark-defaults.conf", "livy-client.conf" };

      for (String file : confFiles) {
        URL url = classLoader().getResource(file);
        if (url != null) {
          Reader r = new InputStreamReader(url.openStream(), UTF_8);
          try {
            config.load(r);
          } finally {
            r.close();
          }
        }
      }
    }
  }

  public LivyClientBuilder setURI(URI uri) {
    config.setProperty(LIVY_URI_KEY, uri.toString());
    return this;
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
    String uriStr = config.getProperty(LIVY_URI_KEY);
    if (uriStr == null) {
      throw new IllegalArgumentException("URI must be provided.");
    }
    URI uri;
    try {
      uri = new URI(uriStr);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI.", e);
    }

    LivyClient client = null;
    ServiceLoader<LivyClientFactory> loader = ServiceLoader.load(LivyClientFactory.class,
      classLoader());
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

  private ClassLoader classLoader() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = getClass().getClassLoader();
    }
    return cl;
  }

}
