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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.cloudera.livy.client.common.ClientConf;

class HttpConf extends ClientConf<HttpConf> {

  private static final String HTTP_CONF_PREFIX = "livy.client.http.";

  static enum Entry implements ConfEntry {
    CONNECTION_TIMEOUT("connection.timeout", "10s"),
    CONNECTION_IDLE_TIMEOUT("connection.idle.timeout", "10m"),
    SOCKET_TIMEOUT("connection.socket.timeout", "5m"),

    JOB_INITIAL_POLL_INTERVAL("job.initial-poll-interval", "100ms"),
    JOB_MAX_POLL_INTERVAL("job.max-poll-interval", "5s"),

    CONTENT_COMPRESS_ENABLE("content.compress.enable", true),

    // Kerberos related configuration
    SPNEGO_ENABLED("spnego.enable", false),
    AUTH_LOGIN_CONFIG("auth.login.config", null),
    KRB5_DEBUG_ENABLED("krb5.debug", false),
    KRB5_CONF("krb5.conf", null);

    private final String key;
    private final Object dflt;

    private Entry(String key, Object dflt) {
      this.key = HTTP_CONF_PREFIX + key;
      this.dflt = dflt;
    }

    @Override
    public String key() { return key; }

    @Override
    public Object dflt() { return dflt; }
  }

  HttpConf(Properties config) {
    super(config);

    if (getBoolean(Entry.SPNEGO_ENABLED)) {
      if (get(Entry.AUTH_LOGIN_CONFIG ) == null) {
        throw new IllegalArgumentException(Entry.AUTH_LOGIN_CONFIG.key + " should not be null");
      }

      if (get(Entry.KRB5_CONF) == null) {
        throw new IllegalArgumentException(Entry.KRB5_CONF.key + " should not be null");
      }

      System.setProperty("java.security.auth.login.config", get(Entry.AUTH_LOGIN_CONFIG));
      System.setProperty("java.security.krb5.conf", get(Entry.KRB5_CONF));
      System.setProperty(
        "sun.security.krb5.debug", String.valueOf(getBoolean(Entry.KRB5_DEBUG_ENABLED)));
      // This is needed to get Kerberos credentials from the environment, instead of
      // requiring the application to manually obtain the credentials.
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    }
  }

  boolean isSpnegoEnabled() {
    return getBoolean(Entry.SPNEGO_ENABLED);
  }

  private static final Map<String, DeprecatedConf> configsWithAlternatives
    = new HashMap<String, DeprecatedConf>() {{
      put(HttpConf.Entry.JOB_INITIAL_POLL_INTERVAL.key, DepConf.JOB_INITIAL_POLL_INTERVAL);
      put(HttpConf.Entry.JOB_MAX_POLL_INTERVAL.key, DepConf.JOB_MAX_POLL_INTERVAL);
  }};

  // Maps deprecated key to DeprecatedConf with the same key.
  // There are no deprecated configs without alternatives currently.
  private static final Map<String, DeprecatedConf> deprecatedConfigs = new HashMap<>();

  public Map<String, DeprecatedConf> getConfigsWithAlternatives() {
    return configsWithAlternatives;
  }

  public Map<String, DeprecatedConf> getDeprecatedConfigs() {
    return deprecatedConfigs;
  }

  static enum DepConf implements DeprecatedConf {
    JOB_INITIAL_POLL_INTERVAL("job.initial_poll_interval", "0.4"),
    JOB_MAX_POLL_INTERVAL("job.max_poll_interval", "0.4");

    private final String key;
    private final String version;
    private final String deprecationMessage;

    private DepConf(String key, String version) {
      this(key, version, "");
    }

    private DepConf(String key, String version, String deprecationMessage) {
      this.key = HTTP_CONF_PREFIX + key;
      this.version = version;
      this.deprecationMessage = deprecationMessage;
    }

    @Override
    public String key() { return key; }

    @Override
    public String version() { return version; }

    @Override
    public String deprecationMessage() { return deprecationMessage; }
  }

}
