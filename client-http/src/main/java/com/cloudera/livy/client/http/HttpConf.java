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

import java.util.Properties;

import com.cloudera.livy.client.common.ClientConf;

class HttpConf extends ClientConf<HttpConf> {

  static enum Entry implements ConfEntry {
    CONNECTION_TIMEOUT("connection.timeout", "10s"),
    CONNECTION_IDLE_TIMEOUT("connection.idle.timeout", "10m"),
    SOCKET_TIMEOUT("connection.socket.timeout", "5m"),

    JOB_INITIAL_POLL_INTERVAL("job.initial_poll_interval", "100ms"),
    JOB_MAX_POLL_INTERVAL("job.max_poll_interval", "5s"),

    CONTENT_COMPRESS_ENABLE("content.compress.enable", true);

    private final String key;
    private final Object dflt;

    private Entry(String key, Object dflt) {
      this.key = "livy.client.http." + key;
      this.dflt = dflt;
    }

    @Override
    public String key() { return key; }

    @Override
    public Object dflt() { return dflt; }
  }

  HttpConf(Properties config) {
    super(config);
  }

}
