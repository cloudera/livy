/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.cloudera.hue.livy.client.conf;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RscConf extends Configuration {

  private static final Map<String, ConfVars> vars = new HashMap<String, ConfVars>();

  static {
    for (ConfVars confVar : ConfVars.values()) {
      vars.put(confVar.varname, confVar);
    }
  }

  public static enum ConfVars {
    SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT("hive.spark.client.server.connect.timeout",
      "90000ms"),
    SPARK_RPC_CLIENT_CONNECT_TIMEOUT("hive.spark.client.connect.timeout",
      "10000ms"),
    SPARK_RPC_CHANNEL_LOG_LEVEL("hive.spark.client.channel.log.level", null),
    SPARK_RPC_MAX_MESSAGE_SIZE("hive.spark.client.rpc.max.size", 50 * 1024 * 1024),
    SPARK_RPC_MAX_THREADS("hive.spark.client.rpc.threads", 8),
    SPARK_RPC_SECRET_RANDOM_BITS("hive.spark.client.secret.bits", "256");

    public final String varname;
    public final String defaultValStr;
    public final int defaultValInt;
    public final long defaultValLong;
    private final String defaultExpr;

    ConfVars(String varname, Object defaultVal) {
      this.defaultExpr = defaultVal == null ? null : String.valueOf(defaultVal);

      if (defaultVal == null || defaultVal instanceof String) {
        this.varname = varname;
        this.defaultValStr = defaultVal == null ? null : defaultVal.toString();
        this.defaultValInt = -1;
        this.defaultValLong = -1;
      } else if (defaultVal instanceof Integer) {
        this.varname = varname;
        this.defaultValStr = null;
        this.defaultValInt = (Integer) defaultVal;
        this.defaultValLong = -1;
      } else if (defaultVal instanceof Long) {
        this.varname = varname;
        this.defaultValStr = null;
        this.defaultValInt = -1;
        this.defaultValLong = (Long) defaultVal;
      } else {
        throw new IllegalArgumentException("Not supported type value " + defaultVal.getClass() +
          " for name " + varname);
      }
    }
  }

  public long getTimeVar(ConfVars var, TimeUnit outUnit) {
    long result;
    if (var.defaultValInt != -1) result = (long) var.defaultValInt;
    else if (var.defaultValStr != null) {
      String long_time = var.defaultValStr;
      long_time = long_time.replace("ms", "");
      result = Long.parseLong(long_time);
    } else result = var.defaultValLong;
    return result;
  }

  public static ConfVars getConfVars(String name) {
    return vars.get(name);
  }
}
