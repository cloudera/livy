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
package com.cloudera.livy.client.local;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.sasl.Sasl;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalConf extends Configuration {

  private static final Logger LOG = LoggerFactory.getLogger(LocalConf.class);

  /**
   * Prefix for Livy configurations embedded in SparkConf properties, since SparkConf
   * disallows anything that does not start with "spark.".
   */
  static final String SPARK_CONF_PREFIX = "spark.__livy__.";

  private static final ImmutableMap<String, TimeUnit> TIME_SUFFIXES =
    ImmutableMap.<String, TimeUnit>builder()
      .put("us", TimeUnit.MICROSECONDS)
      .put("ms", TimeUnit.MILLISECONDS)
      .put("s", TimeUnit.SECONDS)
      .put("m", TimeUnit.MINUTES)
      .put("min", TimeUnit.MINUTES)
      .put("h", TimeUnit.HOURS)
      .put("d", TimeUnit.DAYS)
      .build();

  public static enum Entry {
    CLIENT_ID("client.auth.id", null),
    CLIENT_SECRET("client.auth.secret", null),
    CLIENT_IN_PROCESS("client.do_not_use.run_driver_in_process", null),

    LIVY_JARS("jars", null),

    RPC_SERVER_ADDRESS("rpc.server.address", null),
    RPC_CLIENT_HANDSHAKE_TIMEOUT("server.connect.timeout", "90000ms"),
    RPC_CLIENT_CONNECT_TIMEOUT("client.connect.timeout", "10000ms"),
    RPC_CHANNEL_LOG_LEVEL("channel.log.level", null),
    RPC_MAX_MESSAGE_SIZE("rpc.max.size", 50 * 1024 * 1024),
    RPC_MAX_THREADS("rpc.threads", 8),
    RPC_SECRET_RANDOM_BITS("secret.bits", 256),

    SASL_MECHANISMS("rpc.sasl.mechanisms", "DIGEST-MD5"),
    SASL_QOP("rpc.sasl.qop", null);

    public final String key;
    private final String defaultValStr;
    private final int defaultValInt;
    private final long defaultValLong;

    Entry(String key, Object defaultVal) {
      this.key = "livy.local." + key;
      if (defaultVal == null || defaultVal instanceof String) {
        this.defaultValStr = defaultVal == null ? null : defaultVal.toString();
        this.defaultValInt = -1;
        this.defaultValLong = -1;
      } else if (defaultVal instanceof Integer) {
        this.defaultValStr = null;
        this.defaultValInt = (Integer) defaultVal;
        this.defaultValLong = -1;
      } else if (defaultVal instanceof Long) {
        this.defaultValStr = null;
        this.defaultValInt = -1;
        this.defaultValLong = (Long) defaultVal;
      } else {
        throw new IllegalArgumentException("Not supported type value " + defaultVal.getClass() +
          " for key " + this.key);
      }
    }
  }

  public LocalConf(Properties config) {
    super(false);
    if (config != null) {
      for (String key : config.stringPropertyNames()) {
        set(key, config.getProperty(key));
      }
    }
  }

  public String get(Entry e) {
    return get(e.key, e.defaultValStr);
  }

  public int getInt(Entry e) {
    return getInt(e.key, e.defaultValInt);
  }

  public long getLong(Entry e) {
    return getLong(e.key, e.defaultValLong);
  }

  public LocalConf setAll(Configuration other) {
    for (Map.Entry<String, String> e : other) {
      set(e.getKey(), e.getValue());
    }
    return this;
  }

  public LocalConf set(Entry e, String value) {
    set(e.key, value);
    return this;
  }

  public long getTimeAsMs(Entry var) {
    String time = get(var);
    Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(time.toLowerCase());
    if (!m.matches()) {
      throw new NumberFormatException("Invalid time string: " + time);
    }

    long val = Long.parseLong(m.group(1));
    String suffix = m.group(2);

    if (suffix != null && !TIME_SUFFIXES.containsKey(suffix)) {
      throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
    }

    return TimeUnit.MILLISECONDS.convert(val,
      suffix != null ? TIME_SUFFIXES.get(suffix) : TimeUnit.MILLISECONDS);
  }

  public Map<String, String> getSaslOptions() {
    Map<String, String> opts = new HashMap<>();

    // TODO: add more options?
    Map<String, Entry> saslOpts = ImmutableMap.<String, Entry>builder()
      .put(Sasl.QOP, Entry.SASL_QOP)
      .build();

    for (Map.Entry<String, Entry> e : saslOpts.entrySet()) {
      String value = get(e.getValue());
      if (value != null) {
        opts.put(e.getKey(), value);
      }
    }
    return opts;
  }

  public String findLocalAddress() throws IOException {
    InetAddress address = InetAddress.getLocalHost();
    if (address.isLoopbackAddress()) {
      // Address resolves to something like 127.0.1.1, which happens on Debian;
      // try to find a better address using the local network interfaces
      Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
      while (ifaces.hasMoreElements()) {
        NetworkInterface ni = ifaces.nextElement();
        Enumeration<InetAddress> addrs = ni.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress addr = addrs.nextElement();
          if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
              && addr instanceof Inet4Address) {
            // We've found an address that looks reasonable!
            LOG.warn("Your hostname, {}, resolves to a loopback address; using {} "
                + " instead (on interface {})", address.getHostName(), addr.getHostAddress(),
                ni.getName());
            LOG.warn("Set '{}' if you need to bind to another address.",
              Entry.RPC_SERVER_ADDRESS.key);
            return addr.getHostAddress();
          }
        }
      }
    }

    LOG.warn("Your hostname, {}, resolves to a loopback address, but we couldn't find "
        + " any external IP address!", address.getHostName());
    LOG.warn("Set {} if you need to bind to another address.",
      Entry.RPC_SERVER_ADDRESS.key);
    return address.getHostName();
  }

}
