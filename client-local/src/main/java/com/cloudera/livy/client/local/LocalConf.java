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

package com.cloudera.livy.client.local;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.security.sasl.Sasl;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.common.ClientConf;

public class LocalConf extends ClientConf<LocalConf> {

  public static final String SPARK_CONF_PREFIX = "spark.";
  public static final String LIVY_SPARK_PREFIX = SPARK_CONF_PREFIX + "__livy__.";

  private static final Logger LOG = LoggerFactory.getLogger(LocalConf.class);

  public static enum Entry implements ConfEntry {
    CLIENT_ID("client.auth.id", null),
    CLIENT_SECRET("client.auth.secret", null),
    CLIENT_IN_PROCESS("client.do_not_use.run_driver_in_process", null),
    CLIENT_SHUTDOWN_TIMEOUT("client.shutdown_timeout", "10s"),
    CLIENT_REPL_MODE("repl", false),

    LIVY_JARS("jars", null),
    SPARKR_PACKAGE("sparkr.package", null),

    PROXY_USER("proxy_user", null),

    RPC_SERVER_ADDRESS("rpc.server.address", null),
    RPC_CLIENT_HANDSHAKE_TIMEOUT("server.connect.timeout", "90000ms"),
    RPC_CLIENT_CONNECT_TIMEOUT("client.connect.timeout", "10000ms"),
    RPC_CHANNEL_LOG_LEVEL("channel.log.level", null),
    RPC_MAX_MESSAGE_SIZE("rpc.max.size", 50 * 1024 * 1024),
    RPC_MAX_THREADS("rpc.threads", 8),
    RPC_SECRET_RANDOM_BITS("secret.bits", 256),

    SASL_MECHANISMS("rpc.sasl.mechanisms", "DIGEST-MD5"),
    SASL_QOP("rpc.sasl.qop", null);

    private final String key;
    private final Object dflt;

    private Entry(String key, Object dflt) {
      this.key = "livy.local." + key;
      this.dflt = dflt;
    }

    @Override
    public String key() { return key; }

    @Override
    public Object dflt() { return dflt; }
  }

  public LocalConf(Properties config) {
    super(config);
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
