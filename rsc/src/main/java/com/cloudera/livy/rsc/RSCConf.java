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

package com.cloudera.livy.rsc;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.security.sasl.Sasl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.common.ClientConf;

public class RSCConf extends ClientConf<RSCConf> {

  public static final String SPARK_CONF_PREFIX = "spark.";
  public static final String LIVY_SPARK_PREFIX = SPARK_CONF_PREFIX + "__livy__.";

  private static final Logger LOG = LoggerFactory.getLogger(RSCConf.class);

  public static enum Entry implements ConfEntry {
    CLIENT_ID("client.auth.id", null),
    CLIENT_SECRET("client.auth.secret", null),
    CLIENT_IN_PROCESS("client.do_not_use.run_driver_in_process", false),
    CLIENT_SHUTDOWN_TIMEOUT("client.shutdown_timeout", "10s"),
    DRIVER_CLASS("driver_class", null),
    SESSION_KIND("session.kind", null),

    LIVY_JARS("jars", null),
    SPARKR_PACKAGE("sparkr.package", null),
    PYSPARK_ARCHIVES("pyspark.archives", null),

    // Address for the RSC driver to connect back with it's connection info.
    LAUNCHER_ADDRESS("launcher.address", null),
    LAUNCHER_PORT("launcher.port", -1),

    // How long will the RSC wait for a connection for a Livy server before shutting itself down.
    SERVER_IDLE_TIMEOUT("server.idle_timeout", "10m"),

    PROXY_USER("proxy_user", null),

    RPC_SERVER_ADDRESS("rpc.server.address", null),
    RPC_CLIENT_HANDSHAKE_TIMEOUT("server.connect.timeout", "90s"),
    RPC_CLIENT_CONNECT_TIMEOUT("client.connect.timeout", "10s"),
    RPC_CHANNEL_LOG_LEVEL("channel.log.level", null),
    RPC_MAX_MESSAGE_SIZE("rpc.max.size", 50 * 1024 * 1024),
    RPC_MAX_THREADS("rpc.threads", 8),
    RPC_SECRET_RANDOM_BITS("secret.bits", 256),

    SASL_MECHANISMS("rpc.sasl.mechanisms", "DIGEST-MD5"),
    SASL_QOP("rpc.sasl.qop", null),

    TEST_STUCK_END_SESSION("test.do_not_use.stuck_end_session", false),
    TEST_STUCK_START_DRIVER("test.do_not_use.stuck_start_driver", false),

    JOB_CANCEL_TRIGGER_INTERVAL("job_cancel.trigger_interval", "100ms"),
    JOB_CANCEL_TIMEOUT("job_cancel.timeout", "30s"),

    RETAINED_STATEMENT_NUMBER("retained_statements", 100);

    private final String key;
    private final Object dflt;

    private Entry(String key, Object dflt) {
      this.key = "livy.rsc." + key;
      this.dflt = dflt;
    }

    @Override
    public String key() { return key; }

    @Override
    public Object dflt() { return dflt; }
  }

  public RSCConf(Properties config) {
    super(config);
  }

  public Map<String, String> getSaslOptions() {
    Map<String, String> opts = new HashMap<>();

    // TODO: add more options?
    String qop = get(Entry.SASL_QOP);
    if (qop != null) {
      opts.put(Sasl.QOP, qop);
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
        + "any external IP address!", address.getCanonicalHostName());
    LOG.warn("Set {} if you need to bind to another address.",
      Entry.RPC_SERVER_ADDRESS.key);
    return address.getCanonicalHostName();
  }

}
