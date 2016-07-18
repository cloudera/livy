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
package com.cloudera.livy.repl;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.charset.Charset;

import py4j.Gateway;
import py4j.GatewayServer;
import py4j.Protocol;
import py4j.Py4JException;
import py4j.reflection.PythonProxyHandler;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class BypassPySparkJob implements Job<byte[]> {

    private final byte[] serializedJob;
    private GatewayServer gatewayServer;

    public BypassPySparkJob(byte[] serializedJob, GatewayServer gatewayServer) {
        this.serializedJob = serializedJob;
        this.gatewayServer = gatewayServer;
    }

    @Override
    public byte[] call(JobContext jc) throws Exception {
        Field f = null;
        Gateway gateway = null;
        f = gatewayServer.getClass().getDeclaredField("gateway");
        f.setAccessible(true);
        gateway = (Gateway) f.get(gatewayServer);
        String fakeCommandPart = "f" + Protocol.ENTRY_POINT_OBJECT_ID + ";" +
                "com.cloudera.livy.repl.BypassPySparkJobProcessor";
        BypassPySparkJobProcessor processor =
                (BypassPySparkJobProcessor)getPythonProxy(fakeCommandPart, gateway);
        String value = processor.process(serializedJob);
        return value != null ? value.getBytes(Charset.forName("UTF-8")) : null;
    }

    // This method is a hack to get around the classLoader issues faced in py4j 0.8.2.1 for
    // dynamically adding jars to the driver. The change is to use the context classLoader instead
    // of the system classLoader when initiating a new Proxy instance
    // ISSUE - https://issues.apache.org/jira/browse/SPARK-6047
    // FIX - https://github.com/bartdag/py4j/pull/196
    public static Object getPythonProxy(String commandPart, Gateway gateway) {
        String proxyString = commandPart.substring(1, commandPart.length());
        String[] parts = proxyString.split(";");
        int length = parts.length;
        Class[] interfaces = new Class[length - 1];
        if(length < 2) {
            throw new Py4JException("Invalid Python Proxy.");
        } else {
            for(int proxy = 1; proxy < length; ++proxy) {
                try {
                    interfaces[proxy - 1] = Class.forName(parts[proxy]);
                    if(!interfaces[proxy - 1].isInterface()) {
                        throw new Py4JException("This class " + parts[proxy]
                                + " is not an interface and cannot be used as a Python Proxy.");
                    }
                } catch (ClassNotFoundException var8) {
                    throw new Py4JException("Invalid interface name: " + parts[proxy]);
                }
            }
            Object var9 = Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                    interfaces, new PythonProxyHandler(parts[0],
                            gateway.getCallbackClient(), gateway));
            return var9;
        }
    }
}
