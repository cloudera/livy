package com.cloudera.livy.repl;

import py4j.Gateway;
import py4j.Py4JException;
import py4j.reflection.PythonProxyHandler;

import java.lang.reflect.Proxy;

/**
 * Created by manikandan.nagarajan on 7/18/16.
 */
public class PySparkJobHelper {

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
