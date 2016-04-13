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

package com.cloudera.livy.client.local.driver;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.spark.SparkConf;

import com.cloudera.livy.client.local.LocalConf;
import static com.cloudera.livy.client.local.LocalConf.Entry.*;

/**
 * The entry point for the RSC. Parses command line arguments and instantiates the correct
 * driver based on the configuration.
 *
 * The driver is expected to have a public constructor that takes a two parameters:
 * a SparkConf and a LocalConf.
 */
public final class RSCDriverBootstrapper {

  public static void main(String[] args) throws Exception {
    Properties props;

    switch (args.length) {
    case 0:
      props = System.getProperties();
      break;

    case 1:
      props = new Properties();
      Reader r = new InputStreamReader(new FileInputStream(args[0]), UTF_8);
      try {
        props.load(r);
      } finally {
        r.close();
      }
      break;

    default:
      throw new IllegalArgumentException("Too many arguments.");
    }

    SparkConf conf = new SparkConf(false);
    LocalConf livyConf = new LocalConf(null);

    for (String key : props.stringPropertyNames()) {
      String value = props.getProperty(key);
      if (key.startsWith(LocalConf.LIVY_SPARK_PREFIX)) {
        livyConf.set(key.substring(LocalConf.LIVY_SPARK_PREFIX.length()), value);
      } else if (key.startsWith(LocalConf.SPARK_CONF_PREFIX)) {
        conf.set(key, value);
      }
    }

    String driverClass = livyConf.get(DRIVER_CLASS);
    if (driverClass == null) {
      driverClass = RSCDriver.class.getName();
    }

    RSCDriver driver = (RSCDriver) Thread.currentThread()
      .getContextClassLoader()
      .loadClass(driverClass)
      .getConstructor(SparkConf.class, LocalConf.class)
      .newInstance(conf, livyConf);

    driver.run();
  }

}
