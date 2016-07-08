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

import org.apache.spark.launcher.SparkAppHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSCAppListener implements SparkAppHandle.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(RSCAppListener.class);
  private SparkAppHandle appHandle;
  private String appId;
  private SparkAppHandle.State appState;

  @Override
  public void stateChanged(SparkAppHandle handle) {
    this.appHandle = handle;
    this.appState = handle.getState();
  }

  @Override
  public void infoChanged(SparkAppHandle handle) {
    this.appHandle = handle;
    this.appId = handle.getAppId();
  }

  public String getAppId() {
    return appId;
  }

  public SparkAppHandle.State getAppState() {
    return appState;
  }

  public void stopApp() {
    LOG.info("Trying to stop app {}", appId);
    appHandle.stop();
  }

  public void disconnect() {
    LOG.info("Disconnect with app {}", appId);
    appHandle.disconnect();
  }
}
