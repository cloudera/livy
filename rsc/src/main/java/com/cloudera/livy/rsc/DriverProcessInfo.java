package com.cloudera.livy.rsc;

import io.netty.util.concurrent.Promise;

/**
 * Information about driver process and @{@link ContextInfo}
 */
public class DriverProcessInfo {

  private Promise<ContextInfo> contextInfo;
  private transient Process driverProcess;


  public DriverProcessInfo(Promise<ContextInfo> contextInfo, Process driverProcess) {
    this.contextInfo = contextInfo;
    this.driverProcess = driverProcess;
  }

  public Promise<ContextInfo> getContextInfo() {
    return contextInfo;
  }

  public Process getDriverProcess() {
    return driverProcess;
  }
}
