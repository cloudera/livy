package com.cloudera.livy.spark

import com.cloudera.livy.LineBufferedProcess

object SparkProcess {
  def apply(process: Process): SparkProcess = {
    new SparkProcess(process)
  }
}

class SparkProcess(process: Process) extends LineBufferedProcess(process) {
}
