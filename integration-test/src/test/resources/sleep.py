#!/usr/bin/env /python

import sys
import time
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
    sc = SparkContext(appName="Just sleep")

    time.sleep(int(sys.argv[1]))

    sc.stop()
