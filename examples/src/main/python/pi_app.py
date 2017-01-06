from __future__ import print_function
#
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
from random import random
from operator import add

from livy.client import HttpClient

if __name__ == "__main__":
    """
        Usage: PiJob [livy url] [slices]
    """
    slices = int(sys.argv[2])
    samples = 100000 * slices

    client = HttpClient(sys.argv[1])

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    def pi_job(context):
        count = context.sc.parallelize(range(1, samples + 1), slices).map(f).reduce(add)
        return 4.0 * count / samples

    pi = client.submit(pi_job).result()

    print("Pi is roughly %f" % pi)
    client.stop(True)

