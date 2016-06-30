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
import unittest
from livy.client import HttpClient
from urlparse import urlparse


class LivyTestCase(unittest.TestCase):

    def test_builder_default_config(self):
        client = HttpClient(urlparse("http://localhost:8080/"))
        self.assertTrue(type(client) is HttpClient)

    def test_builder_optional_config(self):
        client = HttpClient(urlparse("http://localhost:8080/"), False)
        self.assertTrue(type(client) is HttpClient)

if __name__ == '__main__':
    unittest.main()

