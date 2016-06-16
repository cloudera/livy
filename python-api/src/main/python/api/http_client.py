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

from client import Client
from urlparse import urlparse
from ConfigParser import SafeConfigParser

class HttpClient(Client):

    def __init__(self, uri=urlparse, config=SafeConfigParser):
        self._config = config
        self._uri = uri

    def submit(self, context):
        pass

    def add_file(self, uri=urlparse):
        pass

    def run(self, context):
        pass

    def upload_jar(self, input_file=file):
        pass

    def upload_file(self, input_file=file):
        pass

    def add_jar(self, uri=urlparse):
        pass
