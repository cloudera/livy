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

from ConfigParser import SafeConfigParser
import inspect
from StringIO import StringIO
from urlparse import urlparse
import client_factory

class ClientBuilder(object):

    _CONFIG_SECTION = 'env'

    def __init__(self, load_defaults=True):
        self._config = SafeConfigParser()
        if load_defaults is True:
            self._handle_default_config_loading()
        else:
            self._config.add_section(self._CONFIG_SECTION)

    def _handle_default_config_loading(self):
        config_files = ['spark-defaults.conf', 'livy-client.conf']
        for config_file in config_files:
            with open(config_file) as stream:
                stream = StringIO("[" + self._CONFIG_SECTION + "]\n" + stream.read())
                self._config.readfp(stream)

    def set_uri(self, uri_str):
        self._config.set(self._CONFIG_SECTION, 'livy.uri', uri_str)
        return self

    def set_conf(self, key, value):
        if value is not None:
            self._config.set(self._CONFIG_SECTION, key, value)
        else:
            self._config.remove_option('env', key)
        return self

    def set_multiple_conf(self, conf_dict):
        if conf_dict is not None:
            for key, value in conf_dict.iteritems():
                self.set_conf(self._CONFIG_SECTION, key, value)
        return self

    def build(self):
        uri_str = self._config.get(self._CONFIG_SECTION, 'livy.uri')
        uri = urlparse(uri_str)
        abstract_factory = getattr(client_factory, "_ClientFactory")
        for name, cls in inspect.getmembers(client_factory, inspect.isclass):
            if cls.__class__.__module__ == '__builtin__':
                continue
            if inspect.isclass(cls) and not cls is abstract_factory and \
                    issubclass(cls, abstract_factory):
                factory_instance = cls()
                client = factory_instance.create_client(uri, self._config)
                if client is not None:
                    break
        if client is None:
            raise 'Uri not supported by any of the client factories', uri

        return client

