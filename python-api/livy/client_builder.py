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
from StringIO import StringIO
from client_factory import _ClientFactory
from urlparse import urlparse
import os

"""A builder for Livy clients."""
class ClientBuilder(object):

    _CONFIG_SECTION = 'env'
    _LIVY_CLIENT_CONF_DIR = "LIVY_CLIENT_CONF_DIR"

    def __init__(self, uri, load_defaults=True, conf_dict = dict()):
        """
        Creates a new builder that will load the config
        :param uri: Livy server uri generated from urlparse lib
        :type uri: ParseResult generated from urlparse lib

        >>> from urlparse import urlparse
        >>> uri = urlparse("http://test")

        :param boolean load_defaults: Default value is true. If true, loads the
            livy client configuration from the files 'livy-client.conf' and
            'spark-defaults.conf'. The path to the files should be defined in the
            'LIVY_CLIENT_CONF_DIR' environment variable
        :param dict: Default value is empty dict. The key-value pairs in the dict
            will be loaded to the config
        :returns An instance of the ClientBuilder
        """
        self._config = SafeConfigParser()
        self._load_config(load_defaults, conf_dict)
        self._set_uri(uri)

    def _set_uri(self, uri):
        self._config.set(self._CONFIG_SECTION, 'livy.uri', uri.geturl())

    def _set_conf(self, key, value):
        if value is not None:
            self._config.set(self._CONFIG_SECTION, key, value)
        else:
            self._config.remove_option('env', key)

    def _set_multiple_conf(self, conf_dict):
        if conf_dict is not None:
            for key, value in conf_dict.iteritems():
                self.set_conf(self._CONFIG_SECTION, key, value)

    def _load_config(self, load_defaults, conf_dict):
        self._config.add_section(self._CONFIG_SECTION)
        if load_defaults is True:
            self._handle_default_config_loading()
        self._set_multiple_conf(conf_dict)

    def _handle_default_config_loading(self):
        if os.environ.has_key(self._LIVY_CLIENT_CONF_DIR):
            config_dir = os.environ.get(self._LIVY_CLIENT_CONF_DIR)
            config_files = os.listdir(config_dir)
            default_conf_files = ['livy-client.conf', 'spark-defaults.conf']
            for config_file in config_files:
                if default_conf_files.__contains__(config_file):
                    with open(config_dir + "/" + config_file) as stream:
                        stream = StringIO("[" + self._CONFIG_SECTION + "]\n" + stream.read())
                        self._config.readfp(stream)

    def build(self):
        """
        :returns An instance of the Client based on the uri config provided
        :exception Throws an exceptions if the uri config is not supported by the _ClientFactory
        """
        client_factory = _ClientFactory()
        url_str = self._config.get(self._CONFIG_SECTION, 'livy.uri')
        client = client_factory.create_client(urlparse(url_str), self._config)
        if client is None:
            raise 'Uri not supported by any of the client factories', url_str
        return client


