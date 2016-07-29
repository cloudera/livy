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

from setuptools import setup, find_packages
import os
import io
import versioneer

readme_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                           os.pardir))
with io.open(os.path.join(readme_path, 'README.rst'), encoding='utf-8') \
        as long_description_file:
    long_description = long_description_file.read()

DESCRIPTION = "A simple Python API for Livy powered by requests"

CLASSIFIERS = [
    'Development Status :: 1 - Planning',
    'Intended Audience :: Developers',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 2.7',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

requirements = [
    'pathlib2>=2.1.0',
    'futures>=3.0.5',
    'cloudpickle>=0.2.1',
    'requests>=2.10.0'
]

setup(
    name='livy-python-api',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(),
    url='https://github.com/manikandan89/livy',
    license='Cloudera Inc',
    description=DESCRIPTION,
    long_description=long_description,
    platforms=['any'],
    keywords='livy pyspark development',
    classifiers=CLASSIFIERS,
    install_requires=requirements,
    test_suite='tests'
)
