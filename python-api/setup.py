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

from setuptools import setup
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
    'cloudpickle>=0.2.1',
    'configparser>=3.5.0',
    'future>=0.15.2',
    'futures>=3.0.5',
    'pytest-runner>=2.9',
    'requests>=2.10.0',
    'responses>=0.5.1',
    'twine>=1.6.5'
]

setup(
    name='livy-python-api',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=["livy", "tests"],
    package_dir={"": "src/main/python"},
    url='https://github.com/cloudera/livy',
    author_email='livy-user@cloudera.org',
    license='Apache License, Version 2.0',
    description=DESCRIPTION,
    long_description=long_description,
    platforms=['any'],
    keywords='livy pyspark development',
    classifiers=CLASSIFIERS,
    install_requires=requirements,
    setup_requires=['pytest-runner', 'flake8'],
    tests_require=['pytest']
)
