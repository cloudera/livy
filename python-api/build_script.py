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
import subprocess
import sys
import os


def remove_version_file(file_path):
    try:
        os.remove(file_path)
    except OSError:
        pass

arg = sys.argv[1]

if arg == 'install':
    return_code = subprocess.call(["python", "setup.py", "sdist"])
    remove_version_file(os.path.dirname(os.path.abspath(__file__)) +
        "/src/main/python/livy/_version.py")
    remove_version_file(os.path.dirname(os.path.abspath(__file__)) +
        "/versioneer.py")
elif arg == 'generate-sources':
    subprocess.call(["versioneer", "install"])



