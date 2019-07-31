#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script generates the build info for Livy and places it into the livy-version-info.properties file.
# Arguments:
#   build_tgt_directory - The target directory where properties file would be created. [./server/target/extra-resources]
#   livy_version - The current version of livy

RESOURCE_DIR="$1"
mkdir -p "$RESOURCE_DIR"
LIVY_BUILD_INFO="${RESOURCE_DIR}"/livy-version-info.properties

echo_build_properties() {
  echo version=$1
  echo user=$USER
  echo revision=$(git rev-parse HEAD)
  echo branch=$(git rev-parse --abbrev-ref HEAD)
  echo date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo url=$(git config --get remote.origin.url)
}

echo_build_properties $2 > "$LIVY_BUILD_INFO"
