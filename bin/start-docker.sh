#!/usr/bin/env bash
##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
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

set -e
set -x

# Try out some standard docker machine names that could work
eval "$(docker-machine env docker-vm)"
eval "$(docker-machine env dinghy)"

BIN_DIR=$(pushd `dirname "$0"` 2>&1 > /dev/null && pwd && popd  2>&1 > /dev/null)
BASE_DIR=$(pushd "${BIN_DIR}/../" 2>&1 > /dev/null && pwd && popd  2>&1 > /dev/null)

${BIN_DIR}/copy-protobuf.sh

# Go into the base dir. This just makes things cleaner.
pushd ${BASE_DIR}

# Make sure that there is a thrid-party dir.
mkdir third-party || true

# Get gtest
# TODO(eclark): Remove this ( see HBASE-15427 )
if [[ ! -d third-party/googletest ]]; then
        git clone https://github.com/google/googletest.git third-party/googletest
fi

# We don't want to have to re-download all the jars in docker if we can help it
if [[ ! -d ~/.m2 ]]; then
    echo "~/.m2 directory doesn't exist. Check Apache Maven is installed."
    exit 1
fi;

# Build the image
# 
# This shouldn't be needed after the development environment is a little more stable.
docker build -t hbase_native .

# After the image is built run the thing
docker run -p 16010:16010/tcp \
           -e "JAVA_HOME=/usr/lib/jvm/java-8-oracle" \
           -v ${BASE_DIR}/..:/usr/src/hbase \
           -v ~/.m2:/root/.m2 \
           -it hbase_native  /bin/bash
popd
