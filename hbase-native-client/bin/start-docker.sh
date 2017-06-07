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

# Build the image
docker build -t hbase_native .


mkdir third-party || true
if [[ ! -d third-party/googletest ]]; then
        git clone https://github.com/google/googletest.git third-party/googletest
fi

if [[ ! -d ~/.m2 ]]; then
    echo "~/.m2 directory doesn't exist. Check Apache Maven is installed."
    exit 1
fi;

docker run -p 16010:16010/tcp \
           -e "JAVA_HOME=/usr/lib/jvm/java-8-oracle" \
           -v ${PWD}/..:/usr/src/hbase \
           -v ~/.m2:/root/.m2 \
           -it hbase_native  /bin/bash
