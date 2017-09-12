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
set -euo pipefail
IFS=$'\n\t'

# Copy the version.h generated from hbase-common/src/saveVersion.sh script via the mvn build
BIN_DIR=$(dirname "$0")
VERSION_SOURCE_DIR="${BIN_DIR}/../../hbase-common/target/generated-sources/native/utils/"
VERSION_DEST_DIR="${BIN_DIR}/../include/hbase/utils/"
cp $VERSION_SOURCE_DIR/* $VERSION_DEST_DIR/
