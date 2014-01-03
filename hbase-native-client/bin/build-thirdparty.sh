#! /usr/bin/env bash
#
#/**
# * Copyright The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

set -x
set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

source ${DIR}/../bin/hbase-client-env.sh

# On some systems, autotools installs libraries to lib64 rather than lib.  Fix
# this by setting up lib64 as a symlink to lib.  We have to do this step first
# to handle cases where one third-party library depends on another.
mkdir -p "${HBASE_PREFIX}/lib"
cd ${HBASE_PREFIX}
ln -sf lib "${HBASE_PREFIX}/lib64"

if [ ! -f gtest ]; then
  cd ${HBASE_GTEST_DIR}
  cmake .
  make -j4
  cd ..
  ln -sf ${HBASE_GTEST_DIR} gtest
fi

if [ ! -f ${HBASE_PREFIX}/lib/libprotobuf.a ]; then
  cd ${HBASE_PROTOBUF_DIR}
  ./configure --with-pic --disable-shared --prefix=${HBASE_PREFIX}
  make -j4 install
fi

if [ ! -f ${HBASE_PREFIX}/lib/libev.a ]; then
  cd ${HBASE_LIBEV_DIR}
  ./configure --with-pic --disable-shared --prefix=${HBASE_PREFIX}
  make -j4 install
fi

echo "---------------------"
echo "Thirdparty dependencies built and installed into $HBASE_PREFIX successfully"
