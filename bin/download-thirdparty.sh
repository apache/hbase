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

mkdir -p ${HBASE_TP_DIR}
cd ${HBASE_TP_DIR}


if [ ! -d ${HBASE_GTEST_DIR} ]; then
  echo "Fetching gtest"
  wget -c http://googletest.googlecode.com/files/gtest-${HBASE_GTEST_VERSION}.zip
  unzip gtest-${HBASE_GTEST_VERSION}.zip
  rm gtest-${HBASE_GTEST_VERSION}.zip
fi

if [ ! -d ${HBASE_PROTOBUF_DIR} ]; then
  echo "Fetching protobuf"
  wget -c http://protobuf.googlecode.com/files/protobuf-${HBASE_PROTOBUF_VERSION}.tar.gz
  tar xzf protobuf-${HBASE_PROTOBUF_VERSION}.tar.gz
  rm protobuf-${HBASE_PROTOBUF_VERSION}.tar.gz
fi

if [ ! -d $HBASE_LIBEV_DIR ]; then
  echo "Fetching libev"
  wget -c http://dist.schmorp.de/libev/libev-${HBASE_LIBEV_VERSION}.tar.gz
  tar zxf libev-${HBASE_LIBEV_VERSION}.tar.gz
  rm -rf libev-${HBASE_LIBEV_VERSION}.tar.gz
fi

if [ ! -d $HBASE_PREFIX/bin/cpplint.py ]; then
  echo "Fetching cpplint"
  mkdir -p $HBASE_PREFIX/bin/
  wget -O $HBASE_PREFIX/bin/cpplint.py http://google-styleguide.googlecode.com/svn/trunk/cpplint/cpplint.py
  chmod +x $HBASE_PREFIX/bin/cpplint.py
fi


echo "---------------"
echo "Thirdparty dependencies downloaded successfully"
