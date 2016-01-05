#!/bin/bash
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
##
# script to run protoc to generate protocol buf files.
# usage: ./build-proto.sh
#

which protoc
if [ $? != 0 ] ; then
  echo "Must have protoc compiler in your path to generate code"
  exit 1
fi

HBASE_DIR=`dirname $0`/..
PROTO_DIR=$HBASE_DIR/src/main/protobuf
JAVA_DIR=$HBASE_DIR/src/main/java

set -x
for f in $PROTO_DIR/*.proto ; do
  protoc -I$PROTO_DIR --java_out=$JAVA_DIR $f
done

