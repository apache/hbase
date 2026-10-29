#!/usr/bin/env bash
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

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
: "${JAVA_HOME:?Set JAVA_HOME to a JDK before running this test}"
test_home=$(mktemp -d)
trap 'rm -rf "$test_home"' EXIT
mkdir -p "$test_home/lib/client-facing-thirdparty" "$test_home/lib/slf4j2" \
  "$test_home/lib/shaded-clients" "$test_home/conf"
touch "$test_home/lib/hadoop-common.jar" \
  "$test_home/lib/slf4j-reload4j-1.7.36.jar" \
  "$test_home/lib/client-facing-thirdparty/slf4j-api-1.7.30.jar" \
  "$test_home/lib/client-facing-thirdparty/log4j-api-2.26.1.jar" \
  "$test_home/lib/client-facing-thirdparty/log4j-core-2.26.1.jar" \
  "$test_home/lib/client-facing-thirdparty/log4j-1.2-api-2.26.1.jar" \
  "$test_home/lib/client-facing-thirdparty/log4j-slf4j-impl-2.26.1.jar" \
  "$test_home/lib/slf4j2/slf4j-api-2.0.20.jar" \
  "$test_home/lib/slf4j2/log4j-slf4j2-impl-2.26.1.jar" \
  "$test_home/lib/shaded-clients/hbase-shaded-client.jar" \
  "$test_home/lib/shaded-clients/hbase-shaded-mapreduce.jar"

export HBASE_HOME="$test_home" \
  HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
export HBASE_CLASSPATH="$test_home/lib/slf4j-reload4j-1.7.36.jar"

contains() { [[ ":$1:" == *":$2:"* ]]; }

default_cp=$($repo_dir/bin/hbase classpath)
contains "$default_cp" "$test_home/lib/client-facing-thirdparty/slf4j-api-1.7.30.jar"
[[ "$default_cp" != *"slf4j2"* ]]

for command in classpath mapredcp; do
  cp=$($repo_dir/bin/hbase --slf4j2 "$command")
  contains "$cp" "$test_home/lib/slf4j2/slf4j-api-2.0.20.jar"
  contains "$cp" "$test_home/lib/slf4j2/log4j-slf4j2-impl-2.26.1.jar"
  contains "$cp" "$test_home/lib/client-facing-thirdparty/log4j-api-2.26.1.jar"
  contains "$cp" "$test_home/lib/client-facing-thirdparty/log4j-core-2.26.1.jar"
  contains "$cp" "$test_home/lib/client-facing-thirdparty/log4j-1.2-api-2.26.1.jar"
  [[ "$cp" != *"slf4j-api-1.7.30.jar"* ]]
  [[ "$cp" != *"log4j-slf4j-impl-2.26.1.jar"* ]]
  [[ "$cp" != *"slf4j-reload4j-1.7.36.jar"* ]]
done

rm "$test_home/lib/slf4j2/log4j-slf4j2-impl-2.26.1.jar"
if $repo_dir/bin/hbase --slf4j2 classpath > /dev/null 2>&1; then
  echo "classpath succeeded without the SLF4J 2 provider" >&2
  exit 1
fi

echo "SLF4J 2 classpath checks passed"
