#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
function usage {
  echo "Usage: ${0} /path/for/output/dir"
  echo ""
  echo "  Gather info about a build machine that test harnesses should poll before running."
  echo "  presumes you'll then archive the passed output dir."

  exit 1
}

if [ "$#" -lt 1 ]; then
  usage
fi


declare output=$1

if [ ! -d "${output}" ] || [ ! -w "${output}" ]; then
  echo "Specified output directory must exist and be writable." >&2
  exit 1
fi

echo "getting machine specs, find in ${BUILD_URL}/artifact/${output}/"
echo "JAVA_HOME: ${JAVA_HOME}" >"${output}/java_home" 2>&1 || true
ls -l "${JAVA_HOME}" >"${output}/java_home_ls" 2>&1 || true
echo "MAVEN_HOME: ${MAVEN_HOME}" >"${output}/mvn_home" 2>&1 || true
mvn --offline --version  >"${output}/mvn_version" 2>&1 || true
cat /proc/cpuinfo >"${output}/cpuinfo" 2>&1 || true
cat /proc/meminfo >"${output}/meminfo" 2>&1 || true
cat /proc/diskstats >"${output}/diskstats" 2>&1 || true
cat /sys/block/sda/stat >"${output}/sys-block-sda-stat" 2>&1 || true
df -h >"${output}/df-h" 2>&1 || true
ps -Aww >"${output}/ps-Aww" 2>&1 || true
ifconfig -a >"${output}/ifconfig-a" 2>&1 || true
lsblk -ta >"${output}/lsblk-ta" 2>&1 || true
lsblk -fa >"${output}/lsblk-fa" 2>&1 || true
ulimit -a >"${output}/ulimit-a" 2>&1 || true
uptime >"${output}/uptime" 2>&1 || true
