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

if [[ "true" = "${DEBUG}" ]]; then
  set -x
  printenv
fi

##To set jenkins Environment Variables:
export TOOLS_HOME=/home/jenkins/tools
#export JAVA_HOME=${JAVA_HOME_HADOOP_MACHINES_HOME}
export FINDBUGS_HOME=${TOOLS_HOME}/findbugs/latest
export CLOVER_HOME=${TOOLS_HOME}/clover/latest
#export MAVEN_HOME=${MAVEN_3_0_4_HOME}
export MAVEN_HOME=/home/jenkins/tools/maven/apache-maven-3.0.5

#export PATH=$PATH:${JAVA_HOME}/bin:${MAVEN_HOME}/bin:
export PATH=$PATH:${MAVEN_HOME}/bin:

YETUS_RELEASE=0.11.1
COMPONENT=${WORKSPACE}/component
TEST_FRAMEWORK=${WORKSPACE}/test_framework

PATCHPROCESS=${WORKSPACE}/patchprocess
if [[ -d ${PATCHPROCESS} ]]; then
  echo "[WARN] patch process already existed '${PATCHPROCESS}'"
  rm -rf "${PATCHPROCESS}"
fi
mkdir -p "${PATCHPROCESS}"


## Checking on H* machine nonsense
echo "JAVA_HOME: ${JAVA_HOME}"
ls -l "${JAVA_HOME}" || true
echo "MAVEN_HOME: ${MAVEN_HOME}"
echo "maven version:"
mvn --offline --version  || true
echo "getting machine specs, find in ${BUILD_URL}/artifact/patchprocess/machine/"
mkdir "${PATCHPROCESS}/machine"
cat /proc/cpuinfo >"${PATCHPROCESS}/machine/cpuinfo" 2>&1 || true
cat /proc/meminfo >"${PATCHPROCESS}/machine/meminfo" 2>&1 || true
cat /proc/diskstats >"${PATCHPROCESS}/machine/diskstats" 2>&1 || true
cat /sys/block/sda/stat >"${PATCHPROCESS}/machine/sys-block-sda-stat" 2>&1 || true
df -h >"${PATCHPROCESS}/machine/df-h" 2>&1 || true
ps -Awwf >"${PATCHPROCESS}/machine/ps-Awwf" 2>&1 || true
ifconfig -a >"${PATCHPROCESS}/machine/ifconfig-a" 2>&1 || true
lsblk -ta >"${PATCHPROCESS}/machine/lsblk-ta" 2>&1 || true
lsblk -fa >"${PATCHPROCESS}/machine/lsblk-fa" 2>&1 || true
cat /proc/loadavg >"${PATCHPROCESS}/loadavg" 2>&1 || true
ulimit -a >"${PATCHPROCESS}/machine/ulimit-a" 2>&1 || true

## /H*

### Download Yetus
if [[ "true" != "${USE_YETUS_PRERELEASE}" ]]; then
  if [ ! -d "${TEST_FRAMEWORK}/yetus-${YETUS_RELEASE}" ]; then
    mkdir -p "${TEST_FRAMEWORK}"
    cd "${TEST_FRAMEWORK}" || exit 1
    # clear out any cached 'use a prerelease' versions
    rm -rf apache-yetus-*

    mkdir -p "${TEST_FRAMEWORK}/.gpg"
    chmod -R 700 "${TEST_FRAMEWORK}/.gpg"

    curl -L --fail -o "${TEST_FRAMEWORK}/KEYS_YETUS" https://dist.apache.org/repos/dist/release/yetus/KEYS
    gpg --homedir "${TEST_FRAMEWORK}/.gpg" --import "${TEST_FRAMEWORK}/KEYS_YETUS"

    ## Release
    curl -L --fail -O "https://dist.apache.org/repos/dist/release/yetus/${YETUS_RELEASE}/apache-yetus-${YETUS_RELEASE}-bin.tar.gz"
    curl -L --fail -O "https://dist.apache.org/repos/dist/release/yetus/${YETUS_RELEASE}/apache-yetus-${YETUS_RELEASE}-bin.tar.gz.asc"
    gpg --homedir "${TEST_FRAMEWORK}/.gpg" --verify "apache-yetus-${YETUS_RELEASE}-bin.tar.gz.asc"
    tar xzpf "apache-yetus-${YETUS_RELEASE}-bin.tar.gz"
  fi
  TESTPATCHBIN=${TEST_FRAMEWORK}/apache-yetus-${YETUS_RELEASE}/bin/test-patch
  TESTPATCHLIB=${TEST_FRAMEWORK}/apache-yetus-${YETUS_RELEASE}/lib/precommit
else
  prerelease_dirs=("${TEST_FRAMEWORK}/${YETUS_PRERELEASE_GITHUB/\//-}-*")
  if [ ! -d "${prerelease_dirs[0]}" ]; then
    mkdir -p "${TEST_FRAMEWORK}"
    cd "${TEST_FRAMEWORK}" || exit
    ## from github
    curl -L --fail "https://api.github.com/repos/${YETUS_PRERELEASE_GITHUB}/tarball/HEAD" > yetus.tar.gz
    tar xvpf yetus.tar.gz
    prerelease_dirs=("${TEST_FRAMEWORK}/${YETUS_PRERELEASE_GITHUB/\//-}-*")
  fi
  TESTPATCHBIN=${prerelease_dirs[0]}/precommit/test-patch.sh
  TESTPATCHLIB=${prerelease_dirs[0]}/precommit
fi

if [[ "true" = "${DEBUG}" ]]; then
  # DEBUG print the test framework
  ls -l "${TESTPATCHBIN}"
  ls -la "${TESTPATCHLIB}/test-patch.d/"
  # DEBUG print the local customization
  if [ -d "${COMPONENT}/dev-support/test-patch.d" ]; then
    ls -la "${COMPONENT}/dev-support/test-patch.d/"
  fi
  YETUS_ARGS=(--debug "${YETUS_ARGS[@]}")
fi


if [ ! -x "${TESTPATCHBIN}" ] && [ -n "${TEST_FRAMEWORK}" ] && [ -d "${TEST_FRAMEWORK}" ]; then
  echo "Something is amiss with the test framework; removing it. please re-run."
  rm -rf "${TEST_FRAMEWORK}"
  exit 1
fi

cd "${WORKSPACE}" || exit


#
# Yetus *always* builds with JAVA_HOME, so no need to list it.
#
# non-docker-mode JDK:
#         --findbugs-home=/home/jenkins/tools/findbugs/latest \

# docker-mode:  (openjdk 7 added for free)
#         --findbugs-home=/usr \
#         --docker \
#         --multijdkdirs="/usr/lib/jvm/java-8-openjdk-amd64" \

if [[ "true" = "${RUN_IN_DOCKER}" ]]; then
  YETUS_ARGS=(
    --docker \
    "--multijdkdirs=/usr/lib/jvm/java-8-openjdk-amd64" \
    "--findbugs-home=/usr" \
    "${YETUS_ARGS[@]}" \
  )
  if [ -r "${COMPONENT}/dev-support/docker/Dockerfile" ]; then
    YETUS_ARGS=("--dockerfile=${COMPONENT}/dev-support/docker/Dockerfile" "${YETUS_ARGS[@]}")
  fi
else
  YETUS_ARGS=("--findbugs-home=/home/jenkins/tools/findbugs/latest" "${YETUS_ARGS[@]}")
fi

if [ -d "${COMPONENT}/dev-support/test-patch.d" ]; then
  YETUS_ARGS=("--user-plugins=${COMPONENT}/dev-support/test-patch.d" "${YETUS_ARGS[@]}")
fi

# I don't trust Yetus compat enough yet, so in prerelease mode, skip our personality.
# this should give us an incentive to update the Yetus exemplar for HBase periodically.
if [ -r "${COMPONENT}/dev-support/hbase-personality.sh" ] && [[ "true" != "${USE_YETUS_PRERELEASE}" ]] ; then
  YETUS_ARGS=("--personality=${COMPONENT}/dev-support/hbase-personality.sh" "${YETUS_ARGS[@]}")
fi

if [[ true == "${QUICK_HADOOPCHECK}" ]]; then
  YETUS_ARGS=("--quick-hadoopcheck" "${YETUS_ARGS[@]}")
fi

if [[ true == "${SKIP_ERRORPRONE}" ]]; then
  YETUS_ARGS=("--skip-errorprone" "${YETUS_ARGS[@]}")
fi

YETUS_ARGS=("--skip-dirs=dev-support" "${YETUS_ARGS[@]}")

/bin/bash "${TESTPATCHBIN}" \
        "${YETUS_ARGS[@]}" \
        --patch-dir="${PATCHPROCESS}" \
        --basedir="${COMPONENT}" \
        --mvn-custom-repos \
        --whitespace-eol-ignore-list=".*/generated/.*" \
        --whitespace-tabs-ignore-list=".*/generated/.*" \
        --jira-user=HBaseQA \
        --jira-password="${JIRA_PASSWORD}" \
        "HBASE-${ISSUE_NUM}"

find "${COMPONENT}" -name target -exec chmod -R u+w {} \;
