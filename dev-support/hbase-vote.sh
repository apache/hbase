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

set -e -o pipefail

usage() {
  SCRIPT=$(basename "${BASH_SOURCE[@]}")

  cat << __EOF
hbase-vote. A script for standard vote which verifies the following items
1. Checksum of sources and binaries
2. Signature of sources and binaries
3. Rat check
4. Built from source
5. Unit tests

Usage: ${SCRIPT} -s | --source <url> [-k | --key <signature>] [-f | --keys-file-url <url>] [-o | --output-dir </path/to/use>] [-P runSmallTests] [-D property[=value]]
       ${SCRIPT} -h | --help

  -h | --help                   Show this screen.
  -s | --source '<url>'         A URL pointing to the release candidate sources and binaries
                                e.g. https://dist.apache.org/repos/dist/dev/hbase/hbase-<version>RC0/
  -k | --key '<signature>'      A signature of the public key, e.g. 9AD2AE49
  -f | --keys-file-url '<url>'   the URL of the key file, default is
                                https://downloads.apache.org/hbase/KEYS
  -o | --output-dir '</path>'   directory which has the stdout and stderr of each verification target
  -P |                          list of maven profiles to activate for test UT/IT, i.e. <-P runSmallTests> Defaults to runAllTests
  -D |                          list of maven properties to set for the mvn invocations, i.e. <-D hadoop.profile=3.0 -D skipTests> Defaults to unset
__EOF
}

MVN_PROFILES=()
MVN_PROPERTIES=()

while ((${#})); do
  case "${1}" in
    -h | --help )
      usage; exit 0 ;;
    -s | --source )
      SOURCE_URL="${2}"; shift 2 ;;
    -k | --key )
      SIGNING_KEY="${2}"; shift 2 ;;
    -f | --keys-file-url )
      KEY_FILE_URL="${2}"; shift 2 ;;
    -o | --output-dir )
      OUTPUT_DIR="${2}"; shift 2 ;;
    -P )
      MVN_PROFILES+=("-P ${2}"); shift 2 ;;
    -D )
      MVN_PROPERTIES+=("-D ${2}"); shift 2 ;;
    * )
      usage >&2; exit 1             ;;
  esac
done

# Source url must be provided
if [ -z "${SOURCE_URL}" ]; then
  usage;
  exit 1
fi

cat << __EOF
Although This tool helps verifying HBase RC build and unit tests,
operator may still consider to verify the following manually
1. Verify the compat-check-report.html
2. Any on cluster Integration test or performance test, e.g. LTT load 100M or
   ITBLL 1B rows with serverKilling monkey
3. Other concerns if any
__EOF

[[ "${SOURCE_URL}" != */ ]] && SOURCE_URL="${SOURCE_URL}/"
HBASE_RC_VERSION=$(tr "/" "\n" <<< "${SOURCE_URL}" | tail -n2)
HBASE_VERSION=$(echo "${HBASE_RC_VERSION}" | sed -e 's/RC[0-9]//g' | sed -e 's/hbase-//g')
JAVA_VERSION=$(java -version 2>&1 | cut -f3 -d' ' | head -n1 | sed -e 's/"//g')
OUTPUT_DIR="${OUTPUT_DIR:-$(pwd)}"

if [ ! -d "${OUTPUT_DIR}" ]; then
    echo "Output directory ${OUTPUT_DIR} does not exist, please create it before running this script."
    exit 1
fi

# Maven profile must be provided
if [ ${#MVN_PROFILES[@]} -eq 0 ]; then
    MVN_PROFILES=("-P runAllTests")
fi

OUTPUT_PATH_PREFIX="${OUTPUT_DIR}"/"${HBASE_RC_VERSION}"

# default value for verification targets, 0 = failed
SIGNATURE_PASSED=0
CHECKSUM_PASSED=0
RAT_CHECK_PASSED=0
BUILD_FROM_SOURCE_PASSED=0
UNIT_TEST_PASSED=0

function download_and_import_keys() {
    KEY_FILE_URL="${KEY_FILE_URL:-https://downloads.apache.org/hbase/KEYS}"
    echo "Obtain and import the publisher key(s) from ${KEY_FILE_URL}"
    # download the keys file into file KEYS
    wget -O KEYS "${KEY_FILE_URL}"
    gpg --import KEYS
    if [ -n "${SIGNING_KEY}" ]; then
        gpg --list-keys "${SIGNING_KEY}"
    fi
}

function download_release_candidate () {
    # get all files from release candidate repo
    wget -r -np -N -nH --cut-dirs 4 "${SOURCE_URL}"
}

function verify_signatures() {
    rm -f "${OUTPUT_PATH_PREFIX}"_verify_signatures
    for file in *.tar.gz; do
        gpg --verify "${file}".asc "${file}" 2>&1 | tee -a "${OUTPUT_PATH_PREFIX}"_verify_signatures && SIGNATURE_PASSED=1 || SIGNATURE_PASSED=0
    done
}

function verify_checksums() {
    rm -f "${OUTPUT_PATH_PREFIX}"_verify_checksums
    SHA_EXT=$(find . -name "*.sha*" | awk -F '.' '{ print $NF }' | head -n 1)
    for file in *.tar.gz; do
        gpg --print-md SHA512 "${file}" > "${file}"."${SHA_EXT}".tmp
        diff "${file}"."${SHA_EXT}".tmp "${file}"."${SHA_EXT}" 2>&1 | tee -a "${OUTPUT_PATH_PREFIX}"_verify_checksums && CHECKSUM_PASSED=1 || CHECKSUM_PASSED=0
        rm -f "${file}"."${SHA_EXT}".tmp
    done
}

function unzip_from_source() {
    tar -zxvf hbase-"${HBASE_VERSION}"-src.tar.gz
    cd hbase-"${HBASE_VERSION}"
}

function rat_test() {
    rm -f "${OUTPUT_PATH_PREFIX}"_rat_test
    mvn clean apache-rat:check "${MVN_PROPERTIES[@]}" 2>&1 | tee "${OUTPUT_PATH_PREFIX}"_rat_test && RAT_CHECK_PASSED=1
}

function build_from_source() {
    rm -f "${OUTPUT_PATH_PREFIX}"_build_from_source
    # Hardcode skipTests for faster build. Testing is covered later.
    mvn clean install "${MVN_PROPERTIES[@]}" -DskipTests 2>&1 | tee "${OUTPUT_PATH_PREFIX}"_build_from_source && BUILD_FROM_SOURCE_PASSED=1
}

function run_tests() {
    rm -f "${OUTPUT_PATH_PREFIX}"_run_tests
    mvn package "${MVN_PROFILES[@]}" "${MVN_PROPERTIES[@]}" -Dsurefire.rerunFailingTestsCount=3 2>&1 | tee "${OUTPUT_PATH_PREFIX}"_run_tests && UNIT_TEST_PASSED=1
}

function execute() {
   ${1} || print_when_exit
}

function print_when_exit() {
  cat << __EOF
        * Signature: $( ((SIGNATURE_PASSED)) && echo "ok" || echo "failed" )
        * Checksum : $( ((CHECKSUM_PASSED)) && echo "ok" || echo "failed" )
        * Rat check (${JAVA_VERSION}): $( ((RAT_CHECK_PASSED)) && echo "ok" || echo "failed" )
         - mvn clean apache-rat:check ${MVN_PROPERTIES[@]}
        * Built from source (${JAVA_VERSION}): $( ((BUILD_FROM_SOURCE_PASSED)) && echo "ok" || echo "failed" )
         - mvn clean install ${MVN_PROPERTIES[@]} -DskipTests
        * Unit tests pass (${JAVA_VERSION}): $( ((UNIT_TEST_PASSED)) && echo "ok" || echo "failed" )
         - mvn package ${MVN_PROFILES[@]} ${MVN_PROPERTIES[@]} -Dsurefire.rerunFailingTestsCount=3
__EOF
  if ((CHECKSUM_PASSED)) && ((SIGNATURE_PASSED)) && ((RAT_CHECK_PASSED)) && ((BUILD_FROM_SOURCE_PASSED)) && ((UNIT_TEST_PASSED)) ; then
    exit 0
  fi
  exit 1
}

download_and_import_keys
download_release_candidate
pushd "${HBASE_RC_VERSION}"

execute verify_signatures
execute verify_checksums
execute unzip_from_source
execute rat_test
execute build_from_source
execute run_tests

popd

print_when_exit
