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

declare -i missing_env=0
# Validate params
for required_env in "TESTS" "TOOLS" "BASEDIR" "ARCHIVE_PATTERN_LIST" "OUTPUT_RELATIVE" \
                    "BRANCH_SPECIFIC_DOCKERFILE" "OUTPUTDIR" "PROJECT" "AUTHOR_IGNORE_LIST" \
                    "WHITESPACE_IGNORE_LIST" "BRANCH_NAME" "TESTS_FILTER" "DEBUG" \
                    "USE_YETUS_PRERELEASE" "WORKSPACE" "YETUS_RELEASE"; do
  if [ -z "${!required_env}" ]; then
    echo "[ERROR] Required environment variable '${required_env}' is not set."
    missing_env=${missing_env}+1
  fi
done

if [ ${missing_env} -gt 0 ]; then
  echo "[ERROR] Please set the required environment variables before invoking. If this error is " \
       "on Jenkins, then please file a JIRA about the error."
  exit 1
fi

YETUS_ARGS=()
if [[ -n "${MULTIJDK}" ]]; then
  YETUS_ARGS=("--multijdktests=compile,javadoc" "${YETUS_ARGS[@]}")
  YETUS_ARGS=("--multijdkdirs=${MULTIJDK}" "${YETUS_ARGS[@]}")
fi

if [[ -n "${SET_JAVA_HOME}" ]]; then
  YETUS_ARGS=("--java-home=${SET_JAVA_HOME}" "${YETUS_ARGS[@]}")
fi
YETUS_ARGS=("--plugins=${TESTS}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--personality=${TOOLS}/personality.sh" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--basedir=${BASEDIR}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--archive-list=${ARCHIVE_PATTERN_LIST}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--console-urls" "${YETUS_ARGS[@]}")
# YETUS-532, repeat this twice in case the fix is to update args rather than docs
YETUS_ARGS=("--build-url-patchdir=artifact/${OUTPUT_RELATIVE}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--build-url-artifacts=artifact/${OUTPUT_RELATIVE}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--docker" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--dockerfile=${BRANCH_SPECIFIC_DOCKERFILE}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--empty-patch" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--html-report-file=${OUTPUTDIR}/console-report.html" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--jenkins" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--mvn-custom-repos" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--patch-dir=${OUTPUTDIR}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--project=${PROJECT}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--resetrepo" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--author-ignore-list=${AUTHOR_IGNORE_LIST}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--whitespace-eol-ignore-list=${WHITESPACE_IGNORE_LIST}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--whitespace-tabs-ignore-list=${WHITESPACE_IGNORE_LIST}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--sentinel" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--branch=${BRANCH_NAME}" "${YETUS_ARGS[@]}")
YETUS_ARGS=("--tests-filter=${TESTS_FILTER}" "${YETUS_ARGS[@]}")

if [[ true == "${DEBUG}" ]]; then
  YETUS_ARGS=("--debug" "${YETUS_ARGS[@]}")
fi

rm -rf "${OUTPUTDIR}"
mkdir -p "${OUTPUTDIR}"
if [[ true !=  "${USE_YETUS_PRERELEASE}" ]]; then
  YETUS_ARGS=("--shelldocs=${WORKSPACE}/yetus-${YETUS_RELEASE}/bin/shelldocs" "${YETUS_ARGS[@]}")
  TESTPATCHBIN="${WORKSPACE}/yetus-${YETUS_RELEASE}/bin/test-patch"
else
  YETUS_ARGS=("--shelldocs=${WORKSPACE}/yetus-git/shelldocs/shelldocs.py" "${YETUS_ARGS[@]}")
  TESTPATCHBIN="${WORKSPACE}/yetus-git/precommit/test-patch.sh"
fi
echo "Launching yetus with command line:"
echo "${TESTPATCHBIN} ${YETUS_ARGS[*]}"

/usr/bin/env bash "${TESTPATCHBIN}" "${YETUS_ARGS[@]}"
