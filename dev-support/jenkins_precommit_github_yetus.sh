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

# place ourselves in the directory containing the hbase and yetus checkouts
cd "$(dirname "$0")/../.."
echo "executing from $(pwd)"

if [[ "true" = "${DEBUG}" ]]; then
  set -x
  printenv 2>&1 | sort
fi

declare -i missing_env=0
declare -a required_envs=(
  # these ENV variables define the required API with Jenkinsfile_GitHub
  "ARCHIVE_PATTERN_LIST"
  "DOCKERFILE"
  "GITHUB_PASSWORD"
  "GITHUB_USER"
  "PATCHDIR"
  "PLUGINS"
  "SET_JAVA_HOME"
  "SOURCEDIR"
  "TESTS_FILTER"
  "YETUSDIR"
  "AUTHOR_IGNORE_LIST"
  "BLANKS_EOL_IGNORE_FILE"
  "BLANKS_TABS_IGNORE_FILE"
)
# Validate params
for required_env in "${required_envs[@]}"; do
  if [ -z "${!required_env}" ]; then
    echo "[ERROR] Required environment variable '${required_env}' is not set."
    missing_env=${missing_env}+1
  fi
done

# BUILD_URL_ARTIFACTS is required for Jenkins but set in personality for GitHub Actions
if [[ "${GITHUB_ACTIONS}" != "true" ]] && [[ -z "${BUILD_URL_ARTIFACTS}" ]]; then
  echo "[ERROR] Required environment variable 'BUILD_URL_ARTIFACTS' is not set."
  missing_env=${missing_env}+1
fi

if [ ${missing_env} -gt 0 ]; then
  echo "[ERROR] Please set the required environment variables before invoking. If this error is " \
       "on Jenkins, then please file a JIRA about the error."
  exit 1
fi

# TODO (HBASE-23900): cannot assume test-patch runs directly from sources
TESTPATCHBIN="${YETUSDIR}/bin/test-patch"

# this must be clean for every run
rm -rf "${PATCHDIR}"
mkdir -p "${PATCHDIR}"

# Checking on H* machine nonsense
mkdir "${PATCHDIR}/machine"
"${SOURCEDIR}/dev-support/gather_machine_environment.sh" "${PATCHDIR}/machine"

# enable debug output for yetus
if [[ "true" = "${DEBUG}" ]]; then
  YETUS_ARGS+=("--debug")
fi
# If we're doing docker, make sure we don't accidentally pollute the image with a host java path
if [ -n "${JAVA_HOME}" ]; then
  unset JAVA_HOME
fi
YETUS_ARGS+=("--ignore-unknown-options=true")
YETUS_ARGS+=("--patch-dir=${PATCHDIR}")
# where the source is located
YETUS_ARGS+=("--basedir=${SOURCEDIR}")
# our project defaults come from a personality file
# which will get loaded automatically by setting the project name
YETUS_ARGS+=("--project=hbase")
# lots of different output formats
YETUS_ARGS+=("--brief-report-file=${PATCHDIR}/brief.txt")
YETUS_ARGS+=("--console-report-file=${PATCHDIR}/console.txt")
YETUS_ARGS+=("--html-report-file=${PATCHDIR}/report.html")
# enable writing back to Github
YETUS_ARGS+=("--github-token=${GITHUB_PASSWORD}")
# GitHub Actions fork PRs cannot write comments (GITHUB_TOKEN has no PR write permission)
# Jenkins can write comments via its own credentials
if [[ "${GITHUB_ACTIONS}" != "true" ]]; then
  YETUS_ARGS+=("--github-write-comment")
fi
# auto-kill any surefire stragglers during unit test runs
YETUS_ARGS+=("--reapermode=kill")
# set relatively high limits for ASF machines
# changing these to higher values may cause problems
# with other jobs on systemd-enabled machines
YETUS_ARGS+=("--dockermemlimit=20g")
# -1 spotbugs issues that show up prior to the patch being applied
YETUS_ARGS+=("--spotbugs-strict-precheck")
# rsync these files back into the archive dir
YETUS_ARGS+=("--archive-list=${ARCHIVE_PATTERN_LIST}")
# URL for user-side presentation in reports and such to our artifacts
if [[ -n "${BUILD_URL_ARTIFACTS}" ]]; then
  YETUS_ARGS+=("--build-url-artifacts=${BUILD_URL_ARTIFACTS}")
fi
# plugins to enable
YETUS_ARGS+=("--plugins=${PLUGINS},-findbugs")
# run in docker mode and specifically point to our
# Dockerfile since we don't want to use the auto-pulled version.
YETUS_ARGS+=("--docker")
YETUS_ARGS+=("--dockerfile=${DOCKERFILE}")
YETUS_ARGS+=("--mvn-custom-repos")
YETUS_ARGS+=("--java-home=${SET_JAVA_HOME}")
YETUS_ARGS+=("--author-ignore-list=${AUTHOR_IGNORE_LIST}")
YETUS_ARGS+=("--blanks-eol-ignore-file=${BLANKS_EOL_IGNORE_FILE}")
YETUS_ARGS+=("--blanks-tabs-ignore-file=${BLANKS_TABS_IGNORE_FILE}*")
YETUS_ARGS+=("--tests-filter=${TESTS_FILTER}")
YETUS_ARGS+=("--personality=${SOURCEDIR}/dev-support/hbase-personality.sh")
YETUS_ARGS+=("--quick-hadoopcheck")
if [[ "${SKIP_ERRORPRONE}" = "true" ]]; then
  # skip error prone
  YETUS_ARGS+=("--skip-errorprone")
fi
# Exclude non-code directories from module detection to avoid triggering full builds
YETUS_ARGS+=("--skip-dirs=dev-support,.github,bin,conf")
# For testing with specific hadoop version. Activates corresponding profile in maven runs.
if [[ -n "${HADOOP_PROFILE}" ]]; then
  # Master has only Hadoop3 support. We don't need to activate any profile.
  # The Jenkinsfile should not attempt to run any Hadoop2 tests.
  if [[ "${BRANCH_NAME}" =~ branch-2* ]]; then
    YETUS_ARGS+=("--hadoop-profile=${HADOOP_PROFILE}")
  fi
fi
if [[ -n "${EXCLUDE_TESTS_URL}" ]]; then
  YETUS_ARGS+=("--exclude-tests-url=${EXCLUDE_TESTS_URL}")
fi
# help keep the ASF boxes clean
YETUS_ARGS+=("--sentinel")
# use emoji vote so it is easier to find the broken line
YETUS_ARGS+=("--github-use-emoji-vote")
# pass asf nightlies url in
if [[ -n "${ASF_NIGHTLIES_GENERAL_CHECK_BASE}" ]]; then
  YETUS_ARGS+=("--asf-nightlies-general-check-base=${ASF_NIGHTLIES_GENERAL_CHECK_BASE}")
fi
# pass build parallelism in
if [[ -n "${BUILD_THREAD}" ]]; then
  YETUS_ARGS+=("--build-thread=${BUILD_THREAD}")
fi
if [[ -n "${SUREFIRE_FIRST_PART_FORK_COUNT}" ]]; then
  YETUS_ARGS+=("--surefire-first-part-fork-count=${SUREFIRE_FIRST_PART_FORK_COUNT}")
fi
if [[ -n "${SUREFIRE_SECOND_PART_FORK_COUNT}" ]]; then
  YETUS_ARGS+=("--surefire-second-part-fork-count=${SUREFIRE_SECOND_PART_FORK_COUNT}")
fi
if [[ -n "${JAVA8_HOME}" ]]; then
  YETUS_ARGS+=("--java8-home=${JAVA8_HOME}")
fi

echo "Launching yetus with command line:"
echo "${TESTPATCHBIN} ${YETUS_ARGS[*]}"

/usr/bin/env bash "${TESTPATCHBIN}" "${YETUS_ARGS[@]}"
