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
  echo "Usage: ${0} [options] TestSomeTestName [TestOtherTest...]"
  echo ""
  echo "    --repeat times                        number of times to repeat if successful"
  echo "    --force-timeout seconds               Seconds to wait before killing a given test run"
  echo "    --maven-local-repo /path/to/use       Path for maven artifacts while building"
  echo "    --surefire-fork-count                 set the fork-count. only useful if multiple " \
      "tests running (default 0.5C)"
  echo "    --log-output /path/to/use             path to directory to hold attempt log"
  echo "    --hadoop-profile profile              specify a value passed via -Dhadoop.profile"
  exit 1
}
# Get arguments
declare -i force_timeout=7200
declare fork_count="0.5C"
declare -i attempts=1
declare maven_repo="${HOME}/.m2/repository"
declare output="."
declare hadoop_profile
while [ $# -gt 0 ]
do
  case "$1" in
    --force-timeout) shift; force_timeout=$1; shift;;
    --maven-local-repo) shift; maven_repo=$1; shift;;
    --repeat) shift; attempts=$1; shift;;
    --log-output) shift; output=$1; shift;;
    --surefire-fork-count) shift; fork_count=$1; shift;;
    --hadoop-profile) shift; hadoop_profile=$1; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;  # terminate while loop
  esac
done

if [ "$#" -lt 1 ]; then
  usage
fi

function find_modules
{
  declare testmaybepattern=$1
  declare path
  while IFS= read -r -d $'\0' path; do
    while [ -n "${path}" ]; do
      path=$(dirname "${path}")
      if [ -f "${path}/pom.xml" ]; then
        echo "${path}"
        break
      fi
    done
  done < <(find . -name "${testmaybepattern}.java" -a -type f -a -not -path '*/target/*' -print0)
}

function echo_run_redirect
{
  declare log=$1
  shift
  echo "${*}" >"${log}"
  "${@}" >>"${log}" 2>&1
}

declare -a modules

for test in "${@}"; do
  for module in $(find_modules "${test}"); do
    if [[ ! "${modules[*]}" =~ ${module} ]]; then
      echo "adding module '${module}' to set."
      modules+=("${module}")
    fi
  done
done

declare -a mvn_module_arg
for module in "${modules[@]}"; do
  mvn_module_arg+=(-pl "${module}")
done

declare tests="${*}"
declare -a maven_args=('--batch-mode' "-Dmaven.repo.local=${maven_repo}" "-Dtest=${tests// /,}"
  '-Dsurefire.rerunFailingTestsCount=0' "-Dsurefire.parallel.forcedTimeout=${force_timeout}"
  '-Dsurefire.shutdown=kill' '-DtrimStackTrace=false' '-am' "${mvn_module_arg[@]}"
  "-DforkCount=${fork_count}" test)
if [[ -n "${hadoop_profile}" ]] ; then
  maven_args+=("-Dhadoop.profile=${hadoop_profile}")
fi

if [[ ! -d "${output}" ]] ; then
  mkdir -p "${output}"
fi

for attempt in $(seq "${attempts}"); do
  echo "Attempt ${attempt}" >&2
  echo_run_redirect "${output}/mvn_test.log" mvn "${maven_args[@]}"
done
