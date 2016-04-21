#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# You'll need a local installation of
# [Apache Yetus' precommit checker](http://yetus.apache.org/documentation/0.1.0/#yetus-precommit)
# to use this personality.
#
# Download from: http://yetus.apache.org/downloads/ . You can either grab the source artifact and
# build from it, or use the convenience binaries provided on that download page.
#
# To run against, e.g. HBASE-15074 you'd then do
# ```bash
# test-patch --personality=dev-support/hbase-personality.sh HBASE-15074
# ```
#
# If you want to skip the ~1 hour it'll take to do all the hadoop API checks, use
# ```bash
# test-patch  --plugins=all,-hadoopcheck --personality=dev-support/hbase-personality.sh HBASE-15074
# ````
#
# pass the `--jenkins` flag if you want to allow test-patch to destructively alter local working
# directory / branch in order to have things match what the issue patch requests.

personality_plugins "all"

function personality_globals
{
  #shellcheck disable=SC2034
  PROJECT_NAME=hbase
  #shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=master
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^HBASE-[0-9]+$'
  #shellcheck disable=SC2034
  GITHUB_REPO="apache/hbase"

  # TODO use PATCH_BRANCH to select hadoop versions to use.
  # All supported Hadoop versions that we want to test the compilation with
  HBASE_HADOOP_VERSIONS="2.4.0 2.4.1 2.5.0 2.5.1 2.5.2 2.6.1 2.6.2 2.6.3 2.7.1"

  # TODO use PATCH_BRANCH to select jdk versions to use.

  # Override the maven options
  MAVEN_OPTS="${MAVEN_OPTS:-"-Xmx3100M"}"

}

function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  extra="-DHBasePatchProcess"

  if [[ ${repostatus} == branch
     && ${testtype} == mvninstall ]];then
     personality_enqueue_module . ${extra}
     return
  fi

  if [[ ${testtype} = findbugs ]]; then
    for module in ${CHANGED_MODULES}; do
      # skip findbugs on hbase-shell and hbase-it. hbase-it has nothing
      # in src/main/java where findbugs goes to look
      if [[ ${module} == hbase-shell ]]; then
        continue
      elif [[ ${module} == hbase-it ]]; then
        continue
      else
        # shellcheck disable=SC2086
        personality_enqueue_module ${module} ${extra}
      fi
    done
    return
  fi

  # If EXCLUDE_TESTS_URL/INCLUDE_TESTS_URL is set, fetches the url
  # and sets -Dtest.exclude.pattern/-Dtest to exclude/include the
  # tests respectively.
  if [[ ${testtype} = unit ]]; then
    extra="${extra} -PrunAllTests"
    if [[ -n "$EXCLUDE_TESTS_URL" ]]; then
        wget "$EXCLUDE_TESTS_URL" -O "excludes"
        if [[ $? -eq 0 ]]; then
          excludes=$(cat excludes)
          if [[ -n "${excludes}" ]]; then
            extra="${extra} -Dtest.exclude.pattern=${excludes}"
          fi
        else
          echo "Wget error $? in fetching excludes file from url" \
               "${EXCLUDE_TESTS_URL}. Ignoring and proceeding."
        fi
    elif [[ -n "$INCLUDE_TESTS_URL" ]]; then
        wget "$INCLUDE_TESTS_URL" -O "includes"
        if [[ $? -eq 0 ]]; then
          includes=$(cat includes)
          if [[ -n "${includes}" ]]; then
            extra="${extra} -Dtest=${includes}"
          fi
        else
          echo "Wget error $? in fetching includes file from url" \
               "${INCLUDE_TESTS_URL}. Ignoring and proceeding."
        fi
    fi

    # Inject the jenkins build-id for our surefire invocations
    # Used by zombie detection stuff, even though we're not including that yet.
    if [ -n "${BUILD_ID}" ]; then
      extra="${extra} -Dbuild.id=${BUILD_ID}"
    fi
  fi

  for module in ${CHANGED_MODULES}; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}

###################################################
# Below here are our one-off tests specific to hbase.
# TODO break them into individual files so it's easier to maintain them?

# TODO line length check? could ignore all java files since checkstyle gets them.

###################################################

add_test_type hadoopcheck

function hadoopcheck_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test hadoopcheck
  fi
}

function hadoopcheck_rebuild
{
  local repostatus=$1
  local hadoopver
  local logfile
  local count
  local result=0

  if [[ "${repostatus}" = branch ]]; then
    return 0
  fi

  big_console_header "Compiling against various Hadoop versions"

  export MAVEN_OPTS="${MAVEN_OPTS}"
  for hadoopver in ${HBASE_HADOOP_VERSIONS}; do
    logfile="${PATCH_DIR}/patch-javac-${hadoopver}.txt"
    echo_and_redirect "${logfile}" \
      "${MAVEN}" clean install \
        -DskipTests -DHBasePatchProcess \
        -Dhadoop-two.version="${hadoopver}"
    count=$(${GREP} -c ERROR "${logfile}")
    if [[ ${count} -gt 0 ]]; then
      add_vote_table -1 hadoopcheck "Patch causes ${count} errors with Hadoop v${hadoopver}."
      ((result=result+1))
    fi
  done

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  add_vote_table +1 hadoopcheck "Patch does not cause any errors with Hadoop ${HBASE_HADOOP_VERSIONS}."
  return 0
}

######################################

# TODO if we need th protoc check, we probably need to check building all the modules that rely on hbase-protocol
add_test_type hbaseprotoc

function hbaseprotoc_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.proto$ ]]; then
    add_test hbaseprotoc
  fi
}

function hbaseprotoc_rebuild
{
  local i=0
  local fn
  local module
  local logfile
  local count
  local result

  if [[ "${repostatus}" = branch ]]; then
    return 0
  fi

  verify_needed_test hbaseprotoc
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "Patch HBase protoc plugin"

  start_clock


  personality_modules patch hbaseprotoc
  modules_workers patch hbaseprotoc compile -DskipTests -Pcompile-protobuf -X -DHBasePatchProcess

  # shellcheck disable=SC2153
  until [[ $i -eq ${#MODULE[@]} ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi
    module=${MODULE[$i]}
    fn=$(module_file_fragment "${module}")
    logfile="${PATCH_DIR}/patch-hbaseprotoc-${fn}.txt"

    count=$(${GREP} -c ERROR "${logfile}")

    if [[ ${count} -gt 0 ]]; then
      module_status ${i} -1 "patch-hbaseprotoc-${fn}.txt" "Patch generated "\
        "${count} new protoc errors in ${module}."
      ((result=result+1))
    fi
    ((i=i+1))
  done

  modules_messages patch hbaseprotoc true
  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

######################################

add_test_type hbaseanti

function hbaseanti_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test hbaseanti
  fi
}

function hbaseanti_patchfile
{
  local patchfile=$1
  local warnings
  local result

  verify_needed_test hbaseanti
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "Checking for known anti-patterns"

  start_clock

  warnings=$(${GREP} 'new TreeMap<byte.*()' "${patchfile}")
  if [[ ${warnings} -gt 0 ]]; then
    add_vote_table -1 hbaseanti "" "The patch appears to have anti-pattern where BYTES_COMPARATOR was omitted: ${warnings}."
    ((result=result+1))
  fi

  warnings=$(${GREP} 'import org.apache.hadoop.classification' "${patchfile}")
  if [[ ${warnings} -gt 0 ]]; then
    add_vote_table -1 hbaseanti "" "The patch appears use Hadoop classification instead of HBase: ${warnings}."
    ((result=result+1))
  fi

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  add_vote_table +1 hbaseanti "" "Patch does not have any anti-patterns."
  return 0
}

# Work around HBASE-15042
function mvnsite_filefilter
{
  local filename=$1

  if [[ ${BUILDTOOL} = maven ]]; then
    if [[ ${filename} =~ src/main/site || ${filename} =~ src/main/asciidoc ]]; then
      yetus_debug "tests/mvnsite: ${filename}"
      add_test mvnsite
    fi
  fi
}

## This is named so that yetus will check us right after running tests.
## Essentially, we check for normal failures and then we look for zombies.
#function hbase_unit_logfilter
#{
#  declare testtype="unit"
#  declare input=$1
#  declare output=$2
#  declare processes
#  declare process_output
#  declare zombies
#  declare zombie_count=0
#  declare zombie_process
#
#  yetus_debug "in hbase-specific unit logfilter."
#
#  # pass-through to whatever is counting actual failures
#  if declare -f ${BUILDTOOL}_${testtype}_logfilter >/dev/null; then
#    "${BUILDTOOL}_${testtype}_logfilter" "${input}" "${output}"
#  elif declare -f ${testtype}_logfilter >/dev/null; then
#    "${testtype}_logfilter" "${input}" "${output}"
#  fi
#
#  start_clock
#  if [ -n "${BUILD_ID}" ]; then
#    yetus_debug "Checking for zombie test processes."
#    processes=$(jps -v | "${GREP}" surefirebooter | "${GREP}" -e "hbase.build.id=${BUILD_ID}")
#    if [ -n "${processes}" ] && [ "$(echo "${processes}" | wc -l)" -gt 0 ]; then
#      yetus_warn "Found some suspicious process(es). Waiting a bit to see if they're just slow to stop."
#      yetus_debug "${processes}"
#      sleep 30
#      #shellcheck disable=SC2016
#      for pid in $(echo "${processes}"| ${AWK} '{print $1}'); do
#        # Test our zombie still running (and that it still an hbase build item)
#        process_output=$(ps -p "${pid}" | tail +2 | "${GREP}" -e "hbase.build.id=${BUILD_ID}")
#        if [[ -n "${process_output}" ]]; then
#          yetus_error "Zombie: ${process_output}"
#          ((zombie_count = zombie_count + 1))
#          zombie_process=$(jstack "${pid}" | "${GREP}" -e "\.Test" | "${GREP}" -e "\.java"| head -3)
#          zombies="${zombies} ${zombie_process}"
#        fi
#      done
#    fi
#    if [ "${zombie_count}" -ne 0 ]; then
#      add_vote_table -1 zombies "There are ${zombie_count} zombie test(s)"
#      populate_test_table "zombie unit tests" "${zombies}"
#    else
#      yetus_info "Zombie check complete. All test runs exited normally."
#      stop_clock
#    fi
#  else
#    add_vote_table -0 zombies "There is no BUILD_ID env variable; can't check for zombies."
#  fi
#
#}
