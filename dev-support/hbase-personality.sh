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
# pass the `--sentinel` flag if you want to allow test-patch to destructively alter local working
# directory / branch in order to have things match what the issue patch requests.

personality_plugins "all"

if ! declare -f "yetus_info" >/dev/null; then

  function yetus_info
  {
    echo "[$(date) INFO]: $*" 1>&2
  }

fi

# work around yetus overwriting JAVA_HOME from our docker image
function docker_do_env_adds
{
  declare k

  for k in "${DOCKER_EXTRAENVS[@]}"; do
    if [[ "JAVA_HOME" == "${k}" ]]; then
      if [ -n "${JAVA_HOME}" ]; then
        DOCKER_EXTRAARGS+=("--env=JAVA_HOME=${JAVA_HOME}")
      fi
    else
      DOCKER_EXTRAARGS+=("--env=${k}=${!k}")
    fi
  done
}


## @description  Globals specific to this personality
## @audience     private
## @stability    evolving
function personality_globals
{
  BUILDTOOL=maven
  #shellcheck disable=SC2034
  PROJECT_NAME=hbase
  #shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=master
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^HBASE-[0-9]+$'
  #shellcheck disable=SC2034
  GITHUB_REPO="apache/hbase"

  # TODO use PATCH_BRANCH to select jdk versions to use.

  # Yetus 0.7.0 enforces limits. Default proclimit is 1000.
  # Up it. See HBASE-25081 for how we arrived at this number.
  #shellcheck disable=SC2034
  PROC_LIMIT=30000

  # Set docker container to run with 20g. Default is 4g in yetus.
  # See HBASE-19902 for how we arrived at 20g.
  #shellcheck disable=SC2034
  DOCKERMEMLIMIT=20g
}

## @description  Parse extra arguments required by personalities, if any.
## @audience     private
## @stability    evolving
function personality_parse_args
{
  declare i

  for i in "$@"; do
    case ${i} in
      --exclude-tests-url=*)
        delete_parameter "${i}"
        EXCLUDE_TESTS_URL=${i#*=}
      ;;
      --include-tests-url=*)
        delete_parameter "${i}"
        INCLUDE_TESTS_URL=${i#*=}
      ;;
      --hadoop-profile=*)
        delete_parameter "${i}"
        HADOOP_PROFILE=${i#*=}
      ;;
      --skip-errorprone)
        delete_parameter "${i}"
        SKIP_ERRORPRONE=true
      ;;
      --asf-nightlies-general-check-base=*)
        delete_parameter "${i}"
        ASF_NIGHTLIES_GENERAL_CHECK_BASE=${i#*=}
      ;;
      --build-thread=*)
        delete_parameter "${i}"
        BUILD_THREAD=${i#*=}
      ;;
      --surefire-first-part-fork-count=*)
        delete_parameter "${i}"
        SUREFIRE_FIRST_PART_FORK_COUNT=${i#*=}
      ;;
      --surefire-second-part-fork-count=*)
        delete_parameter "${i}"
        SUREFIRE_SECOND_PART_FORK_COUNT=${i#*=}
      ;;
    esac
  done
}

## @description  Queue up modules for this personality
## @audience     private
## @stability    evolving
## @param        repostatus
## @param        testtype
function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""
  local branch1jdk8=()
  local jdk8module=""
  local MODULES=("${CHANGED_MODULES[@]}")

  yetus_info "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  # At a few points, hbase modules can run build, test, etc. in parallel
  # Let it happen. Means we'll use more CPU but should be for short bursts.
  # https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3
  if [[ -n "${BUILD_THREAD}" ]]; then
    extra="--threads=${BUILD_THREAD}"
  else
    extra="--threads=2"
  fi

  # Set java.io.tmpdir to avoid exhausting the /tmp space
  # Just simply set to 'target', it is not very critical so we do not care
  # whether it is placed in the root directory or a sub module's directory
  # let's make it absolute
  tmpdir=$(realpath target)
  extra="${extra} -Djava.io.tmpdir=${tmpdir} -DHBasePatchProcess"

  if [[ "${PATCH_BRANCH}" = branch-1* ]]; then
    extra="${extra} -Dhttps.protocols=TLSv1.2"
  fi

  # If we have HADOOP_PROFILE specified and we're on branch-2.x, pass along
  # the hadoop.profile system property. Ensures that Hadoop2 and Hadoop3
  # logic is not both activated within Maven.
  if [[ -n "${HADOOP_PROFILE}" ]] && [[ "${PATCH_BRANCH}" = branch-2* ]] ; then
    extra="${extra} -Dhadoop.profile=${HADOOP_PROFILE}"
  fi

  # BUILDMODE value is 'full' when there is no patch to be tested, and we are running checks on
  # full source code instead. In this case, do full compiles, tests, etc instead of per
  # module.
  # Used in nightly runs.
  # If BUILDMODE is 'patch', for unit and compile testtypes, there is no need to run individual
  # modules if root is included. HBASE-18505
  if [[ "${BUILDMODE}" == "full" ]] || \
     { { [[ "${testtype}" == unit ]] || [[ "${testtype}" == compile ]] || [[ "${testtype}" == checkstyle ]]; } && \
     [[ "${MODULES[*]}" =~ \. ]]; }; then
    MODULES=(.)
  fi

  # If the checkstyle configs change, check everything.
  if [[ "${testtype}" == checkstyle ]] && [[ "${MODULES[*]}" =~ hbase-checkstyle ]]; then
    MODULES=(.)
  fi

  if [[ ${testtype} == mvninstall ]]; then
    # shellcheck disable=SC2086
    personality_enqueue_module . ${extra}
    return
  fi

  # This list should include any modules that require jdk8. Maven should be configured to only
  # include them when a proper JDK is in use, but that doesn' work if we specifically ask for the
  # module to build as yetus does if something changes in the module.  Rather than try to
  # figure out what jdk is in use so we can duplicate the module activation logic, just
  # build at the top level if anything changes in one of these modules and let maven sort it out.
  branch1jdk8=(hbase-error-prone hbase-tinylfu-blockcache)
  if [[ "${PATCH_BRANCH}" = branch-1* ]]; then
    for jdk8module in "${branch1jdk8[@]}"; do
      if [[ "${MODULES[*]}" =~ ${jdk8module} ]]; then
        MODULES=(.)
        break
      fi
    done
  fi

  if [[ ${testtype} == spotbugs ]]; then
    # Run spotbugs on each module individually to diff pre-patch and post-patch results and
    # report new warnings for changed modules only.
    # For some reason, spotbugs on root is not working, but running on individual modules is
    # working. For time being, let it run on original list of CHANGED_MODULES. HBASE-19491
    for module in "${CHANGED_MODULES[@]}"; do
      # skip spotbugs on any module that lacks content in `src/main/java`
      if [[ "$(find "${BASEDIR}/${module}" -iname '*.java' -and -ipath '*/src/main/java/*' \
          -type f | wc -l | tr -d '[:space:]')" -eq 0 ]]; then
        yetus_debug "no java files found under ${module}/src/main/java. skipping."
        continue
      else
        # shellcheck disable=SC2086
        personality_enqueue_module ${module} ${extra}
      fi
    done
    return
  fi

  if [[ ${testtype} == compile ]] && [[ "${SKIP_ERRORPRONE}" != "true" ]] &&
      [[ "${PATCH_BRANCH}" != branch-1* ]] ; then
    extra="${extra} -PerrorProne"
  fi

  # If EXCLUDE_TESTS_URL/INCLUDE_TESTS_URL is set, fetches the url
  # and sets -Dtest.exclude.pattern/-Dtest to exclude/include the
  # tests respectively.
  if [[ ${testtype} == unit ]]; then
    local tests_arg=""
    get_include_exclude_tests_arg tests_arg
    extra="${extra} -PrunAllTests ${tests_arg}"

    # Inject the jenkins build-id for our surefire invocations
    # Used by zombie detection stuff, even though we're not including that yet.
    if [ -n "${BUILD_ID}" ]; then
      extra="${extra} -Dbuild.id=${BUILD_ID}"
    fi

    # set forkCount
    if [[ -n "${SUREFIRE_FIRST_PART_FORK_COUNT}" ]]; then
      extra="${extra} -Dsurefire.firstPartForkCount=${SUREFIRE_FIRST_PART_FORK_COUNT}"
    fi

    if [[ -n "${SUREFIRE_SECOND_PART_FORK_COUNT}" ]]; then
      extra="${extra} -Dsurefire.secondPartForkCount=${SUREFIRE_SECOND_PART_FORK_COUNT}"
    fi

    # If the set of changed files includes CommonFSUtils then add the hbase-server
    # module to the set of modules (if not already included) to be tested
    for f in "${CHANGED_FILES[@]}"
    do
      if [[ "${f}" =~ CommonFSUtils ]]; then
        if [[ ! "${MODULES[*]}" =~ hbase-server ]] && [[ ! "${MODULES[*]}" =~ \. ]]; then
          MODULES+=("hbase-server")
        fi
        break
      fi
    done
  fi

  for module in "${MODULES[@]}"; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}

## @description places where we override the built in assumptions about what tests to run
## @audience    private
## @stability   evolving
## @param       filename of changed file
function personality_file_tests
{
  local filename=$1
  yetus_debug "HBase specific personality_file_tests"
  # If the change is to the refguide, then we don't need any builtin yetus tests
  # the refguide test (below) will suffice for coverage.
  if [[ ${filename} =~ src/main/asciidoc ]] ||
     [[ ${filename} =~ src/main/xslt ]]; then
    yetus_debug "Skipping builtin yetus checks for ${filename}. refguide test should pick it up."
  else
    # If we change our asciidoc, rebuild mvnsite
    if [[ ${BUILDTOOL} = maven ]]; then
      if [[ ${filename} =~ src/site || ${filename} =~ src/main/asciidoc ]]; then
        yetus_debug "tests/mvnsite: ${filename}"
        add_test mvnsite
      fi
    fi
    # If we change checkstyle configs, run checkstyle
    if [[ ${filename} =~ checkstyle.*\.xml ]]; then
      yetus_debug "tests/checkstyle: ${filename}"
      add_test checkstyle
    fi
    # fallback to checking which tests based on what yetus would do by default
    if declare -f "${BUILDTOOL}_builtin_personality_file_tests" >/dev/null; then
      "${BUILDTOOL}_builtin_personality_file_tests" "${filename}"
    elif declare -f builtin_personality_file_tests >/dev/null; then
      builtin_personality_file_tests "${filename}"
    fi
  fi
}

## @description  Uses relevant include/exclude env variable to fetch list of included/excluded
#                tests and sets given variable to arguments to be passes to maven command.
## @audience     private
## @stability    evolving
## @param        name of variable to set with maven arguments
function get_include_exclude_tests_arg
{
  local  __resultvar=$1
  yetus_info "EXCLUDE_TESTS_URL=${EXCLUDE_TESTS_URL}"
  yetus_info "INCLUDE_TESTS_URL=${INCLUDE_TESTS_URL}"
  if [[ -n "${EXCLUDE_TESTS_URL}" ]]; then
      if wget "${EXCLUDE_TESTS_URL}" -O "excludes"; then
        excludes=$(cat excludes)
        yetus_debug "excludes=${excludes}"
        if [[ -n "${excludes}" ]]; then
          eval "${__resultvar}='-Dtest.exclude.pattern=${excludes}'"
        fi
        rm excludes
      else
        yetus_error "Wget error $? in fetching excludes file from url" \
             "${EXCLUDE_TESTS_URL}. Ignoring and proceeding."
      fi
  elif [[ -n "$INCLUDE_TESTS_URL" ]]; then
      if wget "$INCLUDE_TESTS_URL" -O "includes"; then
        includes=$(cat includes)
        yetus_debug "includes=${includes}"
        if [[ -n "${includes}" ]]; then
          eval "${__resultvar}='-Dtest=${includes}'"
        fi
        rm includes
      else
        yetus_error "Wget error $? in fetching includes file from url" \
             "${INCLUDE_TESTS_URL}. Ignoring and proceeding."
      fi
  else
    # Use branch specific exclude list when EXCLUDE_TESTS_URL and INCLUDE_TESTS_URL are empty
    FLAKY_URL="https://ci-hadoop.apache.org/job/HBase/job/HBase-Find-Flaky-Tests/job/${PATCH_BRANCH}/lastSuccessfulBuild/artifact/output/excludes"
    if wget "${FLAKY_URL}" -O "excludes"; then
      excludes=$(cat excludes)
        yetus_debug "excludes=${excludes}"
        if [[ -n "${excludes}" ]]; then
          eval "${__resultvar}='-Dtest.exclude.pattern=${excludes}'"
        fi
        rm excludes
      else
        yetus_error "Wget error $? in fetching excludes file from url" \
             "${FLAKY_URL}. Ignoring and proceeding."
      fi
  fi
}

###################################################
# Below here are our one-off tests specific to hbase.
# TODO break them into individual files so it's easier to maintain them?

# TODO line length check? could ignore all java files since checkstyle gets them.

###################################################

add_test_type refguide

function refguide_initialize
{
  maven_add_install refguide
}

function refguide_filefilter
{
  local filename=$1

  if [[ ${filename} =~ src/main/asciidoc ]] ||
     [[ ${filename} =~ src/main/xslt ]] ||
     [[ ${filename} =~ hbase-common/src/main/resources/hbase-default\.xml ]]; then
    add_test refguide
  fi
}

function refguide_rebuild
{
  local repostatus=$1
  local logfile="${PATCH_DIR}/${repostatus}-refguide.log"
  declare -i count
  declare pdf_output

  if ! verify_needed_test refguide; then
    return 0
  fi

  big_console_header "Checking we can create the ref guide on ${repostatus}"

  start_clock

  # disabled because "maven_executor" needs to return both command and args
  # shellcheck disable=2046
  echo_and_redirect "${logfile}" \
    $(maven_executor) clean site --batch-mode \
      -pl . \
      -Dtest=NoUnitTests -DHBasePatchProcess -Prelease \
      -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true

  count=$(${GREP} -c '\[ERROR\]' "${logfile}")
  if [[ ${count} -gt 0 ]]; then
    add_vote_table -1 refguide "${repostatus} has ${count} errors when building the reference guide."
    add_footer_table refguide "@@BASE@@/${repostatus}-refguide.log"
    return 1
  fi

  if ! mv target/site "${PATCH_DIR}/${repostatus}-site"; then
    add_vote_table -1 refguide "${repostatus} failed to produce a site directory."
    add_footer_table refguide "@@BASE@@/${repostatus}-refguide.log"
    return 1
  fi

  if [[ ! -f "${PATCH_DIR}/${repostatus}-site/book.html" ]]; then
    add_vote_table -1 refguide "${repostatus} failed to produce the html version of the reference guide."
    add_footer_table refguide "@@BASE@@/${repostatus}-refguide.log"
    return 1
  fi

  if [[ "${PATCH_BRANCH}" = branch-1* ]]; then
    pdf_output="book.pdf"
  else
    pdf_output="apache_hbase_reference_guide.pdf"
  fi

  if [[ ! -f "${PATCH_DIR}/${repostatus}-site/${pdf_output}" ]]; then
    add_vote_table -1 refguide "${repostatus} failed to produce the pdf version of the reference guide."
    add_footer_table refguide "@@BASE@@/${repostatus}-refguide.log"
    return 1
  fi

  add_vote_table 0 refguide "${repostatus} has no errors when building the reference guide. See footer for rendered docs, which you should manually inspect."
  if [[ -n "${ASF_NIGHTLIES_GENERAL_CHECK_BASE}" ]]; then
    add_footer_table refguide "${ASF_NIGHTLIES_GENERAL_CHECK_BASE}/${repostatus}-site/book.html"
  else
    add_footer_table refguide "@@BASE@@/${repostatus}-site/book.html"
  fi
  return 0
}

add_test_type shadedjars


function shadedjars_initialize
{
  yetus_debug "initializing shaded client checks."
  maven_add_install shadedjars
}

## @description  only run the test if java changes.
## @audience     private
## @stability    evolving
## @param        filename
function shadedjars_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]] || [[ ${filename} =~ pom.xml$ ]]; then
    add_test shadedjars
  fi
}

## @description test the shaded client artifacts
## @audience private
## @stability evolving
## @param repostatus
function shadedjars_rebuild
{
  local repostatus=$1
  local logfile="${PATCH_DIR}/${repostatus}-shadedjars.txt"

  if ! verify_needed_test shadedjars; then
    return 0
  fi

  big_console_header "Checking shaded client builds on ${repostatus}"

  start_clock

  local -a maven_args=('clean' 'verify' '-fae' '--batch-mode'
    '-pl' 'hbase-shaded/hbase-shaded-check-invariants' '-am'
    '-DskipTests' '-DHBasePatchProcess' '-Prelease'
    '-Dmaven.javadoc.skip=true' '-Dcheckstyle.skip=true' '-Dspotbugs.skip=true')
  # If we have HADOOP_PROFILE specified and we're on branch-2.x, pass along
  # the hadoop.profile system property. Ensures that Hadoop2 and Hadoop3
  # logic is not both activated within Maven.
  if [[ -n "${HADOOP_PROFILE}" ]] && [[ "${PATCH_BRANCH}" = branch-2* ]] ; then
    maven_args+=("-Dhadoop.profile=${HADOOP_PROFILE}")
  fi

  # disabled because "maven_executor" needs to return both command and args
  # shellcheck disable=2046
  echo_and_redirect "${logfile}" $(maven_executor) "${maven_args[@]}"

  count=$(${GREP} -c '\[ERROR\]' "${logfile}")
  if [[ ${count} -gt 0 ]]; then
    add_vote_table -1 shadedjars "${repostatus} has ${count} errors when building our shaded downstream artifacts."
    add_footer_table shadedjars "@@BASE@@/${repostatus}-shadedjars.txt"
    return 1
  fi

  add_vote_table +1 shadedjars "${repostatus} has no errors when building our shaded downstream artifacts."
  return 0
}

###################################################

add_test_type hadoopcheck

## @description  hadoopcheck file filter
## @audience     private
## @stability    evolving
## @param        filename
function hadoopcheck_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]] || [[ ${filename} =~ pom\.xml$ ]]; then
    add_test hadoopcheck
  fi
}

## @description  Parse args to detect if QUICK_HADOOPCHECK mode is enabled.
## @audience     private
## @stability    evolving
function hadoopcheck_parse_args
{
  declare i

  for i in "$@"; do
    case ${i} in
      --quick-hadoopcheck)
        delete_parameter "${i}"
        QUICK_HADOOPCHECK=true
      ;;
    esac
  done
}

## @description  Adds QUICK_HADOOPCHECK env variable to DOCKER_EXTRAARGS.
## @audience     private
## @stability    evolving
function hadoopcheck_docker_support
{
  DOCKER_EXTRAARGS=("${DOCKER_EXTRAARGS[@]}" "--env=QUICK_HADOOPCHECK=${QUICK_HADOOPCHECK}")
}

## @description  hadoopcheck test
## @audience     private
## @stability    evolving
## @param        repostatus
function hadoopcheck_rebuild
{
  local repostatus=$1
  local hadoopver
  local logfile
  local count
  local result=0
  local hbase_hadoop2_versions
  local hbase_hadoop3_versions

  if [[ "${repostatus}" = branch ]]; then
    return 0
  fi

  if ! verify_needed_test hadoopcheck; then
    return 0
  fi

  big_console_header "Compiling against various Hadoop versions"

  start_clock

  # All supported Hadoop versions that we want to test the compilation with
  # See the Hadoop section on prereqs in the HBase Reference Guide
  if [[ "${PATCH_BRANCH}" = branch-1.4 ]]; then
    yetus_info "Setting Hadoop 2 versions to test based on branch-1.4 rules."
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop2_versions="2.7.7"
    else
      hbase_hadoop2_versions="2.7.1 2.7.2 2.7.3 2.7.4 2.7.5 2.7.6 2.7.7"
    fi
  elif [[ "${PATCH_BRANCH}" = branch-1 ]]; then
    yetus_info "Setting Hadoop 2 versions to test based on branch-1 rules."
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop2_versions="2.10.0"
    else
      hbase_hadoop2_versions="2.10.0"
    fi
  elif [[ "${PATCH_BRANCH}" = branch-2.0 ]]; then
    yetus_info "Setting Hadoop 2 versions to test based on branch-2.0 rules."
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop2_versions="2.6.5 2.7.7 2.8.5"
    else
      hbase_hadoop2_versions="2.6.1 2.6.2 2.6.3 2.6.4 2.6.5 2.7.1 2.7.2 2.7.3 2.7.4 2.7.5 2.7.6 2.7.7 2.8.2 2.8.3 2.8.4 2.8.5"
    fi
  elif [[ "${PATCH_BRANCH}" = branch-2.1 ]]; then
    yetus_info "Setting Hadoop 2 versions to test based on branch-2.1 rules."
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop2_versions="2.7.7 2.8.5"
    else
      hbase_hadoop2_versions="2.7.1 2.7.2 2.7.3 2.7.4 2.7.5 2.7.6 2.7.7 2.8.2 2.8.3 2.8.4 2.8.5"
    fi
  elif [[ "${PATCH_BRANCH}" = branch-2.2 ]]; then
    yetus_info "Setting Hadoop 2 versions to test based on branch-2.2 rules."
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop2_versions="2.8.5 2.9.2 2.10.0"
    else
      hbase_hadoop2_versions="2.8.5 2.9.2 2.10.0"
    fi
  elif [[ "${PATCH_BRANCH}" = branch-2.* ]]; then
    yetus_info "Setting Hadoop 2 versions to test based on branch-2.3+ rules."
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop2_versions="2.10.1"
    else
      hbase_hadoop2_versions="2.10.0 2.10.1"
    fi
  else
    yetus_info "Setting Hadoop 2 versions to null on master/feature branch rules since we do not support hadoop 2 for hbase 3.x any more."
    hbase_hadoop2_versions=""
  fi
  if [[ "${PATCH_BRANCH}" = branch-1* ]]; then
    yetus_info "Setting Hadoop 3 versions to test based on branch-1.x rules."
    hbase_hadoop3_versions=""
  elif [[ "${PATCH_BRANCH}" = branch-2.0 ]] || [[ "${PATCH_BRANCH}" = branch-2.1 ]]; then
    yetus_info "Setting Hadoop 3 versions to test based on branch-2.0/branch-2.1 rules"
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop3_versions="3.0.3 3.1.2"
    else
      hbase_hadoop3_versions="3.0.3 3.1.1 3.1.2"
    fi
  elif [[ "${PATCH_BRANCH}" = branch-2.2 ]] || [[ "${PATCH_BRANCH}" = branch-2.3 ]]; then
    yetus_info "Setting Hadoop 3 versions to test based on branch-2.2/branch-2.3 rules"
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop3_versions="3.1.2 3.2.2"
    else
      hbase_hadoop3_versions="3.1.1 3.1.2 3.2.0 3.2.1 3.2.2"
    fi
  else
    yetus_info "Setting Hadoop 3 versions to test based on branch-2.4+/master/feature branch rules"
    if [[ "${QUICK_HADOOPCHECK}" == "true" ]]; then
      hbase_hadoop3_versions="3.1.2 3.2.2 3.3.1"
    else
      hbase_hadoop3_versions="3.1.1 3.1.2 3.2.0 3.2.1 3.2.2 3.3.0 3.3.1"
    fi
  fi

  export MAVEN_OPTS="${MAVEN_OPTS}"
  for hadoopver in ${hbase_hadoop2_versions}; do
    logfile="${PATCH_DIR}/patch-javac-${hadoopver}.txt"
    # disabled because "maven_executor" needs to return both command and args
    # shellcheck disable=2046
    echo_and_redirect "${logfile}" \
      $(maven_executor) clean install \
        -DskipTests -DHBasePatchProcess \
        -Dhadoop-two.version="${hadoopver}"
    count=$(${GREP} -c '\[ERROR\]' "${logfile}")
    if [[ ${count} -gt 0 ]]; then
      add_vote_table -1 hadoopcheck "${BUILDMODEMSG} causes ${count} errors with Hadoop v${hadoopver}."
      add_footer_table hadoopcheck "@@BASE@@/patch-javac-${hadoopver}.txt"
      ((result=result+1))
    fi
  done

  hadoop_profile=""
  if [[ "${PATCH_BRANCH}" = branch-2* ]]; then
    hadoop_profile="-Dhadoop.profile=3.0"
  fi
  for hadoopver in ${hbase_hadoop3_versions}; do
    logfile="${PATCH_DIR}/patch-javac-${hadoopver}.txt"
    # disabled because "maven_executor" needs to return both command and args
    # shellcheck disable=2046
    echo_and_redirect "${logfile}" \
      $(maven_executor) clean install \
        -DskipTests -DHBasePatchProcess \
        -Dhadoop-three.version="${hadoopver}" \
        ${hadoop_profile}
    count=$(${GREP} -c '\[ERROR\]' "${logfile}")
    if [[ ${count} -gt 0 ]]; then
      add_vote_table -1 hadoopcheck "${BUILDMODEMSG} causes ${count} errors with Hadoop v${hadoopver}."
      add_footer_table hadoopcheck "@@BASE@@/patch-javac-${hadoopver}.txt"
      ((result=result+1))
    fi
  done

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  if [[ -n "${hbase_hadoop3_versions}" ]]; then
    if [[ -n "${hbase_hadoop2_versions}" ]]; then
      add_vote_table +1 hadoopcheck "Patch does not cause any errors with Hadoop ${hbase_hadoop2_versions} or ${hbase_hadoop3_versions}."
    else
      add_vote_table +1 hadoopcheck "Patch does not cause any errors with Hadoop ${hbase_hadoop3_versions}."
    fi
  else
    add_vote_table +1 hadoopcheck "Patch does not cause any errors with Hadoop ${hbase_hadoop2_versions}."
  fi

  logfile="${PATCH_DIR}/patch-install-after-hadoopcheck.txt"
  echo_and_redirect "${logfile}" \
    $(maven_executor) clean install \
      -DskipTests -DHBasePatchProcess

  return 0
}

######################################

# TODO if we need the protoc check, we probably need to check building all the modules that rely on hbase-protocol
add_test_type hbaseprotoc

function hbaseprotoc_initialize
{
  # So long as there are inter-module dependencies on the protoc modules, we
  # need to run a full `mvn install` before a patch can be tested.
  yetus_debug "initializing HBase Protoc plugin."
  maven_add_install hbaseprotoc
}

## @description  hbaseprotoc file filter
## @audience     private
## @stability    evolving
## @param        filename
function hbaseprotoc_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.proto$ ]]; then
    add_test hbaseprotoc
  fi
}

## @description  check hbase proto compilation
## @audience     private
## @stability    evolving
## @param        repostatus
function hbaseprotoc_rebuild
{
  declare repostatus=$1
  declare i=0
  declare fn
  declare module
  declare logfile
  declare count
  declare result

  if [[ "${repostatus}" = branch ]]; then
    return 0
  fi

  if ! verify_needed_test hbaseprotoc; then
    return 0
  fi

  big_console_header "HBase protoc plugin: ${BUILDMODE}"

  start_clock

  personality_modules patch hbaseprotoc
  # Need to run 'install' instead of 'compile' because shading plugin
  # is hooked-up to 'install'; else hbase-protocol-shaded is left with
  # half of its process done.
  modules_workers patch hbaseprotoc install -DskipTests -X -DHBasePatchProcess

  # shellcheck disable=SC2153
  until [[ $i -eq "${#MODULE[@]}" ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi
    module=${MODULE[$i]}
    fn=$(module_file_fragment "${module}")
    logfile="${PATCH_DIR}/patch-hbaseprotoc-${fn}.txt"

    count=$(${GREP} -c '\[ERROR\]' "${logfile}")

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

## @description  hbaseanti file filter
## @audience     private
## @stability    evolving
## @param        filename
function hbaseanti_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test hbaseanti
  fi
}

## @description  hbaseanti patch file check
## @audience     private
## @stability    evolving
## @param        filename
function hbaseanti_patchfile
{
  local patchfile=$1
  local warnings
  local result

  if [[ "${BUILDMODE}" = full ]]; then
    return 0
  fi

  if ! verify_needed_test hbaseanti; then
    return 0
  fi

  big_console_header "Checking for known anti-patterns"

  start_clock

  warnings=$(${GREP} -c 'new TreeMap<byte.*()' "${patchfile}")
  if [[ ${warnings} -gt 0 ]]; then
    add_vote_table -1 hbaseanti "" "The patch appears to have anti-pattern where BYTES_COMPARATOR was omitted."
    ((result=result+1))
  fi

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  add_vote_table +1 hbaseanti "" "Patch does not have any anti-patterns."
  return 0
}

## @description  process the javac output for generating WARNING/ERROR
## @audience     private
## @stability    evolving
## @param        input filename
## @param        output filename
# Override the default javac_logfilter so that we can do a sort before outputing the WARNING/ERROR.
# This is because that the output order of the error prone warnings is not stable, so the diff
# method will report unexpected errors if we do not sort it. Notice that a simple sort will cause
# line number being sorted by lexicographical so the output maybe a bit strange to human but it is
# really hard to sort by file name first and then line number and column number in shell...
function hbase_javac_logfilter
{
  declare input=$1
  declare output=$2

  ${GREP} -E '\[(ERROR|WARNING)\] /.*\.java:' "${input}" | sort > "${output}"
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
