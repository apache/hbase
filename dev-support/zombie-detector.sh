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

# Looks for any running zombies left over from old build runs.
# Will report and try to do stack trace on stale processes so can
# figure how they are hung. Echos state as the script runs
# on STDERR but prints final output on STDOUT formatted so it
# will fold into the test result formatting done by test-patch.sh.
# This script is called from test-patch.sh but also after tests
# have run up on builds.apache.org.

# TODO: format output to suit context -- test-patch, jenkins or dev env

#set -x
# printenv

### Setup some variables.  
bindir=$(dirname $0)

# This key is set by our surefire configuration up in the main pom.xml
# This key needs to match the key we set up there.
HBASE_BUILD_ID_KEY="hbase.build.id="
JENKINS=

PS=${PS:-ps}
AWK=${AWK:-awk}
WGET=${WGET:-wget}
GREP=${GREP:-grep}
JIRACLI=${JIRA:-jira}

###############################################################################
printUsage() {
  echo "Usage: $0 [options]" BUILD_ID
  echo
  echo "Where:"
  echo "  BUILD_ID is build id to look for in process listing"
  echo
  echo "Options:"
  echo "--ps-cmd=<cmd>         The 'ps' command to use (default 'ps')"
  echo "--awk-cmd=<cmd>        The 'awk' command to use (default 'awk')"
  echo "--grep-cmd=<cmd>       The 'grep' command to use (default 'grep')"
  echo
  echo "Jenkins-only options:"
  echo "--jenkins              Run by Jenkins (runs tests and posts results to JIRA)"
  echo "--wget-cmd=<cmd>       The 'wget' command to use (default 'wget')"
  echo "--jira-cmd=<cmd>       The 'jira' command to use (default 'jira')"
}

###############################################################################
parseArgs() {
  for i in $*
  do
    case $i in
    --jenkins)
      JENKINS=true
      ;;
    --ps-cmd=*)
      PS=${i#*=}
      ;;
    --awk-cmd=*)
      AWK=${i#*=}
      ;;
    --wget-cmd=*)
      WGET=${i#*=}
      ;;
    --grep-cmd=*)
      GREP=${i#*=}
      ;;
    --jira-cmd=*)
      JIRACLI=${i#*=}
      ;;
    *)
      BUILD_ID=$i
      ;;
    esac
  done
  if [ -z "$BUILD_ID" ]; then
    printUsage
    exit 1
  fi
}

### Return list of the processes found with passed build id.
find_processes () {
  jps -v | grep surefirebooter | grep -e "${HBASE_BUILD_TAG}"
}

### Look for zombies
zombies () {
  ZOMBIES=`find_processes`
  if [[ -z ${ZOMBIES} ]]
  then
    ZOMBIE_TESTS_COUNT=0
  else
    ZOMBIE_TESTS_COUNT=`echo "${ZOMBIES}"| wc -l| xargs`
  fi
  if [[ $ZOMBIE_TESTS_COUNT != 0 ]] ; then
    wait=30
    echo "`date` Found ${ZOMBIE_TESTS_COUNT} suspicious java process(es) listed below; waiting ${wait}s to see if just slow to stop" >&2
    echo ${ZOMBIES} >&2
    sleep ${wait}
    PIDS=`echo "${ZOMBIES}"|${AWK} '{print $1}'`
    ZOMBIE_TESTS_COUNT=0
    for pid in $PIDS
    do
      # Test our zombie still running (and that it still an hbase build item)
      PS_OUTPUT=`ps -p $pid | tail +2 | grep -e "${HBASE_BUILD_TAG}"`
      if [[ ! -z "${PS_OUTPUT}" ]]
      then
        echo "`date` Zombie: $PS_OUTPUT" >&2
        let "ZOMBIE_TESTS_COUNT+=1"
        PS_STACK=`jstack $pid | grep -e "\.Test" | grep -e "\.java"| head -3`
        echo "${PS_STACK}" >&2
        ZB_STACK="${ZB_STACK}\nPID=${pid} ${PS_STACK}"
      fi
    done
    if [[ $ZOMBIE_TESTS_COUNT != 0 ]]
    then
      echo "`date` There are ${ZOMBIE_TESTS_COUNT} possible zombie test(s)." >&2
      # If JIRA_COMMENT in environment, append our findings to it
      echo -e "$JIRA_COMMENT

    {color:red}-1 core zombie tests{color}.  There are ${ZOMBIE_TESTS_COUNT} possible zombie test(s):
        ${ZB_STACK}"
      # Exit with exit code of 1.
      exit 1
    else
      echo "`date` We're ok: there was a zombie candidate but it went away" >&2
      echo "$JIRA_COMMENT

    {color:green}+1 core zombie tests -- (was a candidate but now) no zombies!{color}."
    fi
  else
      echo "`date` We're ok: there is no zombie test" >&2
      echo "$JIRA_COMMENT

    {color:green}+1 core zombie tests -- no zombies!{color}."
  fi
}

### Check if arguments to the script have been specified properly or not
parseArgs $@
HBASE_BUILD_TAG="${HBASE_BUILD_ID_KEY}${BUILD_ID}"
zombies
RESULT=$?
if [[ $JENKINS == "true" ]] ; then
  if [[ $RESULT != 0 ]] ; then
    exit 100
  fi
fi
RESULT=$?
