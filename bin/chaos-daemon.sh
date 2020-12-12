#!/usr/bin/env bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
#

usage="Usage: chaos-daemon.sh (start|stop) chaosagent"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo "$usage"
  exit 1
fi

# get arguments
startStop=$1
shift

command=$1
shift

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$HBASE_PID_DIR"
    if [ -f "$CHAOS_PID" ]; then
      if kill -0 "$(cat "$CHAOS_PID")" > /dev/null 2>&1; then
        echo "$command" running as process "$(cat "$CHAOS_PID")".  Stop it first.
        exit 1
      fi
    fi
}

bin=`dirname "${BASH_SOURCE-$0}"`
bin=$(cd "$bin">/dev/null || exit; pwd)

. "$bin"/hbase-config.sh
. "$bin"/hbase-common.sh

CLASSPATH=$HBASE_CONF_DIR
for f in ../lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done

# get log directory
if [ "$HBASE_LOG_DIR" = "" ]; then
  export HBASE_LOG_DIR="$HBASE_HOME/logs"
fi

if [ "$HBASE_PID_DIR" = "" ]; then
  HBASE_PID_DIR=/tmp
fi

if [ "$HBASE_IDENT_STRING" = "" ]; then
  export HBASE_IDENT_STRING="$USER"
fi

if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

export HBASE_LOG_PREFIX=hbase-$HBASE_IDENT_STRING-$command-$HOSTNAME
export CHAOS_LOGFILE=$HBASE_LOG_PREFIX.log

if [ -z "${HBASE_ROOT_LOGGER}" ]; then
export HBASE_ROOT_LOGGER=${HBASE_ROOT_LOGGER:-"INFO,RFA"}
fi

if [ -z "${HBASE_SECURITY_LOGGER}" ]; then
export HBASE_SECURITY_LOGGER=${HBASE_SECURITY_LOGGER:-"INFO,RFAS"}
fi

CHAOS_LOGLOG=${CHAOS_LOGLOG:-"${HBASE_LOG_DIR}/${CHAOS_LOGFILE}"}
CHAOS_PID=$HBASE_PID_DIR/hbase-$HBASE_IDENT_STRING-$command.pid

if [ -z "$CHAOS_JAVA_OPTS" ]; then
  CHAOS_JAVA_OPTS="-Xms1024m -Xmx4096m"
fi

case $startStop in

(start)
    check_before_start
    echo running $command
    CMD="${JAVA_HOME}/bin/java -Dapp.home=${HBASE_CONF_DIR}/../  ${CHAOS_JAVA_OPTS} -cp ${CLASSPATH} org.apache.hadoop.hbase.chaos.ChaosService -$command start &>> ${CHAOS_LOGLOG} &"

    eval $CMD
    PID=$(echo $!)
    echo ${PID} >${CHAOS_PID}

    echo "Chaos ${1} process Started with ${PID} !"
    now=$(date)
    echo "${now} Chaos ${1} process Started with ${PID} !" >>${CHAOS_LOGLOG}
    ;;

(stop)
    echo stopping $command
    if [ -f $CHAOS_PID ]; then
      pidToKill=`cat $CHAOS_PID`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $CHAOS_LOGLOG
        kill $pidToKill > /dev/null 2>&1
        waitForProcessEnd $pidToKill $command
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $CHAOS_PID
    fi
    rm -f $CHAOS_PID
  ;;

(*)
  echo $usage
  exit 1
  ;;

esac
