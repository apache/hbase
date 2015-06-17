#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
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
# Runs a Hadoop hbase command as a daemon.
#
# Environment Variables
#
#   HBASE_CONF_DIR   Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HBASE_LOG_DIR    Where log files are stored.  PWD by default.
#   HBASE_PID_DIR    The pid files are stored. /tmp by default.
#   HBASE_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   HBASE_NICENESS The scheduling priority for daemons. Defaults to 0.
#   HBASE_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if it has not stopped.
#                        Default 1200 seconds.
#
# Modelled after $HADOOP_HOME/bin/hadoop-daemon.sh

usage="Usage: hbase-daemon.sh [--config <conf-dir>]\
 (start|stop|restart|autorestart|foreground_start) <hbase-command> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hbase-config.sh
. "$bin"/hbase-common.sh

# get arguments
startStop=$1
shift

command=$1
shift

hbase_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

cleanAfterRun() {
  if [ -f ${HBASE_PID} ]; then
    # If the process is still running time to tear it down.
    kill -9 `cat ${HBASE_PID}` > /dev/null 2>&1
    rm -f ${HBASE_PID} > /dev/null 2>&1
  fi

  if [ -f ${HBASE_ZNODE_FILE} ]; then
    if [ "$command" = "master" ]; then
      $bin/hbase master clear > /dev/null 2>&1
    else
      #call ZK to delete the node
      ZNODE=`cat ${HBASE_ZNODE_FILE}`
      $bin/hbase zkcli delete ${ZNODE} > /dev/null 2>&1
    fi
    rm ${HBASE_ZNODE_FILE}
  fi
}

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$HBASE_PID_DIR"
    if [ -f $HBASE_PID ]; then
      if kill -0 `cat $HBASE_PID` > /dev/null 2>&1; then
        echo $command running as process `cat $HBASE_PID`.  Stop it first.
        exit 1
      fi
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${HBASE_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

# get log directory
if [ "$HBASE_LOG_DIR" = "" ]; then
  export HBASE_LOG_DIR="$HBASE_HOME/logs"
fi
mkdir -p "$HBASE_LOG_DIR"

if [ "$HBASE_PID_DIR" = "" ]; then
  HBASE_PID_DIR=/tmp
fi

if [ "$HBASE_IDENT_STRING" = "" ]; then
  export HBASE_IDENT_STRING="$USER"
fi

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
export HBASE_LOG_PREFIX=hbase-$HBASE_IDENT_STRING-$command-$HOSTNAME
export HBASE_LOGFILE=$HBASE_LOG_PREFIX.log

if [ -z "${HBASE_ROOT_LOGGER}" ]; then
export HBASE_ROOT_LOGGER=${HBASE_ROOT_LOGGER:-"INFO,RFA"}
fi

if [ -z "${HBASE_SECURITY_LOGGER}" ]; then 
export HBASE_SECURITY_LOGGER=${HBASE_SECURITY_LOGGER:-"INFO,RFAS"}
fi

HBASE_LOGOUT=${HBASE_LOGOUT:-"$HBASE_LOG_DIR/$HBASE_LOG_PREFIX.out"}
HBASE_LOGGC=${HBASE_LOGGC:-"$HBASE_LOG_DIR/$HBASE_LOG_PREFIX.gc"}
HBASE_LOGLOG=${HBASE_LOGLOG:-"${HBASE_LOG_DIR}/${HBASE_LOGFILE}"}
HBASE_PID=$HBASE_PID_DIR/hbase-$HBASE_IDENT_STRING-$command.pid
export HBASE_ZNODE_FILE=$HBASE_PID_DIR/hbase-$HBASE_IDENT_STRING-$command.znode
export HBASE_START_FILE=$HBASE_PID_DIR/hbase-$HBASE_IDENT_STRING-$command.autorestart

if [ -n "$SERVER_GC_OPTS" ]; then
  export SERVER_GC_OPTS=${SERVER_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${HBASE_LOGGC}"}
fi
if [ -n "$CLIENT_GC_OPTS" ]; then
  export CLIENT_GC_OPTS=${CLIENT_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${HBASE_LOGGC}"}
fi

# Set default scheduling priority
if [ "$HBASE_NICENESS" = "" ]; then
    export HBASE_NICENESS=0
fi

thiscmd="$bin/$(basename ${BASH_SOURCE-$0})"
args=$@

case $startStop in

(start)
    check_before_start
    hbase_rotate_log $HBASE_LOGOUT
    hbase_rotate_log $HBASE_LOGGC
    echo starting $command, logging to $HBASE_LOGOUT
    nohup $thiscmd --config "${HBASE_CONF_DIR}" \
        foreground_start $command $args < /dev/null > ${HBASE_LOGOUT} 2>&1  &
    sleep 1; head "${HBASE_LOGOUT}"
  ;;

(autorestart)
    check_before_start
    hbase_rotate_log $HBASE_LOGOUT
    hbase_rotate_log $HBASE_LOGGC
    nohup $thiscmd --config "${HBASE_CONF_DIR}" \
        internal_autorestart $command $args < /dev/null > ${HBASE_LOGOUT} 2>&1  &
  ;;

(foreground_start)
    trap cleanAfterRun SIGHUP SIGINT SIGTERM EXIT
    if [ "$HBASE_NO_REDIRECT_LOG" != "" ]; then
        # NO REDIRECT
        echo "`date` Starting $command on `hostname`"
        echo "`ulimit -a`"
        # in case the parent shell gets the kill make sure to trap signals.
        # Only one will get called. Either the trap or the flow will go through.
        nice -n $HBASE_NICENESS "$HBASE_HOME"/bin/hbase \
            --config "${HBASE_CONF_DIR}" \
            $command "$@" start &
    else
        echo "`date` Starting $command on `hostname`" >> ${HBASE_LOGLOG}
        echo "`ulimit -a`" >> "$HBASE_LOGLOG" 2>&1
        # in case the parent shell gets the kill make sure to trap signals.
        # Only one will get called. Either the trap or the flow will go through.
        nice -n $HBASE_NICENESS "$HBASE_HOME"/bin/hbase \
            --config "${HBASE_CONF_DIR}" \
            $command "$@" start >> ${HBASE_LOGOUT} 2>&1 &
    fi
    # Add to the command log file vital stats on our environment.
    hbase_pid=$!
    echo $hbase_pid > ${HBASE_PID}
    wait $hbase_pid
  ;;

(internal_autorestart)
    touch "$HBASE_START_FILE"
    #keep starting the command until asked to stop. Reloop on software crash
    while true
      do
        lastLaunchDate=`date +%s`
        $thiscmd --config "${HBASE_CONF_DIR}" foreground_start $command $args

        #if the file does not exist it means that it was not stopped properly by the stop command
        if [ ! -f "$HBASE_START_FILE" ]; then
          exit 1
        fi

        #if the cluster is being stopped then do not restart it again.
        zparent=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.parent`
        if [ "$zparent" == "null" ]; then zparent="/hbase"; fi
        zkrunning=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.state`
        if [ "$zkrunning" == "null" ]; then zkrunning="running"; fi
        zkFullRunning=$zparent/$zkrunning
        $bin/hbase zkcli stat $zkFullRunning 2>&1 | grep "Node does not exist"  1>/dev/null 2>&1
        #grep returns 0 if it found something, 1 otherwise
        if [ $? -eq 0 ]; then
          exit 1
        fi

        #If ZooKeeper cannot be found, then do not restart
        $bin/hbase zkcli stat $zkFullRunning 2>&1 | grep Exception | grep ConnectionLoss  1>/dev/null 2>&1
        if [ $? -eq 0 ]; then
          exit 1
        fi

        #if it was launched less than 5 minutes ago, then wait for 5 minutes before starting it again.
        curDate=`date +%s`
        limitDate=`expr $lastLaunchDate + 300`
        if [ $limitDate -gt $curDate ]; then
          sleep 300
        fi
      done
    ;;

(stop)
    rm -f "$HBASE_START_FILE"
    if [ -f $HBASE_PID ]; then
      pidToKill=`cat $HBASE_PID`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $HBASE_LOGLOG
        kill $pidToKill > /dev/null 2>&1
        waitForProcessEnd $pidToKill $command
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $HBASE_PID
    fi
    rm -f $HBASE_PID
  ;;

(restart)
    # stop the command
    $thiscmd --config "${HBASE_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${HBASE_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the command
    $thiscmd --config "${HBASE_CONF_DIR}" start $command $args &
    wait_until_done $!
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
