#! /bin/bash
#
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

# This file is used to run multiple instances of certain HBase daemons using init scripts.
# It replaces the local-regionserver.sh and local-master.sh scripts for Bigtop packages.
# By default, this script runs a single daemon normally. If offsets are provided, additional
# daemons are run, identified by the offset in log and pid files, and listening on the default
# port + the offset. Offsets can be provided as arguments when invoking init scripts directly:
#
#     /etc/init.d/hbase-@HBASE_DAEMON@ start 1 2 3 4
#
# or you can list the offsets to run in /etc/init.d/@HBASE_DAEMON@_offsets:
#
#    echo "@HBASE_DAEMON@_OFFSETS='1 2 3 4' >> /etc/default/hbase"
#    sudo service hbase-$HBASE_DAEMON@ start
#
# Offsets specified on the command-line always override the offsets file. If no offsets are
# specified on the command-line when stopping or restarting daemons, all running instances of the
# daemon are stopped (regardless of the contents of the offsets file).

# chkconfig: @CHKCONFIG@
# description: Summary: HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data. This project's goal is the hosting of very large tables -- billions of rows X millions of columns -- atop clusters of commodity hardware.
# processname: HBase
#
### BEGIN INIT INFO
# Provides:          hbase-@HBASE_DAEMON@
# Required-Start:    $network $local_fs $remote_fs
# Required-Stop:     $remote_fs
# Should-Start:      $named
# Should-Stop:
# Default-Start:     @INIT_DEFAULT_START@
# Default-Stop:      @INIT_DEFAULT_STOP@
# Short-Description: Hadoop HBase @HBASE_DAEMON@ daemon
### END INIT INFO

DEFAULTS_DIR=${DEFAULTS_DIR-/etc/default}
[ -n "${DEFAULTS_DIR}" -a -r ${DEFAULTS_DIR}/hadoop ] && . ${DEFAULTS_DIR}/hadoop
[ -n "${DEFAULTS_DIR}" -a -r ${DEFAULTS_DIR}/hbase ] && . ${DEFAULTS_DIR}/hbase

# Our default HBASE_HOME, HBASE_PID_DIR and HBASE_CONF_DIR
export HBASE_HOME=${HBASE_HOME:-/usr/lib/hbase}
export HBASE_PID_DIR=${HBASE_PID_DIR:-/var/run/hbase}
export HBASE_LOG_DIR=${HBASE_LOG_DIR:-/var/log/hbase}

install -d -m 0755 -o hbase -g hbase ${HBASE_PID_DIR}

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
DAEMON_SCRIPT=$HBASE_HOME/bin/hbase-daemon.sh
NAME=hbase-@HBASE_DAEMON@
DESC="Hadoop HBase @HBASE_DAEMON@ daemon"
PID_FILE=$HBASE_PID_DIR/hbase-hbase-@HBASE_DAEMON@.pid
CONF_DIR=/etc/hbase/conf

DODTIME=3                   # Time to wait for the server to die, in seconds
                            # If this value is set too low you might not
                            # let some servers to die gracefully and
                            # 'restart' will not work

UPPERCASE_HBASE_DAEMON=$(echo @HBASE_DAEMON@ | tr '[:lower:]' '[:upper:]')

ALL_DAEMONS_RUNNING=0
NO_DAEMONS_RUNNING=1
SOME_OFFSET_DAEMONS_FAILING=2
INVALID_OFFSETS_PROVIDED=3

# These limits are not easily configurable - they are enforced by HBase
if [ "@HBASE_DAEMON@" == "master" ] ; then
    FIRST_PORT=60000
    FIRST_INFO_PORT=60010
    OFFSET_LIMIT=10
elif [ "@HBASE_DAEMON@" == "regionserver" ] ; then
    FIRST_PORT=60200
    FIRST_INFO_PORT=60300
    OFFSET_LIMIT=100
fi

validate_offsets() {
    for OFFSET in $1; do
        if [[ ! $OFFSET =~ ^((0)|([1-9][0-9]{0,2}))$ ]]; then
            echo "ERROR: All offsets must be positive integers (no leading zeros, max $OFFSET_LIMIT)"
            exit $INVALID_OFFSETS_PROVIDED
        fi
        if [ ${OFFSET} -lt 0 ] ; then
            echo "ERROR: Cannot start @HBASE_DAEMON@ with negative offset" >&2
            exit $INVALID_OFFSETS_PROVIDED
        fi
        if [ ${OFFSET} -ge ${OFFSET_LIMIT} ] ; then
            echo "ERROR: Cannot start @HBASE_DAEMON@ with offset higher than $OFFSET_LIMIT" >&2
            exit $INVALID_OFFSETS_PROVIDED
        fi
    done
}

offset_pidfile() {
    echo $HBASE_PID_DIR/hbase-hbase-$1-@HBASE_DAEMON@.pid
}

OFFSETS_FROM_CLI="${*:2}"
validate_offsets "$OFFSETS_FROM_CLI"
if [ -n "$(eval echo \${${UPPERCASE_HBASE_DAEMON}_OFFSETS})" ] ; then
    OFFSETS_FROM_DEFAULT="$(eval echo \${${UPPERCASE_HBASE_DAEMON}_OFFSETS})"
    validate_offsets "$OFFSETS_FROM_DEFAULT"
fi
OFFSET_PID_FILES="`ls $HBASE_PID_DIR/hbase-hbase-*-@HBASE_DAEMON@.pid 2>/dev/null`"
if [ -n "$OFFSET_PID_FILES" ] ; then
    OFFSETS_FROM_PIDS=`echo "$OFFSET_PID_FILES" | sed "s#$HBASE_PID_DIR/hbase-hbase-##" | sed "s#-.*##" | tr '\n' ' '`
fi

multi_hbase_daemon_check_pidfiles() {
  if [ -z "$OFFSETS_FROM_PIDS" ] ; then
    return $NO_DAEMONS_RUNNING
  fi
  if [ -n "$OFFSETS_FROM_CLI" ] ; then
    OFFSETS="$OFFSETS_FROM_CLI"
  else
    OFFSETS="$OFFSETS_FROM_PIDS"
  fi

  RESULT=$ALL_DAEMONS_RUNNING
  for OFFSET in $OFFSETS; do
    echo -n "HBase @HBASE_DAEMON@ $OFFSET: "
    if hbase_check_pidfile `offset_pidfile $OFFSET` ; then
      echo "running"
    else
      echo "not running"
      RESULT=$SOME_OFFSET_DAEMONS_FAILING
    fi
  done
  return $RESULT
}

multi_hbase_daemon_stop_pidfiles() {
  if [ -z "$OFFSETS_FROM_PIDS" ] ; then
    return $NO_DAEMONS_RUNNING
  fi
  if [ -n "$OFFSETS_FROM_CLI" ] ; then
    OFFSETS="$OFFSETS_FROM_CLI"
  else
    OFFSETS="$OFFSETS_FROM_PIDS"
  fi

  RESULT=$NO_DAEMONS_RUNNING
  for OFFSET in $OFFSETS; do
    echo -n "Forcefully stopping HBase @HBASE_DAEMON@ $OFFSET: "
    PID_FILE=`offset_pidfile $OFFSET`
    hbase_stop_pidfile $PID_FILE
    if hbase_check_pidfile $PID_FILE ; then
      echo "ERROR."
    else
      echo "OK."
      rm -f $PID_FILE
    fi
  done
  return $RESULT
}

# Starts and stops multiple instances of HBase daemons
multi_hbase_daemon() {
    COMMAND=$1
    OFFSETS="$OFFSETS_FROM_CLI"
    if [ "$COMMAND" == "start" ] ; then
        ACTION="Starting"
        RUNNING="OK"
        STOPPED="ERROR"
        if [ -z "$OFFSETS_FROM_CLI" ] ; then
            OFFSETS="$OFFSETS_FROM_DEFAULT"
        fi
    elif [ "$COMMAND" == "stop" ] ; then
        ACTION="Stopping"
        RUNNING="ERROR"
        STOPPED="OK"
        if [ -z "$OFFSETS_FROM_CLI" ] ; then
            OFFSETS="$OFFSETS_FROM_PIDS"
        fi
    else
        echo "ERROR: Illegal command: $COMMAND"
        exit 1
    fi

    for OFFSET in ${OFFSETS} ; do

        export HBASE_${UPPERCASE_HBASE_DAEMON}_OPTS=" "

        echo -n "$ACTION @HBASE_DAEMON@ daemon $OFFSET: "
        export HBASE_IDENT_STRING="hbase-${OFFSET}"
        LOG_FILE="$HBASE_LOG_DIR/hbase-$HBASE_IDENT_STRING-@HBASE_DAEMON@-$HOSTNAME.pid"
        PID_FILE="$HBASE_PID_DIR/hbase-$HBASE_IDENT_STRING-@HBASE_DAEMON@.pid"
        HBASE_MULTI_ARGS="-D hbase.regionserver.port=`expr ${FIRST_PORT} + $OFFSET` \
                          -D hbase.regionserver.info.port=`expr ${FIRST_INFO_PORT} + ${OFFSET}`"

        if [ "x$JAVA_TMP_DIR" == "x" ] ; then
            JAVA_TMP_DIR="/tmp/java_tmp_dir"
        fi
        HBASE_TMP_DIR=" -Djava.io.tmpdir=${JAVA_TMP_DIR}/${OFFSET}"

        if [ "x$JMXPORT" != "x" ] ; then
            HBASE_JMX_PORT=" -Dcom.sun.management.jmxremote.port=`expr ${JMXPORT} + ${OFFSET}`"
        fi
        export HBASE_${UPPERCASE_HBASE_DAEMON}_OPTS="`eval '$'HBASE_${UPPERCASE_HBASE_DAEMON}_OPTS`$HBASE_TMP_DIR$HBASE_JMX_PORT"

        hbase_check_pidfile $PID_FILE
        STATUS=$?
        if [[ "$STATUS" == "0" && "$COMMAND" == "start" ]] ; then
            echo "Already running"
            continue
        elif [[ "$STATUS" != "0" && "$COMMAND" == "stop" ]] ; then
            rm -f $PID_FILE
            echo "Already stopped"
            continue
        fi
        runuser -s /bin/bash hbase -c "${DAEMON_SCRIPT} ${COMMAND} regionserver ${HBASE_MULTI_ARGS} >> ${LOG_FILE}"
        if [[ "$COMMAND" == "stop" ]] ; then
            rm -f $PID_FILE
        fi
        if hbase_check_pidfile $PID_FILE ; then
            echo "$RUNNING"
        else
            echo "$STOPPED"
        fi
    done
    return 0
}

# Checks if the given pid represents a live process.
# Returns 0 if the pid is a live process, 1 otherwise
hbase_is_process_alive() {
  local pid="$1"
  ps -fp $pid | grep $pid | grep @HBASE_DAEMON@ > /dev/null 2>&1
}

# Check if the process associated to a pidfile is running.
# Return 0 if the pidfile exists and the process is running, 1 otherwise
hbase_check_pidfile() {
  local pidfile="$1" # IN
  local pid

  pid=`cat "$pidfile" 2>/dev/null`
  if [ "$pid" = '' ]; then
    # The file probably does not exist or is empty.
    return 1
  fi

  set -- $pid
  pid="$1"

  hbase_is_process_alive $pid
}

# Kill the process associated to a pidfile
hbase_stop_pidfile() {
   local pidfile="$1" # IN
   local pid

   pid=`cat "$pidfile" 2>/dev/null`
   if [ "$pid" = '' ]; then
      # The file probably does not exist or is empty. Success
      return 0
   fi

   set -- $pid
   pid="$1"

   # First try the easy way
   if hbase_process_kill "$pid" 15; then
      rm $pidfile
      return 0
   fi

   # Otherwise try the hard way
   if hbase_process_kill "$pid" 9; then
      rm $pidfile
      return 0
   fi

   return 1
}

hbase_process_kill() {
    local pid="$1"    # IN
    local signal="$2" # IN
    local second

    kill -$signal $pid 2>/dev/null

   # Wait a bit to see if the dirty job has really been done
    for second in 0 1 2 3 4 5 6 7 8 9 10; do
        if hbase_is_process_alive "$pid"; then
         # Success
            return 0
        fi

        sleep 1
    done

   # Timeout
    return 1
}

start() {
    if [ -n "${OFFSETS_FROM_CLI}${OFFSETS_FROM_DEFAULT}" ] ; then
        if hbase_check_pidfile $PID_FILE ; then
            echo "$NAME has already been started - cannot start other @HBASE_DAEMON@ daemons."
            return 1
        fi
        multi_hbase_daemon "start"
        return $?
    fi
    multi_hbase_daemon_check_pidfiles > /dev/null
    if [ "$?" != "$NO_DAEMONS_RUNNING" ] ; then
      echo "Cannot start $NAME - other @HBASE_DAEMON@ daemons have already been started."
      return 1
    fi
    echo -n "Starting $DESC: "
    runuser -s /bin/bash hbase -c "$DAEMON_SCRIPT start @HBASE_DAEMON@"
    if hbase_check_pidfile $PID_FILE ; then
        echo "$NAME."
        return $ALL_DAEMONS_RUNNING
    else
        echo "ERROR."
        return $NO_DAEMONS_RUNNING
    fi
}
stop() {
    if [ -n "${OFFSETS_FROM_CLI}${OFFSETS_FROM_PIDS}" ] ; then
        multi_hbase_daemon "stop"
        return "$?"
    fi

    echo -n "Stopping $DESC: "
    runuser -s /bin/bash hbase -c "$DAEMON_SCRIPT stop @HBASE_DAEMON@"
    if hbase_check_pidfile $PID_FILE ; then
        echo "ERROR."
        return 1
    else
        echo "$NAME."
        return 0
    fi
}

force_stop() {
    MULTI_HBASE_DAEMON_STATUS_TEXT=`multi_hbase_daemon_check_pidfiles`
    MULTI_HBASE_DAEMON_STATUS=$?
    if [ "$MULTI_HBASE_DAEMON_STATUS" == "$NO_DAEMONS_RUNNING" ] ; then
        echo -n "Forcefully stopping $DESC: "
        hbase_stop_pidfile $PID_FILE
        if hbase_check_pidfile $PID_FILE ; then
            echo " ERROR."
        else
            echo "$NAME."
        fi
    else
        multi_hbase_daemon_stop_pidfiles
    fi
}

force_reload() {
  # check wether $DAEMON is running. If so, restart
  hbase_check_pidfile $PID_FILE && $0 restart $OFFSETS_FROM_CLI
}

restart() {
    echo -n "Restarting $DESC: "
    $0 stop
    [ -n "$DODTIME" ] && sleep $DODTIME
    $0 start $OFFSETS_FROM_CLI
}

status() {
    MULTI_HBASE_DAEMON_STATUS_TEXT=`multi_hbase_daemon_check_pidfiles`
    MULTI_HBASE_DAEMON_STATUS=$?
    if [ "$MULTI_HBASE_DAEMON_STATUS" == "$NO_DAEMONS_RUNNING" ] ; then
        echo -n "$NAME is "
        if hbase_check_pidfile $PID_FILE ;  then
            echo "running"
        else
            echo "not running."
            return $NO_DAEMONS_RUNNING
        fi
    else
        IFS=''
        echo $MULTI_HBASE_DAEMON_STATUS_TEXT
        return $MULTI_HBASE_DAEMON_STATUS
    fi
}

condrestart(){
    status $@ >/dev/null 2>/dev/null
    DAEMON_STATUS=$?
    if [ "$DAEMON_STATUS" == "$ALL_DAEMONS_RUNNING" -o "$DAEMON_STATUS" == "$SOME_OFFSET_DAEMONS_FAILING" ] ; then
        restart $@
    fi
}

RETVAL=0

case "$1" in
  start)
        start
        RETVAL=$?
  ;;
  stop)
        stop
        RETVAL=$?
  ;;
  force-stop)
        force_stop
        RETVAL=$?
  ;;
  force-reload)
        force_reload
        RETVAL=$?
  ;;
  restart)
        restart
        RETVAL=$?
    ;;
  condrestart)
        condrestart
        RETVAL=$?
  ;;
  status)
        status
        RETVAL=$?
    ;;
  *)
  N=/etc/init.d/$NAME
  echo "Usage: $N {start|stop|restart|force-reload|status|force-stop|condrestart}" >&2
  RETVAL=1
  ;;
esac

exit ${RETVAL}
