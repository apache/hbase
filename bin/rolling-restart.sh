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
# Run a shell command on all regionserver hosts.
#
# Environment Variables
#
#   HBASE_REGIONSERVERS    File naming remote hosts.
#     Default is ${HADOOP_CONF_DIR}/regionservers
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HBASE_CONF_DIR  Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HBASE_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HBASE_SLAVE_TIMEOUT Seconds to wait for timing out a remote command.
#   HBASE_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HADOOP_HOME/bin/slaves.sh.

usage_str="Usage: `basename $0` [--config <hbase-confdir>] [--autostart-window-size <window size in hours>]\
      [--autostart-window-retry-limit <retry count limit for autostart>] [--autostart] [--rs-only] [--master-only] \
      [--graceful] [--maxthreads xx] [--noack] [--movetimeout]]"

function usage() {
  echo "${usage_str}"
}

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# default autostart args value indicating infinite window size and no retry limit
AUTOSTART_WINDOW_SIZE=0
AUTOSTART_WINDOW_RETRY_LIMIT=0

. "$bin"/hbase-config.sh

# start hbase daemons
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi

RR_RS=1
RR_MASTER=1
RR_GRACEFUL=0
RR_MAXTHREADS=1
START_CMD_NON_DIST_MODE=restart
START_CMD_DIST_MODE=start
RESTART_CMD_REGIONSERVER=restart

while [ $# -gt 0 ]; do
  case "$1" in
    --rs-only|-r)
      RR_RS=1
      RR_MASTER=0
      RR_GRACEFUL=0
      shift
      ;;
    --autostart)
      START_CMD_NON_DIST_MODE="--autostart-window-size ${AUTOSTART_WINDOW_SIZE} --autostart-window-retry-limit ${AUTOSTART_WINDOW_RETRY_LIMIT} autorestart"
      START_CMD_DIST_MODE="--autostart-window-size ${AUTOSTART_WINDOW_SIZE} --autostart-window-retry-limit ${AUTOSTART_WINDOW_RETRY_LIMIT} autostart"
      RESTART_CMD_REGIONSERVER="--autostart-window-size ${AUTOSTART_WINDOW_SIZE} --autostart-window-retry-limit ${AUTOSTART_WINDOW_RETRY_LIMIT} autorestart"
      shift
      ;;
    --master-only)
      RR_RS=0
      RR_MASTER=1
      RR_GRACEFUL=0
      shift
      ;;
    --graceful)
      RR_RS=0
      RR_MASTER=0
      RR_GRACEFUL=1
      shift
      ;;
    --maxthreads)
      shift
      RR_MAXTHREADS=$1
      shift
      ;;
    --noack)
      RR_NOACK="--noack"
      shift
      ;;
    --movetimeout)
      shift
      RR_MOVE_TIMEOUT=$1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo Bad argument: $1
      usage
      exit 1
      ;;
  esac
done

# quick function to get a value from the HBase config file
# HBASE-6504 - only take the first line of the output in case verbose gc is on
distMode=`HBASE_CONF_DIR=${HBASE_CONF_DIR} $bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool hbase.cluster.distributed | head -n 1`
if [ "$distMode" == 'false' ]; then
  if [ $RR_RS -ne 1 ] || [ $RR_MASTER -ne 1 ]; then
    echo Cant do selective rolling restart if not running distributed
    exit 1
  fi
  "$bin"/hbase-daemon.sh ${START_CMD_NON_DIST_MODE} master
else
  zparent=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.parent`
  if [ "$zparent" == "null" ]; then zparent="/hbase"; fi

  if [ $RR_MASTER -eq 1 ]; then
    # stop all masters before re-start to avoid races for master znode
    "$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" stop master
    "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
      --hosts "${HBASE_BACKUP_MASTERS}" stop master-backup

    # make sure the master znode has been deleted before continuing
    zmaster=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.master`
    if [ "$zmaster" == "null" ]; then zmaster="master"; fi
    zmaster=$zparent/$zmaster
    echo -n "Waiting for Master ZNode ${zmaster} to expire"
    echo
    while ! "$bin"/hbase zkcli stat $zmaster 2>&1 | grep "Node does not exist"; do
      echo -n "."
      sleep 1
    done
    echo #force a newline

    # all masters are down, now restart
    "$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" ${START_CMD_DIST_MODE} master
    "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
      --hosts "${HBASE_BACKUP_MASTERS}" ${START_CMD_DIST_MODE} master-backup

    echo "Wait a minute for master to come up join cluster"
    sleep 60

    # Master joing cluster will start in cleaning out regions in transition.
    # Wait until the master has cleaned out regions in transition before
    # giving it a bunch of work to do; master is vulnerable during startup
    zunassigned=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.unassigned`
    if [ "$zunassigned" == "null" ]; then zunassigned="region-in-transition"; fi
    zunassigned="$zparent/$zunassigned"
    # Checking if /hbase/region-in-transition exist
    ritZnodeCheck=`$bin/hbase zkcli stat ${zunassigned} 2>&1 | tail -1 \
		  | grep "Node does not exist:" >/dev/null`
    ret=$?
    if test 0 -eq ${ret}
    then
      echo "Znode ${zunassigned} does not exist"
    else
      echo -n "Waiting for ${zunassigned} to empty"
      while true ; do
        unassigned=`$bin/hbase zkcli stat ${zunassigned} 2>&1 \
		   | grep -e 'numChildren = '|sed -e 's,numChildren = ,,'`
        if test 0 -eq ${unassigned}
        then
          echo
          break
        else
          echo -n " ${unassigned}"
        fi
        sleep 1
      done
    fi
  fi

  if [ $RR_RS -eq 1 ]; then
    # unlike the masters, roll all regionservers one-at-a-time
    export HBASE_SLAVE_PARALLEL=false
    "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
      --hosts "${HBASE_REGIONSERVERS}" ${RESTART_CMD_REGIONSERVER} regionserver
  fi

  if [ $RR_GRACEFUL -eq 1 ]; then
    # gracefully restart all online regionservers
    masterport=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool hbase.master.port`
    if [ "$masterport" == "null" ]; then masterport="16000"; fi
    zkrs=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.rs`
    if [ "$zkrs" == "null" ]; then zkrs="rs"; fi
    zkrs="$zparent/$zkrs"
    online_regionservers=`$bin/hbase zkcli ls $zkrs 2>&1 | tail -1 | sed "s/\[//" | sed "s/\]//"`
    echo "Disabling load balancer"
    HBASE_BALANCER_STATE=$(echo 'balance_switch false' | "$bin"/hbase --config "${HBASE_CONF_DIR}" shell -n | tail -1)
    echo "Previous balancer state was $HBASE_BALANCER_STATE"

    for rs in $online_regionservers
    do
        rs_parts=(${rs//,/ })
        hostname=${rs_parts[0]}
        port=${rs_parts[1]}
        if [ "$port" -eq "$masterport" ]; then
          echo "Skipping regionserver on master machine $hostname:$port"
          continue
        else
          echo "Gracefully restarting: $hostname"
          "$bin"/graceful_stop.sh --config ${HBASE_CONF_DIR} --restart --reload -nob --maxthreads  \
		${RR_MAXTHREADS} ${RR_NOACK} --movetimeout ${RR_MOVE_TIMEOUT} $hostname
          sleep 1
        fi
    done
    if [ "$HBASE_BALANCER_STATE" != "false" ]; then
      echo "Restoring balancer state to $HBASE_BALANCER_STATE"
      echo "balance_switch $HBASE_BALANCER_STATE" | "$bin"/hbase --config "${HBASE_CONF_DIR}" shell &> /dev/null
    fi
  fi
fi
