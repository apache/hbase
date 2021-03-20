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

# Move regions off a server then stop it.  Optionally restart and reload.
# Turn off the balancer before running this script.
function usage {
  echo "Usage: graceful_stop.sh [--config <conf-dir>] [-e] [--restart [--reload]] [--thrift] \
[--rest] [-n |--noack] [--maxthreads <number of threads>] [--movetimeout <timeout in seconds>] \
[-nob |--nobalancer] [-d |--designatedfile <file path>] [-x |--excludefile <file path>] <hostname>"
  echo " thrift         If we should stop/start thrift before/after the hbase stop/start"
  echo " rest           If we should stop/start rest before/after the hbase stop/start"
  echo " restart        If we should restart after graceful stop"
  echo " reload         Move offloaded regions back on to the restarted server"
  echo " n|noack        Enable noAck mode in RegionMover. This is a best effort mode for \
moving regions"
  echo " maxthreads xx  Limit the number of threads used by the region mover. Default value is 1."
  echo " movetimeout xx Timeout for moving regions. If regions are not moved by the timeout value,\
exit with error. Default value is INT_MAX."
  echo " hostname       Hostname to stop; match what HBase uses; pass 'localhost' if local to avoid ssh"
  echo " e|failfast     Set -e so exit immediately if any command exits with non-zero status"
  echo " nob|nobalancer Do not manage balancer states. This is only used as optimization in \
rolling_restart.sh to avoid multiple calls to hbase shell"
  echo " d|designatedfile xx Designated file with <hostname:port> per line as unload targets"
  echo " x|excludefile xx Exclude file should have <hostname:port> per line. We do not unload \
regions to hostnames given in exclude file"
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`
# This will set HBASE_HOME, etc.
. "$bin"/hbase-config.sh
# Get arguments
restart=
reload=
noack=
thrift=
rest=
movetimeout=2147483647
maxthreads=1
failfast=
nob=false
designatedfile=
excludefile=
while [ $# -gt 0 ]
do
  case "$1" in
    --thrift)  thrift=true; shift;;
    --rest)  rest=true; shift;;
    --restart)  restart=true; shift;;
    --reload)   reload=true; shift;;
    --failfast | -e)  failfast=true; shift;;
    --noack | -n)  noack="--noack"; shift;;
    --maxthreads) shift; maxthreads=$1; shift;;
    --movetimeout) shift; movetimeout=$1; shift;;
    --nobalancer | -nob) nob=true; shift;;
    --designatedfile | -d) shift; designatedfile=$1; shift;;
    --excludefile | -x) shift; excludefile=$1; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;	# terminate while loop
  esac
done

# "$@" contains the rest. Must be at least the hostname left.
if [ $# -lt 1 ]; then
  usage
fi

# Emit a log line w/ iso8901 date prefixed
log() {
  echo `date +%Y-%m-%dT%H:%M:%S` $1
}

# See if we should set fail fast before we do anything.
if [ "$failfast" != "" ]; then
  log "Set failfast, will exit immediately if any command exits with non-zero status"
  set -e
fi

hostname=$1
filename="/tmp/$hostname"

local=
localhostname=`/bin/hostname -f`

if [ "$localhostname" == "$hostname" ] || [ "$hostname" == "localhost" ]; then
  local=true
  hostname=$localhostname
fi

if [ "$nob" == "true"  ]; then
  log "[ $0 ] skipping disabling balancer -nob argument is used"
  HBASE_BALANCER_STATE=false
else
  log "Disabling load balancer"
  HBASE_BALANCER_STATE=$(echo 'balance_switch false' | "$bin"/hbase --config "${HBASE_CONF_DIR}" shell -n | tail -1)
  log "Previous balancer state was $HBASE_BALANCER_STATE"
fi

unload_args="--filename $filename --maxthreads $maxthreads $noack --operation unload \
--timeout $movetimeout --regionserverhost $hostname"

if [ "$designatedfile" != "" ]; then
  unload_args="$unload_args --designatedfile $designatedfile"
fi

if [ "$excludefile" != "" ]; then
  unload_args="$unload_args --excludefile $excludefile"
fi

log "Unloading $hostname region(s)"
HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.apache.hadoop.hbase.util.RegionMover \
$unload_args
log "Unloaded $hostname region(s)"

# Stop the server(s). Have to put hostname into its own little file for hbase-daemons.sh
hosts="/tmp/$(basename $0).$$.tmp"
echo $hostname >> $hosts
if [ "$thrift" != "" ]; then
  log "Stopping thrift server on $hostname"
  if [ "$local" == true ]; then
    "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} stop thrift
  else
    "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} stop thrift
  fi
fi
if [ "$rest" != "" ]; then
  log "Stopping rest server on $hostname"
  if [ "$local" == true ]; then
    "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} stop rest
  else
    "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} stop rest
  fi
fi
log "Stopping regionserver on $hostname"
if [ "$local" == true ]; then
  "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} stop regionserver
else
  "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} stop regionserver
fi
if [ "$restart" != "" ]; then
  log "Restarting regionserver on $hostname"
  if [ "$local" == true ]; then
    "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} start regionserver
  else
    "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} start regionserver
  fi
  if [ "$thrift" != "" ]; then
    log "Restarting thrift server on $hostname"
    # -b 0.0.0.0 says listen on all interfaces rather than just default.
    if [ "$local" == true ]; then
      "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} start thrift -b 0.0.0.0
    else
      "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} start thrift -b 0.0.0.0
    fi
  fi
  if [ "$rest" != "" ]; then
    log "Restarting rest server on $hostname"
    if [ "$local" == true ]; then
      "$bin"/hbase-daemon.sh --config ${HBASE_CONF_DIR} start rest
    else
      "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} start rest
    fi
  fi
  if [ "$reload" != "" ]; then
    log "Reloading $hostname region(s)"
    HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} \
    org.apache.hadoop.hbase.util.RegionMover --filename $filename --maxthreads $maxthreads $noack \
    --operation "load" --timeout $movetimeout --regionserverhost $hostname
    log "Reloaded $hostname region(s)"
  fi
fi

# Restore balancer state
if [ "$HBASE_BALANCER_STATE" != "false" ] && [ "$nob" != "true"  ]; then
  log "Restoring balancer state to $HBASE_BALANCER_STATE"
  echo "balance_switch $HBASE_BALANCER_STATE" | "$bin"/hbase --config ${HBASE_CONF_DIR} shell &> /dev/null
else
  log "[ $0 ] skipping restoring balancer"
fi

# Cleanup tmp files.
trap "rm -f  /tmp/$(basename $0).*.tmp &> /dev/null" EXIT
