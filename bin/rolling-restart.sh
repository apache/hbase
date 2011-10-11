#!/usr/bin/env bash
#
#/**
# * Copyright 2010 The Apache Software Foundation
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
# Run a shell command on all regionserver hosts.
#
# Environment Variables
#
#   HBASE_REGIONSERVERS    File naming remote hosts.
#     Default is ${HADOOP_CONF_DIR}/regionservers
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HBASE_CONF_DIR  Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HADOOP_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HADOOP_SLAVE_TIMEOUT Seconds to wait for timing out a remote command.
#   HADOOP_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HADOOP_HOME/bin/slaves.sh.

usage="Usage: $0 [--config <hbase-confdir>] commands..."

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hbase-config.sh

# start hbase daemons
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi

if [ $# -gt 1 ] && [ "--sleep" = "$1" ]; then
  shift
  export HBASE_SLAVE_SLEEP=$1
  shift
else
  export HBASE_SLAVE_SLEEP=15
fi

# quick function to get a value from the HBase config file
distMode=`$bin/hbase org.apache.hadoop.hbase.HBaseConfTool hbase.cluster.distributed`
if [ "$distMode" == 'false' ]; then
  "$bin"/hbase-daemon.sh restart master
else
  # stop all masters before re-start to avoid races for master znode
  # TODO: Ideally, we should find out who the actual primary is
  # dynamically and kill everyone else first.
  echo "Backup master(s):"
  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
       --hosts "${HBASE_BACKUP_MASTERS}" stop master-backup

  echo "Primary master:"
  "$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" stop master

  echo "Sleeping 120 seconds (for /hbase/master znode expiration)..."

  for ((i = 5; i <= 120; i+=5))
  do
    sleep 5
    echo -n $i "sec.."
  done
  echo ""

  echo "Starting master and backup master(s)..."
  # all masters are down, now restart
  "$bin"/hbase-daemon.sh --config "${HBASE_CONF_DIR}" start master
  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
    --hosts "${HBASE_BACKUP_MASTERS}" start master-backup

  # unlike the masters, roll all regionservers one-at-a-time
  export HBASE_SLAVE_PARALLEL=false
  "$bin"/hbase-daemons.sh --config "${HBASE_CONF_DIR}" \
    --hosts "${HBASE_REGIONSERVERS}" restart regionserver

fi
