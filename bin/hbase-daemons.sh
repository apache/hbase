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
# Run a hbase command on all slave hosts.
# Modelled after $HADOOP_HOME/bin/hadoop-daemons.sh

usage="Usage: hbase-daemons.sh [--config <hbase-confdir>] [--autostart-window-size <window size in hours>]\
      [--autostart-window-retry-limit <retry count limit for autostart>] \
      [--hosts regionserversfile] [autostart|autorestart|restart|start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

# default autostart args value indicating infinite window size and no retry limit
AUTOSTART_WINDOW_SIZE=0
AUTOSTART_WINDOW_RETRY_LIMIT=0

. $bin/hbase-config.sh

if [[ "$1" = "autostart" || "$1" = "autorestart" ]]
then
  autostart_args="--autostart-window-size ${AUTOSTART_WINDOW_SIZE} --autostart-window-retry-limit ${AUTOSTART_WINDOW_RETRY_LIMIT}"
fi

remote_cmd="$bin/hbase-daemon.sh --config ${HBASE_CONF_DIR} ${autostart_args} $@"
args="--hosts ${HBASE_REGIONSERVERS} --config ${HBASE_CONF_DIR} $remote_cmd"

command=$2
case $command in
  (zookeeper)
    exec "$bin/zookeepers.sh" $args
    ;;
  (master-backup)
    exec "$bin/master-backup.sh" $args
    ;;
  (*)
    exec "$bin/regionservers.sh" $args
    ;;
esac
