#!/usr/bin/env bash
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
# This is used for starting multiple regionservers on the same machine.
# run it from hbase-dir/ just like 'bin/hbase'
# Supports up to 10 regionservers (limitation = overlapping ports)
# For supporting more instances select different values (e.g. 16200, 16300)
# for HBASE_RS_BASE_PORT and HBASE_RS_INFO_BASE_PORT below
if [ -z "$HBASE_RS_BASE_PORT" ]; then
  HBASE_RS_BASE_PORT=16020
fi
if [ -z "$HBASE_RS_INFO_BASE_PORT" ]; then
  HBASE_RS_INFO_BASE_PORT=16030
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin" >/dev/null && pwd`

if [ $# -lt 2 ]; then
  S=`basename "${BASH_SOURCE-$0}"`
  echo "Usage: $S [--config <conf-dir>]  [--autostart-window-size <window size in hours>]"
  echo "  [--autostart-window-retry-limit <retry count limit for autostart>] [autostart|start|stop] offset(s)"
  echo "    e.g. $S start 1 2"
  exit
fi

# default autostart args value indicating infinite window size and no retry limit
AUTOSTART_WINDOW_SIZE=0
AUTOSTART_WINDOW_RETRY_LIMIT=0

. "$bin"/hbase-config.sh

# sanity check: make sure your regionserver opts don't use ports [i.e. JMX/DBG]
export HBASE_REGIONSERVER_OPTS=" "

run_regionserver () {
  DN=$2
  export HBASE_IDENT_STRING="$USER-$DN"
  HBASE_REGIONSERVER_ARGS="\
    -Dhbase.regionserver.port=`expr "$HBASE_RS_BASE_PORT" + "$DN"` \
    -Dhbase.regionserver.info.port=`expr "$HBASE_RS_INFO_BASE_PORT" + "$DN"`"

  "$bin"/hbase-daemon.sh  --config "${HBASE_CONF_DIR}" \
    --autostart-window-size "${AUTOSTART_WINDOW_SIZE}" \
    --autostart-window-retry-limit "${AUTOSTART_WINDOW_RETRY_LIMIT}" \
    "$1" regionserver "$HBASE_REGIONSERVER_ARGS"
}

cmd=$1
shift;

for i in "$@"
do
  if [[ "$i" =~ ^[0-9]+$ ]]; then
   run_regionserver "$cmd" "$i"
  else
   echo "Invalid argument"
  fi
done
