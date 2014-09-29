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

usage="Usage: considerAsDead.sh --hostname serverName"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. $bin/hbase-config.sh

shift
deadhost=$@

remote_cmd="cd ${HBASE_HOME}; $bin/hbase-daemon.sh --config ${HBASE_CONF_DIR} restart"

zparent=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.parent`
if [ "$zparent" == "null" ]; then zparent="/hbase"; fi

zkrs=`$bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.rs`
if [ "$zkrs" == "null" ]; then zkrs="rs"; fi

zkrs="$zparent/$zkrs"
online_regionservers=`$bin/hbase zkcli ls $zkrs 2>&1 | tail -1 | sed "s/\[//" | sed "s/\]//"`
for rs in $online_regionservers
do
    rs_parts=(${rs//,/ })
    hostname=${rs_parts[0]}
    echo $deadhost
    echo $hostname   
    if [ "$deadhost" == "$hostname" ]; then
		znode="$zkrs/$rs"
		echo "ZNode Deleting:" $znode
		$bin/hbase zkcli delete $znode > /dev/null 2>&1
		sleep 1
		ssh $HBASE_SSH_OPTS $hostname $remote_cmd 2>&1 | sed "s/^/$hostname: /"	
    fi   
done
