/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Wrapper around common arguments used to construct ReplicationQueuesClient. Used to construct
 * various ReplicationQueuesClient Implementations with different constructor arguments by
 * reflection.
 */
@InterfaceAudience.Private
public class ReplicationQueuesClientArguments extends ReplicationQueuesArguments {
  public ReplicationQueuesClientArguments(Configuration conf, Abortable abort,
     ZooKeeperWatcher zk) {
    super(conf, abort, zk);
  }
  public ReplicationQueuesClientArguments(Configuration conf, Abortable abort) {
    super(conf, abort);
  }
}
