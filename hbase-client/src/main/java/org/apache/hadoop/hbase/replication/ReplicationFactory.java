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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * A factory class for instantiating replication objects that deal with replication state.
 */
@InterfaceAudience.Private
public class ReplicationFactory {

  public static ReplicationQueues getReplicationQueues(final ZooKeeperWatcher zk,
      Configuration conf, Abortable abortable) {
    return new ReplicationQueuesZKImpl(zk, conf, abortable);
  }

  public static ReplicationQueuesClient getReplicationQueuesClient(final ZooKeeperWatcher zk,
      Configuration conf, Abortable abortable) {
    return new ReplicationQueuesClientZKImpl(zk, conf, abortable);
  }

  public static ReplicationPeers getReplicationPeers(final ZooKeeperWatcher zk, Configuration conf,
      Abortable abortable) {
    return getReplicationPeers(zk, conf, null, abortable);
  }
  
  public static ReplicationPeers getReplicationPeers(final ZooKeeperWatcher zk, Configuration conf,
      final ReplicationQueuesClient queuesClient, Abortable abortable) {
    return new ReplicationPeersZKImpl(zk, conf, queuesClient, abortable);
  }

  public static ReplicationTracker getReplicationTracker(ZooKeeperWatcher zookeeper,
      final ReplicationPeers replicationPeers, Configuration conf, Abortable abortable,
      Stoppable stopper) {
    return new ReplicationTrackerZKImpl(zookeeper, replicationPeers, conf, abortable, stopper);
  }
}
