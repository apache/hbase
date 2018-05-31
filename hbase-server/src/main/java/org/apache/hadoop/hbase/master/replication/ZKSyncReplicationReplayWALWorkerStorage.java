/**
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
package org.apache.hadoop.hbase.master.replication;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ZKReplicationStorageBase;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

@InterfaceAudience.Private
public class ZKSyncReplicationReplayWALWorkerStorage extends ZKReplicationStorageBase {

  public static final String WORKERS_ZNODE = "zookeeper.znode.sync.replication.replaywal.workers";

  public static final String WORKERS_ZNODE_DEFAULT = "replaywal-workers";

  /**
   * The name of the znode that contains a list of workers to replay wal.
   */
  private final String workersZNode;

  public ZKSyncReplicationReplayWALWorkerStorage(ZKWatcher zookeeper, Configuration conf) {
    super(zookeeper, conf);
    String workersZNodeName = conf.get(WORKERS_ZNODE, WORKERS_ZNODE_DEFAULT);
    workersZNode = ZNodePaths.joinZNode(replicationZNode, workersZNodeName);
  }

  private String getPeerNode(String peerId) {
    return ZNodePaths.joinZNode(workersZNode, peerId);
  }

  public void addPeer(String peerId) throws ReplicationException {
    try {
      ZKUtil.createWithParents(zookeeper, getPeerNode(peerId));
    } catch (KeeperException e) {
      throw new ReplicationException(
          "Failed to add peer id=" + peerId + " to replaywal-workers storage", e);
    }
  }

  public void removePeer(String peerId) throws ReplicationException {
    try {
      ZKUtil.deleteNodeRecursively(zookeeper, getPeerNode(peerId));
    } catch (KeeperException e) {
      throw new ReplicationException(
          "Failed to remove peer id=" + peerId + " to replaywal-workers storage", e);
    }
  }

  private String getPeerWorkerNode(String peerId, ServerName worker) {
    return ZNodePaths.joinZNode(getPeerNode(peerId), worker.getServerName());
  }

  public void addPeerWorker(String peerId, ServerName worker) throws ReplicationException {
    try {
      ZKUtil.createWithParents(zookeeper, getPeerWorkerNode(peerId, worker));
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to add worker=" + worker + " for peer id=" + peerId,
          e);
    }
  }

  public void removePeerWorker(String peerId, ServerName worker) throws ReplicationException {
    try {
      ZKUtil.deleteNode(zookeeper, getPeerWorkerNode(peerId, worker));
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to remove worker=" + worker + " for peer id=" + peerId,
          e);
    }
  }

  public Set<ServerName> getPeerWorkers(String peerId) throws ReplicationException {
    try {
      List<String> children = ZKUtil.listChildrenNoWatch(zookeeper, getPeerNode(peerId));
      if (children == null) {
        return new HashSet<>();
      }
      return children.stream().map(ServerName::valueOf).collect(Collectors.toSet());
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to list workers for peer id=" + peerId, e);
    }
  }
}
