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
package org.apache.hadoop.hbase.master.cleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to clean the replication queues belonging to the peer which does not exist.
 */
@InterfaceAudience.Private
public class ReplicationZKNodeCleaner {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationZKNodeCleaner.class);
  private final ReplicationQueueStorage queueStorage;
  private final ReplicationPeers replicationPeers;
  private final ReplicationQueueDeletor queueDeletor;

  public ReplicationZKNodeCleaner(Configuration conf, ZKWatcher zkw, Abortable abortable)
      throws IOException {
    try {
      this.queueStorage = ReplicationStorageFactory.getReplicationQueueStorage(zkw, conf);
      this.replicationPeers =
          ReplicationFactory.getReplicationPeers(zkw, conf, this.queueStorage, abortable);
      this.replicationPeers.init();
      this.queueDeletor = new ReplicationQueueDeletor(zkw, conf, abortable);
    } catch (ReplicationException e) {
      throw new IOException("failed to construct ReplicationZKNodeCleaner", e);
    }
  }

  /**
   * @return undeletedQueues replicator with its queueIds for removed peers
   * @throws IOException
   */
  public Map<ServerName, List<String>> getUnDeletedQueues() throws IOException {
    Map<ServerName, List<String>> undeletedQueues = new HashMap<>();
    Set<String> peerIds = new HashSet<>(this.replicationPeers.getAllPeerIds());
    try {
      List<ServerName> replicators = this.queueStorage.getListOfReplicators();
      if (replicators == null || replicators.isEmpty()) {
        return undeletedQueues;
      }
      for (ServerName replicator : replicators) {
        List<String> queueIds = this.queueStorage.getAllQueues(replicator);
        for (String queueId : queueIds) {
          ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
          if (!peerIds.contains(queueInfo.getPeerId())) {
            undeletedQueues.computeIfAbsent(replicator, (key) -> new ArrayList<>()).add(
              queueId);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Undeleted replication queue for removed peer found: "
                  + String.format("[removedPeerId=%s, replicator=%s, queueId=%s]",
                    queueInfo.getPeerId(), replicator, queueId));
            }
          }
        }
      }
    } catch (ReplicationException ke) {
      throw new IOException("Failed to get the replication queues of all replicators", ke);
    }
    return undeletedQueues;
  }

  /**
   * @return undeletedHFileRefsQueue replicator with its undeleted queueIds for removed peers in
   *         hfile-refs queue
   */
  public Set<String> getUnDeletedHFileRefsQueues() throws IOException {
    Set<String> undeletedHFileRefsQueue = new HashSet<>();
    Set<String> peerIds = new HashSet<>(this.replicationPeers.getAllPeerIds());
    String hfileRefsZNode = queueDeletor.getHfileRefsZNode();
    try {
      List<String> listOfPeers = this.queueStorage.getAllPeersFromHFileRefsQueue();
      Set<String> peers = new HashSet<>(listOfPeers);
      peers.removeAll(peerIds);
      if (!peers.isEmpty()) {
        undeletedHFileRefsQueue.addAll(peers);
      }
    } catch (ReplicationException e) {
      throw new IOException(
          "Failed to get list of all peers from hfile-refs znode " + hfileRefsZNode, e);
    }
    return undeletedHFileRefsQueue;
  }

  private class ReplicationQueueDeletor extends ReplicationStateZKBase {

    public ReplicationQueueDeletor(ZKWatcher zk, Configuration conf, Abortable abortable) {
      super(zk, conf, abortable);
    }

    /**
     * @param replicator The regionserver which has undeleted queue
     * @param queueId The undeleted queue id
     */
    public void removeQueue(final ServerName replicator, final String queueId) throws IOException {
      String queueZnodePath = ZNodePaths
          .joinZNode(ZNodePaths.joinZNode(this.queuesZNode, replicator.getServerName()), queueId);
      try {
        ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
        if (!replicationPeers.getAllPeerIds().contains(queueInfo.getPeerId())) {
          ZKUtil.deleteNodeRecursively(this.zookeeper, queueZnodePath);
          LOG.info("Successfully removed replication queue, replicator: " + replicator +
            ", queueId: " + queueId);
        }
      } catch (KeeperException e) {
        throw new IOException(
            "Failed to delete queue, replicator: " + replicator + ", queueId: " + queueId);
      }
    }

    /**
     * @param hfileRefsQueueId The undeleted hfile-refs queue id
     * @throws IOException
     */
    public void removeHFileRefsQueue(final String hfileRefsQueueId) throws IOException {
      String node = ZNodePaths.joinZNode(this.hfileRefsZNode, hfileRefsQueueId);
      try {
        if (!replicationPeers.getAllPeerIds().contains(hfileRefsQueueId)) {
          ZKUtil.deleteNodeRecursively(this.zookeeper, node);
          LOG.info("Successfully removed hfile-refs queue " + hfileRefsQueueId + " from path "
              + hfileRefsZNode);
        }
      } catch (KeeperException e) {
        throw new IOException("Failed to delete hfile-refs queue " + hfileRefsQueueId
            + " from path " + hfileRefsZNode);
      }
    }

    String getHfileRefsZNode() {
      return this.hfileRefsZNode;
    }
  }

  /**
   * Remove the undeleted replication queue's zk node for removed peers.
   * @param undeletedQueues replicator with its queueIds for removed peers
   * @throws IOException
   */
  public void removeQueues(final Map<ServerName, List<String>> undeletedQueues) throws IOException {
    for (Entry<ServerName, List<String>> replicatorAndQueueIds : undeletedQueues.entrySet()) {
      ServerName replicator = replicatorAndQueueIds.getKey();
      for (String queueId : replicatorAndQueueIds.getValue()) {
        queueDeletor.removeQueue(replicator, queueId);
      }
    }
  }

  /**
   * Remove the undeleted hfile-refs queue's zk node for removed peers.
   * @param undeletedHFileRefsQueues replicator with its undeleted queueIds for removed peers in
   *          hfile-refs queue
   * @throws IOException
   */
  public void removeHFileRefsQueues(final Set<String> undeletedHFileRefsQueues) throws IOException {
    for (String hfileRefsQueueId : undeletedHFileRefsQueues) {
      queueDeletor.removeHFileRefsQueue(hfileRefsQueueId);
    }
  }
}
