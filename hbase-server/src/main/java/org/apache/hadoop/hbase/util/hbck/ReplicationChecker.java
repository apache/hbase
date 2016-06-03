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

package org.apache.hadoop.hbase.util.hbck;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/*
 * Check and fix undeleted replication queues for removed peerId.
 */
@InterfaceAudience.Private
public class ReplicationChecker {
  private static final Log LOG = LogFactory.getLog(ReplicationChecker.class);
  private final ZooKeeperWatcher zkw;
  private ErrorReporter errorReporter;
  private ReplicationQueuesClient queuesClient;
  private ReplicationPeers replicationPeers;
  private ReplicationQueueDeletor queueDeletor;
  // replicator with its queueIds for removed peers
  private Map<String, List<String>> undeletedQueueIds = new HashMap<String, List<String>>();
  // Set of un deleted hfile refs queue Ids
  private Set<String> undeletedHFileRefsQueueIds = new HashSet<>();
  private final String hfileRefsZNode;

  public ReplicationChecker(Configuration conf, ZooKeeperWatcher zkw, HConnection connection,
      ErrorReporter errorReporter) throws IOException {
    try {
      this.zkw = zkw;
      this.errorReporter = errorReporter;
      this.queuesClient = ReplicationFactory.getReplicationQueuesClient(zkw, conf, connection);
      this.queuesClient.init();
      this.replicationPeers = ReplicationFactory.getReplicationPeers(zkw, conf, this.queuesClient,
        connection);
      this.replicationPeers.init();
      this.queueDeletor = new ReplicationQueueDeletor(zkw, conf, connection);
    } catch (ReplicationException e) {
      throw new IOException("failed to construct ReplicationChecker", e);
    }

    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    String replicationZNode = ZKUtil.joinZNode(this.zkw.baseZNode, replicationZNodeName);
    String hfileRefsZNodeName =
        conf.get(ReplicationStateZKBase.ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_KEY,
          ReplicationStateZKBase.ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_DEFAULT);
    hfileRefsZNode = ZKUtil.joinZNode(replicationZNode, hfileRefsZNodeName);
  }

  public boolean hasUnDeletedQueues() {
    return errorReporter.getErrorList()
        .contains(HBaseFsck.ErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE);
  }

  public void checkUnDeletedQueues() throws IOException {
    Set<String> peerIds = new HashSet<String>(this.replicationPeers.getAllPeerIds());
    try {
      List<String> replicators = this.queuesClient.getListOfReplicators();
      for (String replicator : replicators) {
        List<String> queueIds = this.queuesClient.getAllQueues(replicator);
        for (String queueId : queueIds) {
          ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
          if (!peerIds.contains(queueInfo.getPeerId())) {
            if (!undeletedQueueIds.containsKey(replicator)) {
              undeletedQueueIds.put(replicator, new ArrayList<String>());
            }
            undeletedQueueIds.get(replicator).add(queueId);

            String msg = "Undeleted replication queue for removed peer found: "
                + String.format("[removedPeerId=%s, replicator=%s, queueId=%s]",
                  queueInfo.getPeerId(), replicator, queueId);
            errorReporter.reportError(
              HBaseFsck.ErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE, msg);
          }
        }
      }
    } catch (KeeperException ke) {
      throw new IOException(ke);
    }

    checkUnDeletedHFileRefsQueues(peerIds);
  }

  private void checkUnDeletedHFileRefsQueues(Set<String> peerIds) throws IOException {
    try {
      if (-1 == ZKUtil.checkExists(zkw, hfileRefsZNode)) {
        return;
      }
      List<String> listOfPeers = this.queuesClient.getAllPeersFromHFileRefsQueue();
      Set<String> peers = new HashSet<>(listOfPeers);
      peers.removeAll(peerIds);
      if (!peers.isEmpty()) {
        undeletedHFileRefsQueueIds.addAll(peers);
        String msg =
            "Undeleted replication hfile-refs queue for removed peer found: "
                + undeletedHFileRefsQueueIds + " under hfile-refs node " + hfileRefsZNode;
        errorReporter.reportError(HBaseFsck.ErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE,
          msg);
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to get list of all peers from hfile-refs znode "
          + hfileRefsZNode, e);
    }
  }

  private static class ReplicationQueueDeletor extends ReplicationStateZKBase {
    public ReplicationQueueDeletor(ZooKeeperWatcher zk, Configuration conf, Abortable abortable) {
      super(zk, conf, abortable);
    }

    public void removeQueue(String replicator, String queueId) throws IOException {
      String queueZnodePath = ZKUtil.joinZNode(ZKUtil.joinZNode(this.queuesZNode, replicator),
        queueId);
      try {
        ZKUtil.deleteNodeRecursively(this.zookeeper, queueZnodePath);
        LOG.info("remove replication queue, replicator: " + replicator + ", queueId: " + queueId);
      } catch (KeeperException e) {
        throw new IOException("failed to delete queue, replicator: " + replicator + ", queueId: "
            + queueId);
      }
    }
  }

  public void fixUnDeletedQueues() throws IOException {
    for (Entry<String, List<String>> replicatorAndQueueIds : undeletedQueueIds.entrySet()) {
      String replicator = replicatorAndQueueIds.getKey();
      for (String queueId : replicatorAndQueueIds.getValue()) {
        queueDeletor.removeQueue(replicator, queueId);
      }
    }
    fixUnDeletedHFileRefsQueue();
  }

  private void fixUnDeletedHFileRefsQueue() throws IOException {
    for (String hfileRefsQueueId : undeletedHFileRefsQueueIds) {
      String node = ZKUtil.joinZNode(hfileRefsZNode, hfileRefsQueueId);
      try {
        ZKUtil.deleteNodeRecursively(this.zkw, node);
        LOG.info("Successfully deleted hfile-refs queue " + hfileRefsQueueId + " from path "
            + hfileRefsZNode);
      } catch (KeeperException e) {
        throw new IOException("Failed to delete hfile-refs queue " + hfileRefsQueueId
            + " from path " + hfileRefsZNode);
      }
    }
  }
}
