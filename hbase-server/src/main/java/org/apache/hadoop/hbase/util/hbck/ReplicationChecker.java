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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.util.HbckErrorReporter;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check and fix undeleted replication queues for removed peerId.
 */
@InterfaceAudience.Private
public class ReplicationChecker {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationChecker.class);

  private final HbckErrorReporter errorReporter;
  // replicator with its queueIds for removed peers
  private Map<ServerName, List<String>> undeletedQueueIds = new HashMap<>();
  // replicator with its undeleted queueIds for removed peers in hfile-refs queue
  private Set<String> undeletedHFileRefsPeerIds = new HashSet<>();

  private final ReplicationPeerStorage peerStorage;
  private final ReplicationQueueStorage queueStorage;

  public ReplicationChecker(Configuration conf, ZKWatcher zkw, HbckErrorReporter errorReporter) {
    this.peerStorage = ReplicationStorageFactory.getReplicationPeerStorage(zkw, conf);
    this.queueStorage = ReplicationStorageFactory.getReplicationQueueStorage(zkw, conf);
    this.errorReporter = errorReporter;
  }

  public boolean hasUnDeletedQueues() {
    return errorReporter.getErrorList()
        .contains(HbckErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE);
  }

  private Map<ServerName, List<String>> getUnDeletedQueues() throws ReplicationException {
    Map<ServerName, List<String>> undeletedQueues = new HashMap<>();
    Set<String> peerIds = new HashSet<>(peerStorage.listPeerIds());
    for (ServerName replicator : queueStorage.getListOfReplicators()) {
      for (String queueId : queueStorage.getAllQueues(replicator)) {
        ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
        if (!peerIds.contains(queueInfo.getPeerId())) {
          undeletedQueues.computeIfAbsent(replicator, key -> new ArrayList<>()).add(queueId);
          LOG.debug(
            "Undeleted replication queue for removed peer found: " +
              "[removedPeerId={}, replicator={}, queueId={}]",
            queueInfo.getPeerId(), replicator, queueId);
        }
      }
    }
    return undeletedQueues;
  }

  private Set<String> getUndeletedHFileRefsPeers() throws ReplicationException {
    Set<String> undeletedHFileRefsPeerIds =
      new HashSet<>(queueStorage.getAllPeersFromHFileRefsQueue());
    Set<String> peerIds = new HashSet<>(peerStorage.listPeerIds());
    undeletedHFileRefsPeerIds.removeAll(peerIds);
    if (LOG.isDebugEnabled()) {
      for (String peerId : undeletedHFileRefsPeerIds) {
        LOG.debug("Undeleted replication hfile-refs queue for removed peer {} found", peerId);
      }
    }
    return undeletedHFileRefsPeerIds;
  }

  public void checkUnDeletedQueues() throws ReplicationException {
    undeletedQueueIds = getUnDeletedQueues();
    undeletedQueueIds.forEach((replicator, queueIds) -> {
      queueIds.forEach(queueId -> {
        ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
        String msg = "Undeleted replication queue for removed peer found: " +
          String.format("[removedPeerId=%s, replicator=%s, queueId=%s]", queueInfo.getPeerId(),
            replicator, queueId);
        errorReporter.reportError(HbckErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE, msg);
      });
    });
    undeletedHFileRefsPeerIds = getUndeletedHFileRefsPeers();
    undeletedHFileRefsPeerIds.stream().map(
        peerId -> "Undeleted replication hfile-refs queue for removed peer " + peerId + " found")
        .forEach(msg -> errorReporter
            .reportError(HbckErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE, msg));
  }

  public void fixUnDeletedQueues() throws ReplicationException {
    for (Map.Entry<ServerName, List<String>> replicatorAndQueueIds : undeletedQueueIds.entrySet()) {
      ServerName replicator = replicatorAndQueueIds.getKey();
      for (String queueId : replicatorAndQueueIds.getValue()) {
        queueStorage.removeQueue(replicator, queueId);
      }
      queueStorage.removeReplicatorIfQueueIsEmpty(replicator);
    }
    for (String peerId : undeletedHFileRefsPeerIds) {
      queueStorage.removePeerFromHFileRefs(peerId);
    }
  }
}
