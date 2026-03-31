/*
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
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
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
  private Map<ServerName, List<ReplicationQueueId>> undeletedQueueIds = new HashMap<>();
  // replicator with its undeleted queueIds for removed peers in hfile-refs queue
  private Set<String> undeletedHFileRefsPeerIds = new HashSet<>();

  private final ReplicationPeerStorage peerStorage;
  private final ReplicationQueueStorage queueStorage;

  public ReplicationChecker(Configuration conf, ZKWatcher zkw, Connection conn,
    HbckErrorReporter errorReporter) throws IOException {
    this.peerStorage =
      ReplicationStorageFactory.getReplicationPeerStorage(FileSystem.get(conf), zkw, conf);
    this.queueStorage = ReplicationStorageFactory.getReplicationQueueStorage(conn, conf);
    this.errorReporter = errorReporter;
  }

  public boolean hasUnDeletedQueues() {
    return errorReporter.getErrorList()
      .contains(HbckErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE);
  }

  private Map<ServerName, List<ReplicationQueueId>> getUnDeletedQueues()
    throws ReplicationException {
    Map<ServerName, List<ReplicationQueueId>> undeletedQueues = new HashMap<>();
    Set<String> peerIds = new HashSet<>(peerStorage.listPeerIds());
    for (ReplicationQueueData queueData : queueStorage.listAllQueues()) {
      ReplicationQueueId queueId = queueData.getId();
      if (!peerIds.contains(queueId.getPeerId())) {
        undeletedQueues.computeIfAbsent(queueId.getServerName(), key -> new ArrayList<>())
          .add(queueId);
        LOG.debug(
          "Undeleted replication queue for removed peer found: "
            + "[removedPeerId={}, replicator={}, queueId={}]",
          queueId.getPeerId(), queueId.getServerName(), queueId);
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
        String msg = "Undeleted replication queue for removed peer found: "
          + String.format("[removedPeerId=%s, replicator=%s, queueId=%s]", queueId.getPeerId(),
            replicator, queueId);
        errorReporter.reportError(HbckErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE, msg);
      });
    });
    undeletedHFileRefsPeerIds = getUndeletedHFileRefsPeers();
    undeletedHFileRefsPeerIds.stream()
      .map(peerId -> "Undeleted replication hfile-refs queue for removed peer " + peerId + " found")
      .forEach(msg -> errorReporter
        .reportError(HbckErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE, msg));
  }

  public void fixUnDeletedQueues() throws ReplicationException {
    for (Map.Entry<ServerName, List<ReplicationQueueId>> replicatorAndQueueIds : undeletedQueueIds
      .entrySet()) {
      ServerName replicator = replicatorAndQueueIds.getKey();
      for (ReplicationQueueId queueId : replicatorAndQueueIds.getValue()) {
        queueStorage.removeQueue(queueId);
      }
    }
    for (String peerId : undeletedHFileRefsPeerIds) {
      queueStorage.removePeerFromHFileRefs(peerId);
    }
  }

  public boolean checkHasDataInQueues() throws ReplicationException {
    return queueStorage.hasData();
  }
}
