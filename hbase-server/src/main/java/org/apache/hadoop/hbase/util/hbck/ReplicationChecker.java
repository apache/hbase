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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.master.cleaner.ReplicationZKNodeCleaner;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/*
 * Check and fix undeleted replication queues for removed peerId.
 */
@InterfaceAudience.Private
public class ReplicationChecker {
  private final ErrorReporter errorReporter;
  // replicator with its queueIds for removed peers
  private Map<String, List<String>> undeletedQueueIds = new HashMap<>();
  // replicator with its undeleted queueIds for removed peers in hfile-refs queue
  private Set<String> undeletedHFileRefsQueueIds = new HashSet<>();
  private final ReplicationZKNodeCleaner cleaner;

  public ReplicationChecker(Configuration conf, ZooKeeperWatcher zkw, ClusterConnection connection,
      ErrorReporter errorReporter) throws IOException {
    this.cleaner = new ReplicationZKNodeCleaner(conf, zkw, connection);
    this.errorReporter = errorReporter;
  }

  public boolean hasUnDeletedQueues() {
    return errorReporter.getErrorList().contains(
      HBaseFsck.ErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE);
  }

  public void checkUnDeletedQueues() throws IOException {
    undeletedQueueIds = cleaner.getUnDeletedQueues();
    for (Entry<String, List<String>> replicatorAndQueueIds : undeletedQueueIds.entrySet()) {
      String replicator = replicatorAndQueueIds.getKey();
      for (String queueId : replicatorAndQueueIds.getValue()) {
        ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
        String msg = "Undeleted replication queue for removed peer found: "
            + String.format("[removedPeerId=%s, replicator=%s, queueId=%s]", queueInfo.getPeerId(),
              replicator, queueId);
        errorReporter.reportError(HBaseFsck.ErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE,
          msg);
      }
    }

    checkUnDeletedHFileRefsQueues();
  }

  private void checkUnDeletedHFileRefsQueues() throws IOException {
    undeletedHFileRefsQueueIds = cleaner.getUnDeletedHFileRefsQueues();
    if (undeletedHFileRefsQueueIds != null && !undeletedHFileRefsQueueIds.isEmpty()) {
      String msg = "Undeleted replication hfile-refs queue for removed peer found: "
          + undeletedHFileRefsQueueIds + " under hfile-refs node";
      errorReporter
          .reportError(HBaseFsck.ErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE, msg);
    }
  }

  public void fixUnDeletedQueues() throws IOException {
    if (!undeletedQueueIds.isEmpty()) {
      cleaner.removeQueues(undeletedQueueIds);
    }
    fixUnDeletedHFileRefsQueue();
  }

  private void fixUnDeletedHFileRefsQueue() throws IOException {
    if (undeletedHFileRefsQueueIds != null && !undeletedHFileRefsQueueIds.isEmpty()) {
      cleaner.removeHFileRefsQueues(undeletedHFileRefsQueueIds);
    }
  }
}
