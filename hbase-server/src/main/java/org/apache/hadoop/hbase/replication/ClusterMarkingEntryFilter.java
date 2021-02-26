/**
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

import java.util.UUID;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WAL.Entry;


/**
 * Filters out entries with our peerClusterId (i.e. already replicated)
 * and marks all other entries with our clusterID
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
@InterfaceStability.Evolving
public class ClusterMarkingEntryFilter implements WALEntryFilter {
  private UUID clusterId;
  private UUID peerClusterId;
  private ReplicationEndpoint replicationEndpoint;

  /**
   * @param clusterId id of this cluster
   * @param peerClusterId of the other cluster
   * @param replicationEndpoint ReplicationEndpoint which will handle the actual replication
   */
  public ClusterMarkingEntryFilter(UUID clusterId, UUID peerClusterId, ReplicationEndpoint replicationEndpoint) {
    this.clusterId = clusterId;
    this.peerClusterId = peerClusterId;
    this.replicationEndpoint = replicationEndpoint;
  }
  @Override
  public Entry filter(Entry entry) {
    // don't replicate if the log entries have already been consumed by the cluster
    if (replicationEndpoint.canReplicateToSameCluster()
        || !entry.getKey().getClusterIds().contains(peerClusterId)) {
      WALEdit edit = entry.getEdit();
      WALKeyImpl logKey = (WALKeyImpl)entry.getKey();

      if (edit != null && !edit.isEmpty()) {
        // Mark that the current cluster has the change
        logKey.addClusterId(clusterId);
        return entry;
      }
    }
    return null;
  }
}
