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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by the HMaster on startup to split meta logs and assign the meta table.
 */
@InterfaceAudience.Private
class MasterMetaBootstrap {
  private static final Logger LOG = LoggerFactory.getLogger(MasterMetaBootstrap.class);

  private final HMaster master;

  public MasterMetaBootstrap(HMaster master) {
    this.master = master;
  }

  /**
   * For assigning hbase:meta replicas only.
   * TODO: The way this assign runs, nothing but chance to stop all replicas showing up on same
   * server as the hbase:meta region.
   */
  void assignMetaReplicas()
      throws IOException, InterruptedException, KeeperException {
    int numReplicas = master.getConfiguration().getInt(HConstants.META_REPLICAS_NUM,
           HConstants.DEFAULT_META_REPLICA_NUM);
    if (numReplicas <= 1) {
      // No replicaas to assign. Return.
      return;
    }
    final AssignmentManager assignmentManager = master.getAssignmentManager();
    if (!assignmentManager.isMetaLoaded()) {
      throw new IllegalStateException("hbase:meta must be initialized first before we can " +
          "assign out its replicas");
    }
    ServerName metaServername = MetaTableLocator.getMetaRegionLocation(this.master.getZooKeeper());
    for (int i = 1; i < numReplicas; i++) {
      // Get current meta state for replica from zk.
      RegionState metaState = MetaTableLocator.getMetaRegionState(master.getZooKeeper(), i);
      RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(
          RegionInfoBuilder.FIRST_META_REGIONINFO, i);
      LOG.debug(hri.getRegionNameAsString() + " replica region state from zookeeper=" + metaState);
      if (metaServername.equals(metaState.getServerName())) {
        metaState = null;
        LOG.info(hri.getRegionNameAsString() +
          " old location is same as current hbase:meta location; setting location as null...");
      }
      // These assigns run inline. All is blocked till they complete. Only interrupt is shutting
      // down hosting server which calls AM#stop.
      if (metaState != null && metaState.getServerName() != null) {
        // Try to retain old assignment.
        assignmentManager.assign(hri, metaState.getServerName());
      } else {
        assignmentManager.assign(hri);
      }
    }
    unassignExcessMetaReplica(numReplicas);
  }

  private void unassignExcessMetaReplica(int numMetaReplicasConfigured) {
    final ZKWatcher zooKeeper = master.getZooKeeper();
    // unassign the unneeded replicas (for e.g., if the previous master was configured
    // with a replication of 3 and now it is 2, we need to unassign the 1 unneeded replica)
    try {
      List<String> metaReplicaZnodes = zooKeeper.getMetaReplicaNodes();
      for (String metaReplicaZnode : metaReplicaZnodes) {
        int replicaId = zooKeeper.getZNodePaths().getMetaReplicaIdFromZnode(metaReplicaZnode);
        if (replicaId >= numMetaReplicasConfigured) {
          RegionState r = MetaTableLocator.getMetaRegionState(zooKeeper, replicaId);
          LOG.info("Closing excess replica of meta region " + r.getRegion());
          // send a close and wait for a max of 30 seconds
          ServerManager.closeRegionSilentlyAndWait(master.getAsyncClusterConnection(),
              r.getServerName(), r.getRegion(), 30000);
          ZKUtil.deleteNode(zooKeeper, zooKeeper.getZNodePaths().getZNodeForReplica(replicaId));
        }
      }
    } catch (Exception ex) {
      // ignore the exception since we don't want the master to be wedged due to potential
      // issues in the cleanup of the extra regions. We can do that cleanup via hbck or manually
      LOG.warn("Ignoring exception " + ex);
    }
  }
}
