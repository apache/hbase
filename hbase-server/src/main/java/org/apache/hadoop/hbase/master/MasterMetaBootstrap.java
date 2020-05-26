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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.store.LocalStore;
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

  private final LocalStore localStore;

  public MasterMetaBootstrap(HMaster master, LocalStore localStore) {
    this.master = master;
    this.localStore = localStore;
  }

  /**
   * For assigning hbase:meta replicas only.
   */
  void assignMetaReplicas() throws IOException, InterruptedException, KeeperException {
    int numReplicas = master.getConfiguration().getInt(HConstants.META_REPLICAS_NUM,
      HConstants.DEFAULT_META_REPLICA_NUM);
    // only try to assign meta replicas when there are more than 1 replicas
    if (numReplicas > 1) {
      final AssignmentManager am = master.getAssignmentManager();
      if (!am.isMetaLoaded()) {
        throw new IllegalStateException(
          "hbase:meta must be initialized first before we can " + "assign out its replicas");
      }
      RegionStates regionStates = am.getRegionStates();
      for (RegionInfo regionInfo : regionStates.getRegionsOfTable(TableName.META_TABLE_NAME)) {
        if (!RegionReplicaUtil.isDefaultReplica(regionInfo)) {
          continue;
        }
        RegionState regionState = regionStates.getRegionState(regionInfo);
        Set<ServerName> metaServerNames = new HashSet<ServerName>();
        if (regionState.getServerName() != null) {
          metaServerNames.add(regionState.getServerName());
        }
        for (int i = 1; i < numReplicas; i++) {
          RegionInfo secondaryRegionInfo = RegionReplicaUtil.getRegionInfoForReplica(regionInfo, i);
          RegionState secondaryRegionState = regionStates.getRegionState(secondaryRegionInfo);
          ServerName sn = null;
          if (secondaryRegionState != null) {
            sn = secondaryRegionState.getServerName();
            if (sn != null && !metaServerNames.add(sn)) {
              LOG.info("{} old location {} is same with other hbase:meta replica location;" +
                " setting location as null...", secondaryRegionInfo.getRegionNameAsString(), sn);
              sn = null;
            }
          }
          // These assigns run inline. All is blocked till they complete. Only interrupt is shutting
          // down hosting server which calls AM#stop.
          if (sn != null) {
            am.assign(secondaryRegionInfo, sn);
          } else {
            am.assign(secondaryRegionInfo);
          }
        }
      }
    }
    // always try to remomve excess meta replicas
    unassignExcessMetaReplica(numReplicas);
  }

  private void unassignExcessMetaReplica(int numMetaReplicasConfigured) {
    ZKWatcher zooKeeper = master.getZooKeeper();
    AssignmentManager am = master.getAssignmentManager();
    RegionStates regionStates = am.getRegionStates();
    Map<RegionInfo, Integer> region2MaxReplicaId = new HashMap<>();
    for (RegionInfo regionInfo : regionStates.getRegionsOfTable(TableName.META_TABLE_NAME)) {
      RegionInfo primaryRegionInfo = RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo);
      region2MaxReplicaId.compute(primaryRegionInfo,
        (k, v) -> v == null ? regionInfo.getReplicaId() : Math.max(v, regionInfo.getReplicaId()));
      if (regionInfo.getReplicaId() < numMetaReplicasConfigured) {
        continue;
      }
      RegionState regionState = regionStates.getRegionState(regionInfo);
      try {
        ServerManager.closeRegionSilentlyAndWait(master.getAsyncClusterConnection(),
          regionState.getServerName(), regionInfo, 30000);
        if (regionInfo.isFirst()) {
          // for compatibility, also try to remove the replicas on zk.
          ZKUtil.deleteNode(zooKeeper,
            zooKeeper.getZNodePaths().getZNodeForReplica(regionInfo.getReplicaId()));
        }
      } catch (Exception e) {
        // ignore the exception since we don't want the master to be wedged due to potential
        // issues in the cleanup of the extra regions. We can do that cleanup via hbck or manually
        LOG.warn("Ignoring exception " + e);
      }
      regionStates.deleteRegion(regionInfo);
    }
    region2MaxReplicaId.forEach((regionInfo, maxReplicaId) -> {
      if (maxReplicaId >= numMetaReplicasConfigured) {
        byte[] metaRow = MetaTableAccessor.getMetaKeyForRegion(regionInfo);
        Delete delete = MetaTableAccessor.removeRegionReplica(metaRow, numMetaReplicasConfigured,
          maxReplicaId - numMetaReplicasConfigured + 1);
        try {
          localStore.update(r -> r.delete(delete));
        } catch (IOException e) {
          // ignore the exception since we don't want the master to be wedged due to potential
          // issues in the cleanup of the extra regions. We can do that cleanup via hbck or manually
          LOG.warn("Ignoring exception " + e);
        }
      }
    });
  }
}
