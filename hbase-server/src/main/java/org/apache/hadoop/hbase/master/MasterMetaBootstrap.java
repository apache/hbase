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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Used by the HMaster on startup to split meta logs and assign the meta table.
 */
@InterfaceAudience.Private
public class MasterMetaBootstrap {
  private static final Log LOG = LogFactory.getLog(MasterMetaBootstrap.class);

  private final MonitoredTask status;
  private final HMaster master;

  private Set<ServerName> previouslyFailedServers;
  private Set<ServerName> previouslyFailedMetaRSs;

  public MasterMetaBootstrap(final HMaster master, final MonitoredTask status) {
    this.master = master;
    this.status = status;
  }

  public void splitMetaLogsBeforeAssignment() throws IOException, KeeperException {
    // get a list for previously failed RS which need log splitting work
    // we recover hbase:meta region servers inside master initialization and
    // handle other failed servers in SSH in order to start up master node ASAP
    previouslyFailedServers = master.getMasterWalManager().getFailedServersFromLogFolders();

    // log splitting for hbase:meta server
    ServerName oldMetaServerLocation = master.getMetaTableLocator()
        .getMetaRegionLocation(master.getZooKeeper());
    if (oldMetaServerLocation != null && previouslyFailedServers.contains(oldMetaServerLocation)) {
      splitMetaLogBeforeAssignment(oldMetaServerLocation);
      // Note: we can't remove oldMetaServerLocation from previousFailedServers list because it
      // may also host user regions
    }
    previouslyFailedMetaRSs = getPreviouselyFailedMetaServersFromZK();
    // need to use union of previouslyFailedMetaRSs recorded in ZK and previouslyFailedServers
    // instead of previouslyFailedMetaRSs alone to address the following two situations:
    // 1) the chained failure situation(recovery failed multiple times in a row).
    // 2) master get killed right before it could delete the recovering hbase:meta from ZK while the
    // same server still has non-meta wals to be replayed so that
    // removeStaleRecoveringRegionsFromZK can't delete the stale hbase:meta region
    // Passing more servers into splitMetaLog is all right. If a server doesn't have hbase:meta wal,
    // there is no op for the server.
    previouslyFailedMetaRSs.addAll(previouslyFailedServers);
  }

  public void assignMeta() throws InterruptedException, IOException, KeeperException {
    assignMeta(previouslyFailedMetaRSs, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  public void processDeadServers() throws IOException {
    // Master has recovered hbase:meta region server and we put
    // other failed region servers in a queue to be handled later by SSH
    for (ServerName tmpServer : previouslyFailedServers) {
      master.getServerManager().processDeadServer(tmpServer, true);
    }
  }

  public void assignMetaReplicas()
      throws IOException, InterruptedException, KeeperException {
    int numReplicas = master.getConfiguration().getInt(HConstants.META_REPLICAS_NUM,
           HConstants.DEFAULT_META_REPLICA_NUM);
    final Set<ServerName> EMPTY_SET = new HashSet<ServerName>();
    for (int i = 1; i < numReplicas; i++) {
      assignMeta(EMPTY_SET, i);
    }
    unassignExcessMetaReplica(numReplicas);
  }

  private void splitMetaLogBeforeAssignment(ServerName currentMetaServer) throws IOException {
    if (RecoveryMode.LOG_REPLAY == master.getMasterWalManager().getLogRecoveryMode()) {
      // In log replay mode, we mark hbase:meta region as recovering in ZK
      master.getMasterWalManager().prepareLogReplay(currentMetaServer,
        Collections.<HRegionInfo>singleton(HRegionInfo.FIRST_META_REGIONINFO));
    } else {
      // In recovered.edits mode: create recovered edits file for hbase:meta server
      master.getMasterWalManager().splitMetaLog(currentMetaServer);
    }
  }

  private void unassignExcessMetaReplica(int numMetaReplicasConfigured) {
    final ZooKeeperWatcher zooKeeper = master.getZooKeeper();
    // unassign the unneeded replicas (for e.g., if the previous master was configured
    // with a replication of 3 and now it is 2, we need to unassign the 1 unneeded replica)
    try {
      List<String> metaReplicaZnodes = zooKeeper.getMetaReplicaNodes();
      for (String metaReplicaZnode : metaReplicaZnodes) {
        int replicaId = zooKeeper.znodePaths.getMetaReplicaIdFromZnode(metaReplicaZnode);
        if (replicaId >= numMetaReplicasConfigured) {
          RegionState r = MetaTableLocator.getMetaRegionState(zooKeeper, replicaId);
          LOG.info("Closing excess replica of meta region " + r.getRegion());
          // send a close and wait for a max of 30 seconds
          ServerManager.closeRegionSilentlyAndWait(master.getClusterConnection(),
              r.getServerName(), r.getRegion(), 30000);
          ZKUtil.deleteNode(zooKeeper, zooKeeper.znodePaths.getZNodeForReplica(replicaId));
        }
      }
    } catch (Exception ex) {
      // ignore the exception since we don't want the master to be wedged due to potential
      // issues in the cleanup of the extra regions. We can do that cleanup via hbck or manually
      LOG.warn("Ignoring exception " + ex);
    }
  }

  /**
   * Check <code>hbase:meta</code> is assigned. If not, assign it.
   */
  protected void assignMeta(Set<ServerName> previouslyFailedMetaRSs, int replicaId)
      throws InterruptedException, IOException, KeeperException {
    final AssignmentManager assignmentManager = master.getAssignmentManager();

    // Work on meta region
    int assigned = 0;
    long timeout = master.getConfiguration().getLong("hbase.catalog.verification.timeout", 1000);
    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
      status.setStatus("Assigning hbase:meta region");
    } else {
      status.setStatus("Assigning hbase:meta region, replicaId " + replicaId);
    }

    // Get current meta state from zk.
    RegionState metaState = MetaTableLocator.getMetaRegionState(master.getZooKeeper(), replicaId);
    HRegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO,
        replicaId);
    RegionStates regionStates = assignmentManager.getRegionStates();
    regionStates.createRegionState(hri, metaState.getState(),
        metaState.getServerName(), null);

    if (!metaState.isOpened() || !master.getMetaTableLocator().verifyMetaRegionLocation(
        master.getClusterConnection(), master.getZooKeeper(), timeout, replicaId)) {
      ServerName currentMetaServer = metaState.getServerName();
      if (master.getServerManager().isServerOnline(currentMetaServer)) {
        if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
          LOG.info("Meta was in transition on " + currentMetaServer);
        } else {
          LOG.info("Meta with replicaId " + replicaId + " was in transition on " +
                    currentMetaServer);
        }
        assignmentManager.processRegionsInTransition(Collections.singletonList(metaState));
      } else {
        if (currentMetaServer != null) {
          if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
            splitMetaLogBeforeAssignment(currentMetaServer);
            regionStates.logSplit(HRegionInfo.FIRST_META_REGIONINFO);
            previouslyFailedMetaRSs.add(currentMetaServer);
          }
        }
        LOG.info("Re-assigning hbase:meta with replicaId, " + replicaId +
            " it was on " + currentMetaServer);
        assignmentManager.assignMeta(hri);
      }
      assigned++;
    }

    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
      // TODO: should we prevent from using state manager before meta was initialized?
      // tableStateManager.start();
      master.getTableStateManager()
        .setTableState(TableName.META_TABLE_NAME, TableState.State.ENABLED);
    }

    if ((RecoveryMode.LOG_REPLAY == master.getMasterWalManager().getLogRecoveryMode())
        && (!previouslyFailedMetaRSs.isEmpty())) {
      // replay WAL edits mode need new hbase:meta RS is assigned firstly
      status.setStatus("replaying log for Meta Region");
      master.getMasterWalManager().splitMetaLog(previouslyFailedMetaRSs);
    }

    assignmentManager.setEnabledTable(TableName.META_TABLE_NAME);
    master.getTableStateManager().start();

    // Make sure a hbase:meta location is set. We need to enable SSH here since
    // if the meta region server is died at this time, we need it to be re-assigned
    // by SSH so that system tables can be assigned.
    // No need to wait for meta is assigned = 0 when meta is just verified.
    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) enableCrashedServerProcessing(assigned != 0);
    LOG.info("hbase:meta with replicaId " + replicaId + " assigned=" + assigned + ", location="
      + master.getMetaTableLocator().getMetaRegionLocation(master.getZooKeeper(), replicaId));
    status.setStatus("META assigned.");
  }

  private void enableCrashedServerProcessing(final boolean waitForMeta)
      throws IOException, InterruptedException {
    // If crashed server processing is disabled, we enable it and expire those dead but not expired
    // servers. This is required so that if meta is assigning to a server which dies after
    // assignMeta starts assignment, ServerCrashProcedure can re-assign it. Otherwise, we will be
    // stuck here waiting forever if waitForMeta is specified.
    if (!master.isServerCrashProcessingEnabled()) {
      master.setServerCrashProcessingEnabled(true);
      master.getServerManager().processQueuedDeadServers();
    }

    if (waitForMeta) {
      master.getMetaTableLocator().waitMetaRegionLocation(master.getZooKeeper());
    }
  }

  /**
   * This function returns a set of region server names under hbase:meta recovering region ZK node
   * @return Set of meta server names which were recorded in ZK
   */
  private Set<ServerName> getPreviouselyFailedMetaServersFromZK() throws KeeperException {
    final ZooKeeperWatcher zooKeeper = master.getZooKeeper();
    Set<ServerName> result = new HashSet<ServerName>();
    String metaRecoveringZNode = ZKUtil.joinZNode(zooKeeper.znodePaths.recoveringRegionsZNode,
      HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    List<String> regionFailedServers = ZKUtil.listChildrenNoWatch(zooKeeper, metaRecoveringZNode);
    if (regionFailedServers == null) return result;

    for (String failedServer : regionFailedServers) {
      ServerName server = ServerName.parseServerName(failedServer);
      result.add(server);
    }
    return result;
  }
}
