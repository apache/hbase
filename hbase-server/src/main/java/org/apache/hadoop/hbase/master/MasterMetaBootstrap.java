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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Used by the HMaster on startup to split meta logs and assign the meta table.
 */
@InterfaceAudience.Private
public class MasterMetaBootstrap {
  private static final Log LOG = LogFactory.getLog(MasterMetaBootstrap.class);

  private final MonitoredTask status;
  private final HMaster master;

  public MasterMetaBootstrap(final HMaster master, final MonitoredTask status) {
    this.master = master;
    this.status = status;
  }

  public void recoverMeta() throws InterruptedException, IOException {
    master.recoverMeta();
    master.getTableStateManager().start();
    enableCrashedServerProcessing(false);
  }

  public void processDeadServers() {
    // get a list for previously failed RS which need log splitting work
    // we recover hbase:meta region servers inside master initialization and
    // handle other failed servers in SSH in order to start up master node ASAP
    Set<ServerName> previouslyFailedServers =
        master.getMasterWalManager().getFailedServersFromLogFolders();

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
    for (int i = 1; i < numReplicas; i++) {
      assignMeta(i);
    }
    unassignExcessMetaReplica(numReplicas);
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
  protected void assignMeta(int replicaId)
      throws InterruptedException, IOException, KeeperException {
    final AssignmentManager assignmentManager = master.getAssignmentManager();

    // Work on meta region
    // TODO: Unimplemented
    // long timeout =
    //   master.getConfiguration().getLong("hbase.catalog.verification.timeout", 1000);
    if (replicaId == RegionInfo.DEFAULT_REPLICA_ID) {
      status.setStatus("Assigning hbase:meta region");
    } else {
      status.setStatus("Assigning hbase:meta region, replicaId " + replicaId);
    }

    // Get current meta state from zk.
    RegionState metaState = MetaTableLocator.getMetaRegionState(master.getZooKeeper(), replicaId);
    LOG.debug("meta state from zookeeper: " + metaState);
    RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(
        RegionInfoBuilder.FIRST_META_REGIONINFO, replicaId);
    assignmentManager.assignMeta(hri, metaState.getServerName());

    if (replicaId == RegionInfo.DEFAULT_REPLICA_ID) {
      // TODO: should we prevent from using state manager before meta was initialized?
      // tableStateManager.start();
      master.getTableStateManager()
        .setTableState(TableName.META_TABLE_NAME, TableState.State.ENABLED);
    }

    master.getTableStateManager().start();

    // Make sure a hbase:meta location is set. We need to enable SSH here since
    // if the meta region server is died at this time, we need it to be re-assigned
    // by SSH so that system tables can be assigned.
    // No need to wait for meta is assigned = 0 when meta is just verified.
    if (replicaId == RegionInfo.DEFAULT_REPLICA_ID) enableCrashedServerProcessing(false);
    LOG.info("hbase:meta with replicaId " + replicaId + ", location="
      + master.getMetaTableLocator().getMetaRegionLocation(master.getZooKeeper(), replicaId));
    status.setStatus("META assigned.");
  }

  private void enableCrashedServerProcessing(final boolean waitForMeta)
      throws InterruptedException {
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
}
