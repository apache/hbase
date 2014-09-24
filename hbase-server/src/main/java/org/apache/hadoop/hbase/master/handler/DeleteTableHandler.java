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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;

@InterfaceAudience.Private
public class DeleteTableHandler extends TableEventHandler {
  private static final Log LOG = LogFactory.getLog(DeleteTableHandler.class);

  public DeleteTableHandler(TableName tableName, Server server,
      final MasterServices masterServices) {
    super(EventType.C_M_DELETE_TABLE, tableName, server, masterServices);
  }

  @Override
  protected void prepareWithTableLock() throws IOException {
    // The next call fails if no such table.
    getTableDescriptor();
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> regions)
  throws IOException, KeeperException {
    MasterCoprocessorHost cpHost = ((HMaster) this.server)
        .getCoprocessorHost();
    if (cpHost != null) {
      cpHost.preDeleteTableHandler(this.tableName);
    }

    // 1. Wait because of region in transition
    AssignmentManager am = this.masterServices.getAssignmentManager();
    RegionStates states = am.getRegionStates();
    long waitTime = server.getConfiguration().
      getLong("hbase.master.wait.on.region", 5 * 60 * 1000);
    for (HRegionInfo region : regions) {
      long done = System.currentTimeMillis() + waitTime;
      while (System.currentTimeMillis() < done) {
        if (states.isRegionInState(region, State.FAILED_OPEN)) {
          am.regionOffline(region);
        }
        if (!states.isRegionInTransition(region)) break;
        Threads.sleep(waitingTimeForEvents);
        LOG.debug("Waiting on region to clear regions in transition; "
          + am.getRegionStates().getRegionTransitionState(region));
      }
      if (states.isRegionInTransition(region)) {
        throw new IOException("Waited hbase.master.wait.on.region (" +
          waitTime + "ms) for region to leave region " +
          region.getRegionNameAsString() + " in transitions");
      }
    }

    // 2. Remove regions from META
    LOG.debug("Deleting regions from META");
    MetaEditor.deleteRegions(this.server.getCatalogTracker(), regions);

    // 3. Move the table in /hbase/.tmp
    MasterFileSystem mfs = this.masterServices.getMasterFileSystem();
    Path tempTableDir = mfs.moveTableToTemp(tableName);

    try {
      // 4. Delete regions from FS (temp directory)
      FileSystem fs = mfs.getFileSystem();
      for (HRegionInfo hri: regions) {
        LOG.debug("Archiving region " + hri.getRegionNameAsString() + " from FS");
        HFileArchiver.archiveRegion(fs, mfs.getRootDir(),
            tempTableDir, new Path(tempTableDir, hri.getEncodedName()));
      }

      // 5. Delete table from FS (temp directory)
      if (!fs.delete(tempTableDir, true)) {
        LOG.error("Couldn't delete " + tempTableDir);
      }

      LOG.debug("Table '" + tableName + "' archived!");
    } finally {
      // 6. Update table descriptor cache
      LOG.debug("Removing '" + tableName + "' descriptor.");
      this.masterServices.getTableDescriptors().remove(tableName);

      // 7. Clean up regions of the table in RegionStates.
      LOG.debug("Removing '" + tableName + "' from region states.");
      states.tableDeleted(tableName);

      // 8. If entry for this table in zk, and up in AssignmentManager, remove it.
      LOG.debug("Marking '" + tableName + "' as deleted.");
      am.getZKTable().setDeletedTable(tableName);
    }

    if (cpHost != null) {
      cpHost.postDeleteTableHandler(this.tableName);
    }
  }

  @Override
  protected void releaseTableLock() {
    super.releaseTableLock();
    try {
      masterServices.getTableLockManager().tableDeleted(tableName);
    } catch (IOException ex) {
      LOG.warn("Received exception from TableLockManager.tableDeleted:", ex); //not critical
    }
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" + tableName;
  }
}
