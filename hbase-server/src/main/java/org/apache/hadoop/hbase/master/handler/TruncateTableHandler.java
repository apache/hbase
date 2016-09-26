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
import java.io.InterruptedIOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.zookeeper.KeeperException;

/**
 * Truncate the table by removing META and the HDFS files and recreating it.
 * If the 'preserveSplits' option is set to true, the region splits are preserved on recreate.
 *
 * If the operation fails in the middle it may require hbck to fix the system state.
 */
@InterfaceAudience.Private
public class TruncateTableHandler extends DeleteTableHandler {
  private static final Log LOG = LogFactory.getLog(TruncateTableHandler.class);

  private final boolean preserveSplits;

  public TruncateTableHandler(final TableName tableName, final Server server,
      final MasterServices masterServices, boolean preserveSplits) {
    super(tableName, server, masterServices);
    this.preserveSplits = preserveSplits;
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> regions)
      throws IOException, KeeperException {
    MasterCoprocessorHost cpHost = ((HMaster) this.server).getCoprocessorHost();
    if (cpHost != null) {
      cpHost.preTruncateTableHandler(this.tableName);
    }

    // 1. Wait because of region in transition
    waitRegionInTransition(regions);

    // 2. Remove table from .META. and HDFS
    removeTableData(regions);

    // -----------------------------------------------------------------------
    // PONR: At this point the table is deleted.
    //       If the recreate fails, the user can only re-create the table.
    // -----------------------------------------------------------------------

    // 3. Recreate the regions
    recreateTable(regions);

    if (cpHost != null) {
      cpHost.postTruncateTableHandler(this.tableName);
    }
  }

  private void recreateTable(final List<HRegionInfo> regions) throws IOException {
    MasterFileSystem mfs = this.masterServices.getMasterFileSystem();
    Path tempdir = mfs.getTempDir();
    FileSystem fs = mfs.getFileSystem();

    AssignmentManager assignmentManager = this.masterServices.getAssignmentManager();

    // 1. Set table znode
    checkAndSetEnablingTable(assignmentManager, tableName);
    try {
      // 1. Create Table Descriptor
      Path tempTableDir = FSUtils.getTableDir(tempdir, this.tableName);
      new FSTableDescriptors(server.getConfiguration())
        .createTableDescriptorForTableDirectory(tempTableDir, getTableDescriptor(), false);
      Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), this.tableName);

      HRegionInfo[] newRegions;
      if (this.preserveSplits) {
        newRegions = recreateRegionInfo(regions);
        LOG.info("Truncate will preserve " + newRegions.length + " regions");
      } else {
        newRegions = new HRegionInfo[1];
        newRegions[0] = new HRegionInfo(this.tableName, null, null);
        LOG.info("Truncate will not preserve the regions");
      }

      // 2. Create Regions
      List<HRegionInfo> regionInfos = ModifyRegionUtils.createRegions(
        masterServices.getConfiguration(), tempdir,
        getTableDescriptor(), newRegions, null);

      // 3. Move Table temp directory to the hbase root location
      if (!fs.rename(tempTableDir, tableDir)) {
        throw new IOException("Unable to move table from temp=" + tempTableDir +
          " to hbase root=" + tableDir);
      }

      // 4. Add regions to META
      MetaEditor.addRegionsToMeta(masterServices.getCatalogTracker(), regionInfos);

      // 5. Trigger immediate assignment of the regions in round-robin fashion
      ModifyRegionUtils.assignRegions(assignmentManager, regionInfos);

      // 6. Set table enabled flag up in zk.
      try {
        assignmentManager.getZKTable().setEnabledTable(tableName);
      } catch (KeeperException e) {
        throw new IOException("Unable to ensure that " + tableName + " will be" +
          " enabled because of a ZooKeeper issue", e);
      }
    } catch (IOException e) {
      removeEnablingTable(assignmentManager, tableName);
      throw e;
    }
  }

  void checkAndSetEnablingTable(final AssignmentManager assignmentManager, final TableName tableName)
      throws IOException {
    // If we have multiple client threads trying to create the table at the
    // same time, given the async nature of the operation, the table
    // could be in a state where hbase:meta table hasn't been updated yet in
    // the process() function.
    // Use enabling state to tell if there is already a request for the same
    // table in progress. This will introduce a new zookeeper call. Given
    // createTable isn't a frequent operation, that should be ok.
    // TODO: now that we have table locks, re-evaluate above -- table locks are not enough.
    // We could have cleared the hbase.rootdir and not zk. How can we detect this case?
    // Having to clean zk AND hdfs is awkward.
    try {
      if (!assignmentManager.getZKTable().checkAndSetEnablingTable(tableName)) {
        throw new TableExistsException(tableName);
      }
    } catch (KeeperException e) {
      throw new IOException("Unable to ensure that the table will be"
          + " enabling because of a ZooKeeper issue", e);
    }
  }

  void removeEnablingTable(final AssignmentManager assignmentManager, final TableName tableName) {
    // Try deleting the enabling node in case of error
    // If this does not happen then if the client tries to create the table
    // again with the same Active master
    // It will block the creation saying TableAlreadyExists.
    try {
      assignmentManager.getZKTable().removeEnablingTable(tableName, false);
    } catch (KeeperException e) {
      // Keeper exception should not happen here
      LOG.error("Got a keeper exception while removing the ENABLING table znode " + tableName, e);
    }
  }

  /**
   * Removes the table from .META. and archives the HDFS files.
   */
  void removeTableData(final List<HRegionInfo> regions) throws IOException, KeeperException {
    // 1. Remove regions from META
    LOG.debug("Deleting regions from META");
    MetaEditor.deleteRegions(this.server.getCatalogTracker(), regions);

    // clean region references from the server manager
    this.masterServices.getServerManager().removeRegions(regions);

    // -----------------------------------------------------------------------
    // NOTE: At this point we still have data on disk, but nothing in .META.
    // if the rename below fails, hbck will report an inconsistency.
    // -----------------------------------------------------------------------

    // 2. Move the table in /hbase/.tmp
    MasterFileSystem mfs = this.masterServices.getMasterFileSystem();
    Path tempTableDir = mfs.moveTableToTemp(tableName);

    // 3. Archive regions from FS (temp directory)
    FileSystem fs = mfs.getFileSystem();
    for (HRegionInfo hri : regions) {
      LOG.debug("Archiving region " + hri.getRegionNameAsString() + " from FS");
      HFileArchiver.archiveRegion(fs, mfs.getRootDir(), tempTableDir,
        HRegion.getRegionDir(tempTableDir, hri.getEncodedName()));
    }

    // 4. Delete table directory from FS (temp directory)
    if (!fs.delete(tempTableDir, true)) {
      LOG.error("Couldn't delete " + tempTableDir);
    }

    LOG.debug("Table '" + tableName + "' archived!");
  }

  void waitRegionInTransition(final List<HRegionInfo> regions) throws IOException {
    AssignmentManager am = this.masterServices.getAssignmentManager();
    RegionStates states = am.getRegionStates();
    long waitTime = server.getConfiguration().getLong("hbase.master.wait.on.region", 5 * 60 * 1000);
    for (HRegionInfo region : regions) {
      long done = System.currentTimeMillis() + waitTime;
      while (System.currentTimeMillis() < done) {
        if (states.isRegionInState(region, State.FAILED_OPEN)) {
          am.regionOffline(region);
        }
        if (!states.isRegionInTransition(region)) {
          break;
        }
        try {
          Thread.sleep(waitingTimeForEvents);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while sleeping");
          throw (InterruptedIOException) new InterruptedIOException().initCause(e);
        }
        LOG.debug("Waiting on region to clear regions in transition; "
            + am.getRegionStates().getRegionTransitionState(region));
      }
      if (states.isRegionInTransition(region)) {
        throw new IOException("Waited hbase.master.wait.on.region (" + waitTime
            + "ms) for region to leave region " + region.getRegionNameAsString()
            + " in transitions");
      }
    }
  }

  private static HRegionInfo[] recreateRegionInfo(final List<HRegionInfo> regions) {
    HRegionInfo[] newRegions = new HRegionInfo[regions.size()];
    int index = 0;
    for (HRegionInfo hri: regions) {
      newRegions[index++] = new HRegionInfo(hri.getTable(), hri.getStartKey(), hri.getEndKey());
    }
    return newRegions;
  }
}
