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
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;

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
      throws IOException, CoordinatedStateException {
    MasterCoprocessorHost cpHost = ((HMaster) this.server).getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preTruncateTableHandler(this.tableName);
    }

    // 1. Wait because of region in transition
    waitRegionInTransition(regions);

    // 2. Remove table from hbase:meta and HDFS
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
    CreateTableHandler.checkAndSetEnablingTable(assignmentManager, tableName);
    try {
      // 1. Create Table Descriptor
      Path tempTableDir = FSUtils.getTableDir(tempdir, this.tableName);
      new FSTableDescriptors(server.getConfiguration())
        .createTableDescriptorForTableDirectory(tempTableDir, this.hTableDescriptor, false);
      Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), this.tableName);

      HRegionInfo[] newRegions;
      if (this.preserveSplits) {
        newRegions = regions.toArray(new HRegionInfo[regions.size()]);
        LOG.info("Truncate will preserve " + newRegions.length + " regions");
      } else {
        newRegions = new HRegionInfo[1];
        newRegions[0] = new HRegionInfo(this.tableName, null, null);
        LOG.info("Truncate will not preserve the regions");
      }

      // 2. Create Regions
      List<HRegionInfo> regionInfos = ModifyRegionUtils.createRegions(
        masterServices.getConfiguration(), tempdir,
        this.hTableDescriptor, newRegions, null);

      // 3. Move Table temp directory to the hbase root location
      if (!fs.rename(tempTableDir, tableDir)) {
        throw new IOException("Unable to move table from temp=" + tempTableDir +
          " to hbase root=" + tableDir);
      }

      // 4. Add regions to META
      MetaTableAccessor.addRegionsToMeta(masterServices.getConnection(), regionInfos);

      // 5. Trigger immediate assignment of the regions in round-robin fashion
      ModifyRegionUtils.assignRegions(assignmentManager, regionInfos);

      // 6. Set table enabled flag up in zk.
      try {
        assignmentManager.getTableStateManager().setTableState(tableName,
          ZooKeeperProtos.Table.State.ENABLED);
      } catch (CoordinatedStateException e) {
        throw new IOException("Unable to ensure that " + tableName + " will be" +
          " enabled because of a ZooKeeper issue", e);
      }
    } catch (IOException e) {
      CreateTableHandler.removeEnablingTable(assignmentManager, tableName);
      throw e;
    }
  }
}
