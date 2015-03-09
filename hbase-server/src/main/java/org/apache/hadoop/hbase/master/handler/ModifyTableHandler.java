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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class ModifyTableHandler extends TableEventHandler {
  private static final Log LOG = LogFactory.getLog(ModifyTableHandler.class);

  private final HTableDescriptor htd;

  public ModifyTableHandler(final TableName tableName,
      final HTableDescriptor htd, final Server server,
      final MasterServices masterServices) {
    super(EventType.C_M_MODIFY_TABLE, tableName, server, masterServices);
    // This is the new schema we are going to write out as this modification.
    this.htd = htd;
  }

  @Override
  protected void prepareWithTableLock() throws IOException {
    super.prepareWithTableLock();
    // Check operation is possible on the table in its current state
    // Also checks whether the table exists
    if (masterServices.getAssignmentManager().getTableStateManager()
        .isTableState(this.htd.getTableName(), TableState.State.ENABLED)
        && this.htd.getRegionReplication() != getTableDescriptor()
        .getHTableDescriptor().getRegionReplication()) {
      throw new IOException("REGION_REPLICATION change is not supported for enabled tables");
    }
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> hris)
  throws IOException {
    MasterCoprocessorHost cpHost = ((HMaster) this.server).getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preModifyTableHandler(this.tableName, this.htd);
    }
    // Update descriptor
    HTableDescriptor oldDescriptor =
        this.masterServices.getTableDescriptors().get(this.tableName);
    this.masterServices.getTableDescriptors().add(htd);
    deleteFamilyFromFS(hris, oldDescriptor.getFamiliesKeys());
    removeReplicaColumnsIfNeeded(
        this.htd.getRegionReplication(),
        oldDescriptor.getRegionReplication(),
        this.htd.getTableName());
    if (cpHost != null) {
      cpHost.postModifyTableHandler(this.tableName, this.htd);
    }
  }

  private void removeReplicaColumnsIfNeeded(int newReplicaCount, int oldReplicaCount,
      TableName table) throws IOException {
    if (newReplicaCount >= oldReplicaCount) return;
    Set<byte[]> tableRows = new HashSet<byte[]>();
    Connection connection = this.masterServices.getConnection();
    Scan scan = MetaTableAccessor.getScanForTableName(connection, table);
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    try (Table metaTable = connection.getTable(TableName.META_TABLE_NAME)) {
      ResultScanner resScanner = metaTable.getScanner(scan);
      for (Result result : resScanner) {
        tableRows.add(result.getRow());
      }
      MetaTableAccessor.removeRegionReplicasFromMeta(tableRows, newReplicaCount,
        oldReplicaCount - newReplicaCount, masterServices.getConnection());
    }
  }

  /**
   * Removes from hdfs the families that are not longer present in the new table descriptor.
   */
  private void deleteFamilyFromFS(final List<HRegionInfo> hris, final Set<byte[]> oldFamilies) {
    try {
      Set<byte[]> newFamilies = this.htd.getFamiliesKeys();
      MasterFileSystem mfs = this.masterServices.getMasterFileSystem();
      for (byte[] familyName: oldFamilies) {
        if (!newFamilies.contains(familyName)) {
          LOG.debug("Removing family=" + Bytes.toString(familyName) +
                    " from table=" + this.tableName);
          for (HRegionInfo hri: hris) {
            // Delete the family directory in FS for all the regions one by one
            mfs.deleteFamilyFromFS(hri, familyName);
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Unable to remove on-disk directories for the removed families", e);
    }
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" +
      tableName;
  }
}
