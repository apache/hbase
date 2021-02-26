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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Store Region State to hbase:meta table.
 */
@InterfaceAudience.Private
public class RegionStateStore {
  private static final Logger LOG = LoggerFactory.getLogger(RegionStateStore.class);
  private static final Logger METALOG = LoggerFactory.getLogger("org.apache.hadoop.hbase.META");

  /** The delimiter for meta columns for replicaIds &gt; 0 */
  protected static final char META_REPLICA_ID_DELIMITER = '_';

  private final MasterServices master;

  public RegionStateStore(final MasterServices master) {
    this.master = master;
  }

  public interface RegionStateVisitor {
    void visitRegionState(Result result, RegionInfo regionInfo, State state,
      ServerName regionLocation, ServerName lastHost, long openSeqNum);
  }

  public void visitMeta(final RegionStateVisitor visitor) throws IOException {
    MetaTableAccessor.fullScanRegions(master.getConnection(), new MetaTableAccessor.Visitor() {
      final boolean isDebugEnabled = LOG.isDebugEnabled();

      @Override
      public boolean visit(final Result r) throws IOException {
        if (r !=  null && !r.isEmpty()) {
          long st = 0;
          if (LOG.isTraceEnabled()) {
            st = System.currentTimeMillis();
          }
          visitMetaEntry(visitor, r);
          if (LOG.isTraceEnabled()) {
            long et = System.currentTimeMillis();
            LOG.trace("[T] LOAD META PERF " + StringUtils.humanTimeDiff(et - st));
          }
        } else if (isDebugEnabled) {
          LOG.debug("NULL result from meta - ignoring but this is strange.");
        }
        return true;
      }
    });
  }

  /**
   * Queries META table for the passed region encoded name, delegating action upon results to the
   * <code>RegionStateVisitor</code> passed as second parameter.
   * @param regionEncodedName encoded name for the Region we want to query META for.
   * @param visitor The <code>RegionStateVisitor</code> instance to react over the query results.
   * @throws IOException If some error occurs while querying META or parsing results.
   */
  public void visitMetaForRegion(final String regionEncodedName, final RegionStateVisitor visitor)
    throws IOException {
    Result result =
      MetaTableAccessor.scanByRegionEncodedName(master.getConnection(), regionEncodedName);
    if (result != null) {
      visitMetaEntry(visitor, result);
    }
  }

  private void visitMetaEntry(final RegionStateVisitor visitor, final Result result)
      throws IOException {
    final RegionLocations rl = MetaTableAccessor.getRegionLocations(result);
    if (rl == null) return;

    final HRegionLocation[] locations = rl.getRegionLocations();
    if (locations == null) return;

    for (int i = 0; i < locations.length; ++i) {
      final HRegionLocation hrl = locations[i];
      if (hrl == null) continue;

      final RegionInfo regionInfo = hrl.getRegion();
      if (regionInfo == null) continue;

      final int replicaId = regionInfo.getReplicaId();
      final State state = getRegionState(result, regionInfo);

      final ServerName lastHost = hrl.getServerName();
      ServerName regionLocation = MetaTableAccessor.getTargetServerName(result, replicaId);
      final long openSeqNum = hrl.getSeqNum();

      LOG.debug(
        "Load hbase:meta entry region={}, regionState={}, lastHost={}, " +
          "regionLocation={}, openSeqNum={}",
        regionInfo.getEncodedName(), state, lastHost, regionLocation, openSeqNum);
      visitor.visitRegionState(result, regionInfo, state, regionLocation, lastHost, openSeqNum);
    }
  }

  void updateRegionLocation(RegionStateNode regionStateNode) throws IOException {
    if (regionStateNode.getRegionInfo().isMetaRegion()) {
      updateMetaLocation(regionStateNode.getRegionInfo(), regionStateNode.getRegionLocation(),
        regionStateNode.getState());
    } else {
      long openSeqNum = regionStateNode.getState() == State.OPEN ? regionStateNode.getOpenSeqNum() :
        HConstants.NO_SEQNUM;
      updateUserRegionLocation(regionStateNode.getRegionInfo(), regionStateNode.getState(),
        regionStateNode.getRegionLocation(), openSeqNum,
        // The regionStateNode may have no procedure in a test scenario; allow for this.
        regionStateNode.getProcedure() != null ? regionStateNode.getProcedure().getProcId() :
          Procedure.NO_PROC_ID);
    }
  }

  private void updateMetaLocation(RegionInfo regionInfo, ServerName serverName, State state)
    throws IOException {
    try {
      MetaTableLocator.setMetaLocation(master.getZooKeeper(), serverName, regionInfo.getReplicaId(),
        state);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void updateUserRegionLocation(RegionInfo regionInfo, State state,
    ServerName regionLocation, long openSeqNum, long pid) throws IOException {
    long time = EnvironmentEdgeManager.currentTime();
    final int replicaId = regionInfo.getReplicaId();
    final Put put = new Put(MetaTableAccessor.getMetaKeyForRegion(regionInfo), time);
    MetaTableAccessor.addRegionInfo(put, regionInfo);
    final StringBuilder info =
      new StringBuilder("pid=").append(pid).append(" updating hbase:meta row=")
        .append(regionInfo.getEncodedName()).append(", regionState=").append(state);
    if (openSeqNum >= 0) {
      Preconditions.checkArgument(state == State.OPEN && regionLocation != null,
        "Open region should be on a server");
      MetaTableAccessor.addLocation(put, regionLocation, openSeqNum, replicaId);
      // only update replication barrier for default replica
      if (regionInfo.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID &&
        hasGlobalReplicationScope(regionInfo.getTable())) {
        MetaTableAccessor.addReplicationBarrier(put, openSeqNum);
        info.append(", repBarrier=").append(openSeqNum);
      }
      info.append(", openSeqNum=").append(openSeqNum);
      info.append(", regionLocation=").append(regionLocation);
    } else if (regionLocation != null) {
      // Ideally, if no regionLocation, write null to the hbase:meta but this will confuse clients
      // currently; they want a server to hit. TODO: Make clients wait if no location.
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(MetaTableAccessor.getServerNameColumn(replicaId))
        .setTimestamp(put.getTimestamp()).setType(Cell.Type.Put)
        .setValue(Bytes.toBytes(regionLocation.getServerName())).build());
      info.append(", regionLocation=").append(regionLocation);
    }
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
      .setFamily(HConstants.CATALOG_FAMILY).setQualifier(getStateColumn(replicaId))
      .setTimestamp(put.getTimestamp()).setType(Cell.Type.Put).setValue(Bytes.toBytes(state.name()))
      .build());
    LOG.info(info.toString());
    updateRegionLocation(regionInfo, state, put);
  }

  private void updateRegionLocation(RegionInfo regionInfo, State state, Put put)
      throws IOException {
    try (Table table = getMetaTable()) {
      debugLogMutation(put);
      table.put(put);
    } catch (IOException e) {
      // TODO: Revist!!!! Means that if a server is loaded, then we will abort our host!
      // In tests we abort the Master!
      String msg = String.format("FAILED persisting region=%s state=%s",
        regionInfo.getShortNameToLog(), state);
      LOG.error(msg, e);
      master.abort(msg, e);
      throw e;
    }
  }

  private long getOpenSeqNumForParentRegion(RegionInfo region) throws IOException {
    MasterFileSystem fs = master.getMasterFileSystem();
    long maxSeqId = WALSplitUtil.getMaxRegionSequenceId(master.getConfiguration(), region,
      fs::getFileSystem, fs::getWALFileSystem);
    return maxSeqId > 0 ? maxSeqId + 1 : HConstants.NO_SEQNUM;
  }

  private Table getMetaTable() throws IOException {
    return master.getConnection().getTable(TableName.META_TABLE_NAME);
  }

  // ============================================================================================
  // Update Region Splitting State helpers
  // ============================================================================================
  public void splitRegion(RegionInfo parent, RegionInfo hriA, RegionInfo hriB,
      ServerName serverName) throws IOException {
    TableDescriptor htd = getTableDescriptor(parent.getTable());
    long parentOpenSeqNum = HConstants.NO_SEQNUM;
    if (htd.hasGlobalReplicationScope()) {
      parentOpenSeqNum = getOpenSeqNumForParentRegion(parent);
    }
    MetaTableAccessor.splitRegion(master.getConnection(), parent, parentOpenSeqNum, hriA, hriB,
      serverName, getRegionReplication(htd));
  }

  // ============================================================================================
  // Update Region Merging State helpers
  // ============================================================================================
  public void mergeRegions(RegionInfo child, RegionInfo [] parents, ServerName serverName)
      throws IOException {
    TableDescriptor htd = getTableDescriptor(child.getTable());
    boolean globalScope = htd.hasGlobalReplicationScope();
    SortedMap<RegionInfo, Long> parentSeqNums = new TreeMap<>();
    for (RegionInfo ri: parents) {
      parentSeqNums.put(ri, globalScope? getOpenSeqNumForParentRegion(ri): -1);
    }
    MetaTableAccessor.mergeRegions(master.getConnection(), child, parentSeqNums,
        serverName, getRegionReplication(htd));
  }

  // ============================================================================================
  // Delete Region State helpers
  // ============================================================================================
  public void deleteRegion(final RegionInfo regionInfo) throws IOException {
    deleteRegions(Collections.singletonList(regionInfo));
  }

  public void deleteRegions(final List<RegionInfo> regions) throws IOException {
    MetaTableAccessor.deleteRegionInfos(master.getConnection(), regions);
  }

  private Scan getScanForUpdateRegionReplicas(TableName tableName) {
    return MetaTableAccessor.getScanForTableName(master.getConfiguration(), tableName)
      .addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
  }

  public void removeRegionReplicas(TableName tableName, int oldReplicaCount, int newReplicaCount)
    throws IOException {
    if (TableName.isMetaTableName(tableName)) {
      ZKWatcher zk = master.getZooKeeper();
      try {
        for (int i = newReplicaCount; i < oldReplicaCount; i++) {
          ZKUtil.deleteNode(zk, zk.getZNodePaths().getZNodeForReplica(i));
        }
      } catch (KeeperException e) {
        throw new IOException(e);
      }
    } else {
      Scan scan = getScanForUpdateRegionReplicas(tableName);
      List<Delete> deletes = new ArrayList<>();
      long now = EnvironmentEdgeManager.currentTime();
      try (Table metaTable = getMetaTable(); ResultScanner scanner = metaTable.getScanner(scan)) {
        for (;;) {
          Result result = scanner.next();
          if (result == null) {
            break;
          }
          RegionInfo primaryRegionInfo = MetaTableAccessor.getRegionInfo(result);
          if (primaryRegionInfo == null || primaryRegionInfo.isSplitParent()) {
            continue;
          }
          Delete delete = new Delete(result.getRow());
          for (int i = newReplicaCount; i < oldReplicaCount; i++) {
            delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(i),
              now);
            delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getSeqNumColumn(i),
              now);
            delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getStartCodeColumn(i),
              now);
            delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerNameColumn(i),
              now);
            delete.addColumns(HConstants.CATALOG_FAMILY,
              MetaTableAccessor.getRegionStateColumn(i), now);
          }
          deletes.add(delete);
        }
        debugLogMutations(deletes);
        metaTable.delete(deletes);
      }
    }
  }

  // ==========================================================================
  // Table Descriptors helpers
  // ==========================================================================
  private boolean hasGlobalReplicationScope(TableName tableName) throws IOException {
    return hasGlobalReplicationScope(getTableDescriptor(tableName));
  }

  private boolean hasGlobalReplicationScope(TableDescriptor htd) {
    return htd != null ? htd.hasGlobalReplicationScope() : false;
  }

  private int getRegionReplication(TableDescriptor htd) {
    return htd != null ? htd.getRegionReplication() : 1;
  }

  private TableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    return master.getTableDescriptors().get(tableName);
  }

  // ==========================================================================
  // Region State
  // ==========================================================================

  /**
   * Pull the region state from a catalog table {@link Result}.
   * @return the region state, or null if unknown.
   */
  public static State getRegionState(final Result r, RegionInfo regionInfo) {
    Cell cell =
      r.getColumnLatestCell(HConstants.CATALOG_FAMILY, getStateColumn(regionInfo.getReplicaId()));
    if (cell == null || cell.getValueLength() == 0) {
      return null;
    }

    String state =
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    try {
      return State.valueOf(state);
    } catch (IllegalArgumentException e) {
      LOG.warn(
        "BAD value {} in hbase:meta info:state column for region {} , " +
          "Consider using HBCK2 setRegionState ENCODED_REGION_NAME STATE",
        state, regionInfo.getEncodedName());
      return null;
    }
  }

  private static byte[] getStateColumn(int replicaId) {
    return replicaId == 0 ? HConstants.STATE_QUALIFIER :
      Bytes.toBytes(HConstants.STATE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER +
        String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  private static void debugLogMutations(List<? extends Mutation> mutations) throws IOException {
    if (!METALOG.isDebugEnabled()) {
      return;
    }
    // Logging each mutation in separate line makes it easier to see diff between them visually
    // because of common starting indentation.
    for (Mutation mutation : mutations) {
      debugLogMutation(mutation);
    }
  }

  private static void debugLogMutation(Mutation p) throws IOException {
    METALOG.debug("{} {}", p.getClass().getSimpleName(), p.toJSON());
  }
}
