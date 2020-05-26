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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.store.LocalStore;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
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

  /** The delimiter for meta columns for replicaIds &gt; 0 */
  protected static final char META_REPLICA_ID_DELIMITER = '_';

  private final MasterServices master;

  private final LocalStore localStore;

  public RegionStateStore(MasterServices master, LocalStore localStore) {
    this.master = master;
    this.localStore = localStore;
  }

  @FunctionalInterface
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
   * Queries META table for the passed region encoded name,
   * delegating action upon results to the <code>RegionStateVisitor</code>
   * passed as second parameter.
   * @param regionEncodedName encoded name for the Region we want to query META for.
   * @param visitor The <code>RegionStateVisitor</code> instance to react over the query results.
   * @throws IOException If some error occurs while querying META or parsing results.
   */
  public void visitMetaForRegion(final String regionEncodedName, final RegionStateVisitor visitor)
      throws IOException {
    Result result = MetaTableAccessor.
      scanByRegionEncodedName(master.getConnection(), regionEncodedName);
    if (result != null) {
      visitMetaEntry(visitor, result);
    }
  }

  public static void visitMetaEntry(final RegionStateVisitor visitor, final Result result)
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

      // TODO: move under trace, now is visible for debugging
      LOG.info(
        "Load hbase:meta entry region={}, regionState={}, lastHost={}, " +
          "regionLocation={}, openSeqNum={}",
        regionInfo.getEncodedName(), state, lastHost, regionLocation, openSeqNum);
      visitor.visitRegionState(result, regionInfo, state, regionLocation, lastHost, openSeqNum);
    }
  }

  void updateRegionLocation(RegionStateNode regionStateNode) throws IOException {
    long time = EnvironmentEdgeManager.currentTime();
    long openSeqNum = regionStateNode.getState() == State.OPEN ? regionStateNode.getOpenSeqNum() :
      HConstants.NO_SEQNUM;
    RegionInfo regionInfo = regionStateNode.getRegionInfo();
    State state = regionStateNode.getState();
    ServerName regionLocation = regionStateNode.getRegionLocation();
    TransitRegionStateProcedure rit = regionStateNode.getProcedure();
    long pid = rit != null ? rit.getProcId() : Procedure.NO_PROC_ID;
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
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(put.getRow())
          .setFamily(HConstants.CATALOG_FAMILY)
          .setQualifier(MetaTableAccessor.getServerNameColumn(replicaId))
          .setTimestamp(put.getTimestamp())
          .setType(Cell.Type.Put)
          .setValue(Bytes.toBytes(regionLocation.getServerName()))
          .build());
      info.append(", regionLocation=").append(regionLocation);
    }
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(getStateColumn(replicaId))
        .setTimestamp(put.getTimestamp())
        .setType(Cell.Type.Put)
        .setValue(Bytes.toBytes(state.name()))
        .build());
    LOG.info(info.toString());
    updateRegionLocation(regionInfo, state, put);
    if (regionInfo.isMetaRegion() && regionInfo.isFirst()) {
      // mirror the meta location to zookeeper
      mirrorMetaLocation(regionInfo, regionLocation, state);
    }
  }

  public void mirrorMetaLocation(RegionInfo regionInfo, ServerName serverName, State state)
      throws IOException {
    try {
      MetaTableLocator.setMetaLocation(master.getZooKeeper(), serverName, regionInfo.getReplicaId(),
        state);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void updateRegionLocation(RegionInfo regionInfo, State state, Put put)
    throws IOException {
    try {
      if (regionInfo.isMetaRegion()) {
        localStore.update(r -> r.put(put));
      } else {
        try (Table table = master.getConnection().getTable(TableName.META_TABLE_NAME)) {
          table.put(put);
        }
      }
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

  // ============================================================================================
  //  Update Region Splitting State helpers
  // ============================================================================================
  public void splitRegion(RegionInfo parent, RegionInfo hriA, RegionInfo hriB,
      ServerName serverName) throws IOException {
    TableDescriptor htd = getDescriptor(parent.getTable());
    long parentOpenSeqNum = HConstants.NO_SEQNUM;
    if (htd.hasGlobalReplicationScope()) {
      parentOpenSeqNum = getOpenSeqNumForParentRegion(parent);
    }
    MetaTableAccessor.splitRegion(master.getConnection(), parent, parentOpenSeqNum, hriA, hriB,
      serverName, getRegionReplication(htd));
  }

  // ============================================================================================
  //  Update Region Merging State helpers
  // ============================================================================================
  public void mergeRegions(RegionInfo child, RegionInfo [] parents, ServerName serverName)
      throws IOException {
    TableDescriptor htd = getDescriptor(child.getTable());
    boolean globalScope = htd.hasGlobalReplicationScope();
    SortedMap<RegionInfo, Long> parentSeqNums = new TreeMap<>();
    for (RegionInfo ri: parents) {
      parentSeqNums.put(ri, globalScope? getOpenSeqNumForParentRegion(ri): -1);
    }
    MetaTableAccessor.mergeRegions(master.getConnection(), child, parentSeqNums,
        serverName, getRegionReplication(htd));
  }

  // ============================================================================================
  //  Delete Region State helpers
  // ============================================================================================
  public void deleteRegion(final RegionInfo regionInfo) throws IOException {
    deleteRegions(Collections.singletonList(regionInfo));
  }

  public void deleteRegions(final List<RegionInfo> regions) throws IOException {
    MetaTableAccessor.deleteRegionInfos(master.getConnection(), regions);
  }

  // ==========================================================================
  //  Table Descriptors helpers
  // ==========================================================================
  private boolean hasGlobalReplicationScope(TableName tableName) throws IOException {
    return hasGlobalReplicationScope(getDescriptor(tableName));
  }

  private boolean hasGlobalReplicationScope(TableDescriptor htd) {
    return htd != null ? htd.hasGlobalReplicationScope() : false;
  }

  private int getRegionReplication(TableDescriptor htd) {
    return htd != null ? htd.getRegionReplication() : 1;
  }

  private TableDescriptor getDescriptor(TableName tableName) throws IOException {
    return master.getTableDescriptors().get(tableName);
  }

  // ==========================================================================
  //  Region State
  // ==========================================================================

  /**
   * Pull the region state from a catalog table {@link Result}.
   * @return the region state, or null if unknown.
   */
  public static State getRegionState(final Result r, RegionInfo regionInfo) {
    Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        getStateColumn(regionInfo.getReplicaId()));
    if (cell == null || cell.getValueLength() == 0) {
      return null;
    }

    String state = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength());
    try {
      return State.valueOf(state);
    } catch (IllegalArgumentException e) {
      LOG.warn("BAD value {} in hbase:meta info:state column for region {} , " +
              "Consider using HBCK2 setRegionState ENCODED_REGION_NAME STATE",
          state, regionInfo.getEncodedName());
      return null;
    }
  }

  public static byte[] getStateColumn(int replicaId) {
    return replicaId == 0
        ? HConstants.STATE_QUALIFIER
        : Bytes.toBytes(HConstants.STATE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
          + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }
}
