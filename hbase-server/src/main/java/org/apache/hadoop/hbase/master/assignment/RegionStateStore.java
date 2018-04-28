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
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
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

  public RegionStateStore(final MasterServices master) {
    this.master = master;
  }

  public interface RegionStateVisitor {
    void visitRegionState(RegionInfo regionInfo, State state,
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
      final State state = getRegionState(result, replicaId);

      final ServerName lastHost = hrl.getServerName();
      final ServerName regionLocation = getRegionServer(result, replicaId);
      final long openSeqNum = -1;

      // TODO: move under trace, now is visible for debugging
      LOG.info("Load hbase:meta entry region={}, regionState={}, lastHost={}, " +
          "regionLocation={}", regionInfo.getEncodedName(), state, lastHost, regionLocation);
      visitor.visitRegionState(regionInfo, state, regionLocation, lastHost, openSeqNum);
    }
  }

  public void updateRegionLocation(RegionStates.RegionStateNode regionStateNode)
      throws IOException {
    if (regionStateNode.getRegionInfo().isMetaRegion()) {
      updateMetaLocation(regionStateNode.getRegionInfo(), regionStateNode.getRegionLocation());
    } else {
      long openSeqNum = regionStateNode.getState() == State.OPEN ?
          regionStateNode.getOpenSeqNum() : HConstants.NO_SEQNUM;
      updateUserRegionLocation(regionStateNode.getRegionInfo(), regionStateNode.getState(),
          regionStateNode.getRegionLocation(), regionStateNode.getLastHost(), openSeqNum,
          // The regionStateNode may have no procedure in a test scenario; allow for this.
          regionStateNode.getProcedure() != null?
              regionStateNode.getProcedure().getProcId(): Procedure.NO_PROC_ID);
    }
  }

  private void updateMetaLocation(final RegionInfo regionInfo, final ServerName serverName)
      throws IOException {
    try {
      MetaTableLocator.setMetaLocation(master.getZooKeeper(), serverName,
        regionInfo.getReplicaId(), State.OPEN);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void updateUserRegionLocation(final RegionInfo regionInfo, final State state,
      final ServerName regionLocation, final ServerName lastHost, final long openSeqNum,
      final long pid)
      throws IOException {
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
    } else if (regionLocation != null && !regionLocation.equals(lastHost)) {
      // Ideally, if no regionLocation, write null to the hbase:meta but this will confuse clients
      // currently; they want a server to hit. TODO: Make clients wait if no location.
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(put.getRow())
          .setFamily(HConstants.CATALOG_FAMILY)
          .setQualifier(getServerNameColumn(replicaId))
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
  }

  private void updateRegionLocation(RegionInfo regionInfo, State state, Put put)
      throws IOException {
    try (Table table = master.getConnection().getTable(TableName.META_TABLE_NAME)) {
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
    MasterFileSystem mfs = master.getMasterFileSystem();
    long maxSeqId =
        WALSplitter.getMaxRegionSequenceId(mfs.getFileSystem(), mfs.getRegionDir(region));
    return maxSeqId > 0 ? maxSeqId + 1 : HConstants.NO_SEQNUM;
  }

  // ============================================================================================
  //  Update Region Splitting State helpers
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
  //  Update Region Merging State helpers
  // ============================================================================================
  public void mergeRegions(RegionInfo child, RegionInfo hriA, RegionInfo hriB,
      ServerName serverName) throws IOException {
    TableDescriptor htd = getTableDescriptor(child.getTable());
    long regionAOpenSeqNum = -1L;
    long regionBOpenSeqNum = -1L;
    if (htd.hasGlobalReplicationScope()) {
      regionAOpenSeqNum = getOpenSeqNumForParentRegion(hriA);
      regionBOpenSeqNum = getOpenSeqNumForParentRegion(hriB);
    }
    MetaTableAccessor.mergeRegions(master.getConnection(), child, hriA, regionAOpenSeqNum, hriB,
      regionBOpenSeqNum, serverName, getRegionReplication(htd));
  }

  // ============================================================================================
  //  Delete Region State helpers
  // ============================================================================================
  public void deleteRegion(final RegionInfo regionInfo) throws IOException {
    deleteRegions(Collections.singletonList(regionInfo));
  }

  public void deleteRegions(final List<RegionInfo> regions) throws IOException {
    MetaTableAccessor.deleteRegions(master.getConnection(), regions);
  }

  // ==========================================================================
  //  Table Descriptors helpers
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
  //  Server Name
  // ==========================================================================

  /**
   * Returns the {@link ServerName} from catalog table {@link Result}
   * where the region is transitioning. It should be the same as
   * {@link MetaTableAccessor#getServerName(Result,int)} if the server is at OPEN state.
   * @param r Result to pull the transitioning server name from
   * @return A ServerName instance or {@link MetaTableAccessor#getServerName(Result,int)}
   * if necessary fields not found or empty.
   */
  static ServerName getRegionServer(final Result r, int replicaId) {
    final Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        getServerNameColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) {
      RegionLocations locations = MetaTableAccessor.getRegionLocations(r);
      if (locations != null) {
        HRegionLocation location = locations.getRegionLocation(replicaId);
        if (location != null) {
          return location.getServerName();
        }
      }
      return null;
    }
    return ServerName.parseServerName(Bytes.toString(cell.getValueArray(),
      cell.getValueOffset(), cell.getValueLength()));
  }

  private static byte[] getServerNameColumn(int replicaId) {
    return replicaId == 0
        ? HConstants.SERVERNAME_QUALIFIER
        : Bytes.toBytes(HConstants.SERVERNAME_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
          + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  // ==========================================================================
  //  Region State
  // ==========================================================================

  /**
   * Pull the region state from a catalog table {@link Result}.
   * @param r Result to pull the region state from
   * @return the region state, or null if unknown.
   */
  @VisibleForTesting
  public static State getRegionState(final Result r, int replicaId) {
    Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY, getStateColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) {
      return null;
    }
    return State.valueOf(Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength()));
  }

  private static byte[] getStateColumn(int replicaId) {
    return replicaId == 0
        ? HConstants.STATE_QUALIFIER
        : Bytes.toBytes(HConstants.STATE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
          + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }
}
