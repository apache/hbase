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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.replication.ReplicationBarrierFamilyFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;

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

  private final MasterRegion masterRegion;

  public RegionStateStore(MasterServices master, MasterRegion masterRegion) {
    this.master = master;
    this.masterRegion = masterRegion;
  }

  @FunctionalInterface
  public interface RegionStateVisitor {
    void visitRegionState(Result result, RegionInfo regionInfo, State state,
      ServerName regionLocation, ServerName lastHost, long openSeqNum);
  }

  public void visitMeta(final RegionStateVisitor visitor) throws IOException {
    MetaTableAccessor.fullScanRegions(master.getConnection(),
      new ClientMetaTableAccessor.Visitor() {
        final boolean isDebugEnabled = LOG.isDebugEnabled();

        @Override
        public boolean visit(final Result r) throws IOException {
          if (r != null && !r.isEmpty()) {
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
    final RegionLocations rl = CatalogFamilyFormat.getRegionLocations(result);
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
    final Put put = new Put(CatalogFamilyFormat.getMetaKeyForRegion(regionInfo), time);
    MetaTableAccessor.addRegionInfo(put, regionInfo);
    final StringBuilder info =
      new StringBuilder("pid=").append(pid).append(" updating catalog row=")
        .append(regionInfo.getRegionNameAsString()).append(", regionState=").append(state);
    if (openSeqNum >= 0) {
      Preconditions.checkArgument(state == State.OPEN && regionLocation != null,
          "Open region should be on a server");
      MetaTableAccessor.addLocation(put, regionLocation, openSeqNum, replicaId);
      // only update replication barrier for default replica
      if (regionInfo.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID &&
        hasGlobalReplicationScope(regionInfo.getTable())) {
        ReplicationBarrierFamilyFormat.addReplicationBarrier(put, openSeqNum);
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
          .setQualifier(CatalogFamilyFormat.getServerNameColumn(replicaId))
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
        masterRegion.update(r -> r.put(put));
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

  /**
   * Performs an atomic multi-mutate operation against the given table. Used by the likes of merge
   * and split as these want to make atomic mutations across multiple rows.
   */
  private void multiMutate(RegionInfo ri, List<Mutation> mutations) throws IOException {
    debugLogMutations(mutations);
    if (ri.isMetaRegion()) {
      masterRegion.update(region -> {
        List<byte[]> rowsToLock =
          mutations.stream().map(Mutation::getRow).collect(Collectors.toList());
        region.mutateRowsWithLocks(mutations, rowsToLock, HConstants.NO_NONCE, HConstants.NO_NONCE);
      });
    } else {
      byte[] row =
        Bytes.toBytes(RegionReplicaUtil.getRegionInfoForDefaultReplica(ri).getRegionNameAsString() +
          HConstants.DELIMITER);
      MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();
      for (Mutation mutation : mutations) {
        if (mutation instanceof Put) {
          builder.addMutationRequest(
            ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, mutation));
        } else if (mutation instanceof Delete) {
          builder.addMutationRequest(
            ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.DELETE, mutation));
        } else {
          throw new DoNotRetryIOException(
            "multi in MetaEditor doesn't support " + mutation.getClass().getName());
        }
      }
      MutateRowsRequest request = builder.build();
      AsyncTable<?> table =
        master.getConnection().toAsyncConnection().getTable(TableName.META_TABLE_NAME);
      CompletableFuture<MutateRowsResponse> future =
        table.<MultiRowMutationService, MutateRowsResponse> coprocessorService(
          MultiRowMutationService::newStub,
          (stub, controller, done) -> stub.mutateRows(controller, request, done), row);
      FutureUtils.get(future);
    }
  }

  private Result getRegionCatalogResult(RegionInfo region) throws IOException {
    Get get =
      new Get(CatalogFamilyFormat.getMetaKeyForRegion(region)).addFamily(HConstants.CATALOG_FAMILY);
    try (Table table = master.getConnection().getTable(TableName.META_TABLE_NAME)) {
      return table.get(get);
    }
  }


  private static Put addSequenceNum(Put p, long openSeqNum, int replicaId) throws IOException {
    return p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(p.getRow())
      .setFamily(HConstants.CATALOG_FAMILY)
      .setQualifier(CatalogFamilyFormat.getSeqNumColumn(replicaId)).setTimestamp(p.getTimestamp())
      .setType(Type.Put).setValue(Bytes.toBytes(openSeqNum)).build());
  }

  // ============================================================================================
  //  Update Region Splitting State helpers
  // ============================================================================================
  /**
   * Splits the region into two in an atomic operation. Offlines the parent region with the
   * information that it is split into two, and also adds the daughter regions. Does not add the
   * location information to the daughter regions since they are not open yet.
   */
  public void splitRegion(RegionInfo parent, RegionInfo splitA, RegionInfo splitB,
    ServerName serverName, TableDescriptor htd) throws IOException {
    long parentOpenSeqNum = HConstants.NO_SEQNUM;
    if (htd.hasGlobalReplicationScope()) {
      parentOpenSeqNum = getOpenSeqNumForParentRegion(parent);
    }
    long time = EnvironmentEdgeManager.currentTime();
    // Put for parent
    Put putParent = MetaTableAccessor.makePutFromRegionInfo(
      RegionInfoBuilder.newBuilder(parent).setOffline(true).setSplit(true).build(), time);
    MetaTableAccessor.addDaughtersToPut(putParent, splitA, splitB);

    // Puts for daughters
    Put putA = MetaTableAccessor.makePutFromRegionInfo(splitA, time);
    Put putB = MetaTableAccessor.makePutFromRegionInfo(splitB, time);
    if (parentOpenSeqNum > 0) {
      ReplicationBarrierFamilyFormat.addReplicationBarrier(putParent, parentOpenSeqNum);
      ReplicationBarrierFamilyFormat.addReplicationParent(putA, Collections.singletonList(parent));
      ReplicationBarrierFamilyFormat.addReplicationParent(putB, Collections.singletonList(parent));
    }
    // Set initial state to CLOSED
    // NOTE: If initial state is not set to CLOSED then daughter regions get added with the
    // default OFFLINE state. If Master gets restarted after this step, start up sequence of
    // master tries to assign these offline regions. This is followed by re-assignments of the
    // daughter regions from resumed {@link SplitTableRegionProcedure}
    MetaTableAccessor.addRegionStateToPut(putA, RegionState.State.CLOSED);
    MetaTableAccessor.addRegionStateToPut(putB, RegionState.State.CLOSED);

    // new regions, openSeqNum = 1 is fine.
    addSequenceNum(putA, 1, splitA.getReplicaId());
    addSequenceNum(putB, 1, splitB.getReplicaId());

    // Add empty locations for region replicas of daughters so that number of replicas can be
    // cached whenever the primary region is looked up from meta
    int regionReplication = getRegionReplication(htd);
    for (int i = 1; i < regionReplication; i++) {
      MetaTableAccessor.addEmptyLocation(putA, i);
      MetaTableAccessor.addEmptyLocation(putB, i);
    }

    multiMutate(parent, Arrays.asList(putParent, putA, putB));
  }

  // ============================================================================================
  //  Update Region Merging State helpers
  // ============================================================================================
  public void mergeRegions(RegionInfo child, RegionInfo[] parents, ServerName serverName,
    TableDescriptor htd) throws IOException {
    boolean globalScope = htd.hasGlobalReplicationScope();
    long time = HConstants.LATEST_TIMESTAMP;
    List<Mutation> mutations = new ArrayList<>();
    List<RegionInfo> replicationParents = new ArrayList<>();
    for (RegionInfo ri: parents) {
      long seqNum = globalScope ? getOpenSeqNumForParentRegion(ri) : -1;
      // Deletes for merging regions
      mutations.add(MetaTableAccessor.makeDeleteFromRegionInfo(ri, time));
      if (seqNum > 0) {
        mutations
          .add(ReplicationBarrierFamilyFormat.makePutForReplicationBarrier(ri, seqNum, time));
        replicationParents.add(ri);
      }
    }
    // Put for parent
    Put putOfMerged = MetaTableAccessor.makePutFromRegionInfo(child, time);
    putOfMerged = addMergeRegions(putOfMerged, Arrays.asList(parents));
    // Set initial state to CLOSED.
    // NOTE: If initial state is not set to CLOSED then merged region gets added with the
    // default OFFLINE state. If Master gets restarted after this step, start up sequence of
    // master tries to assign this offline region. This is followed by re-assignments of the
    // merged region from resumed {@link MergeTableRegionsProcedure}
    MetaTableAccessor.addRegionStateToPut(putOfMerged, RegionState.State.CLOSED);
    mutations.add(putOfMerged);
    // The merged is a new region, openSeqNum = 1 is fine. ServerName may be null
    // if crash after merge happened but before we got to here.. means in-memory
    // locations of offlined merged, now-closed, regions is lost. Should be ok. We
    // assign the merged region later.
    if (serverName != null) {
      MetaTableAccessor.addLocation(putOfMerged, serverName, 1, child.getReplicaId());
    }

    // Add empty locations for region replicas of the merged region so that number of replicas
    // can be cached whenever the primary region is looked up from meta
    int regionReplication = getRegionReplication(htd);
    for (int i = 1; i < regionReplication; i++) {
      MetaTableAccessor.addEmptyLocation(putOfMerged, i);
    }
    // add parent reference for serial replication
    if (!replicationParents.isEmpty()) {
      ReplicationBarrierFamilyFormat.addReplicationParent(putOfMerged, replicationParents);
    }
    multiMutate(child, mutations);
  }

  /**
   * Check whether the given {@code region} has any 'info:merge*' columns.
   */
  public boolean hasMergeRegions(RegionInfo region) throws IOException {
    return CatalogFamilyFormat.hasMergeRegions(getRegionCatalogResult(region).rawCells());
  }

  /**
   * @return Return all regioninfos listed in the 'info:merge*' columns of the given {@code region}.
   */
  public List<RegionInfo> getMergeRegions(RegionInfo region) throws IOException {
    return CatalogFamilyFormat.getMergeRegions(getRegionCatalogResult(region).rawCells());
  }

  /**
   * Deletes merge qualifiers for the specified merge region.
   * @param connection connection we're using
   * @param mergeRegion the merged region
   */
  public void deleteMergeQualifiers(RegionInfo mergeRegion)
    throws IOException {
    // NOTE: We are doing a new hbase:meta read here.
    Cell[] cells = getRegionCatalogResult(mergeRegion).rawCells();
    if (cells == null || cells.length == 0) {
      return;
    }
    Delete delete = new Delete(mergeRegion.getRegionName());
    List<byte[]> qualifiers = new ArrayList<>();
    for (Cell cell : cells) {
      if (!CatalogFamilyFormat.isMergeQualifierPrefix(cell)) {
        continue;
      }
      byte[] qualifier = CellUtil.cloneQualifier(cell);
      qualifiers.add(qualifier);
      delete.addColumns(HConstants.CATALOG_FAMILY, qualifier, HConstants.LATEST_TIMESTAMP);
    }

    // There will be race condition that a GCMultipleMergedRegionsProcedure is scheduled while
    // the previous GCMultipleMergedRegionsProcedure is still going on, in this case, the second
    // GCMultipleMergedRegionsProcedure could delete the merged region by accident!
    if (qualifiers.isEmpty()) {
      LOG.info("No merged qualifiers for region " + mergeRegion.getRegionNameAsString() +
        " in meta table, they are cleaned up already, Skip.");
      return;
    }
    try (Table table = master.getConnection().getTable(TableName.META_TABLE_NAME)) {
      table.delete(delete);
    }
    LOG.info("Deleted merge references in " + mergeRegion.getRegionNameAsString() +
      ", deleted qualifiers " +
      qualifiers.stream().map(Bytes::toStringBinary).collect(Collectors.joining(", ")));
  }

  @VisibleForTesting
  static Put addMergeRegions(Put put, Collection<RegionInfo> mergeRegions) throws IOException {
    int limit = 10000; // Arbitrary limit. No room in our formatted 'task0000' below for more.
    int max = mergeRegions.size();
    if (max > limit) {
      // Should never happen!!!!! But just in case.
      throw new RuntimeException(
        "Can't merge " + max + " regions in one go; " + limit + " is upper-limit.");
    }
    int counter = 0;
    for (RegionInfo ri : mergeRegions) {
      String qualifier = String.format(HConstants.MERGE_QUALIFIER_PREFIX_STR + "%04d", counter++);
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY).setQualifier(Bytes.toBytes(qualifier))
        .setTimestamp(put.getTimestamp()).setType(Type.Put).setValue(RegionInfo.toByteArray(ri))
        .build());
    }
    return put;
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
