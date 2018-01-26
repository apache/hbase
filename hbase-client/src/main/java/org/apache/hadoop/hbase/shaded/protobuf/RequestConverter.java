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
package org.apache.hadoop.hbase.shaded.protobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionCoprocessorServiceExec;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest.RegionUpdateInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetCleanerChoreRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.TransitReplicationPeerSyncReplicationStateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;

/**
 * Helper utility to build protocol buffer requests,
 * or build components for protocol buffer requests.
 */
@InterfaceAudience.Private
public final class RequestConverter {

  private RequestConverter() {
  }

// Start utilities for Client

  /**
   * Create a protocol buffer GetRequest for a client Get
   *
   * @param regionName the name of the region to get
   * @param get the client Get
   * @return a protocol buffer GetRequest
   */
  public static GetRequest buildGetRequest(final byte[] regionName,
      final Get get) throws IOException {
    GetRequest.Builder builder = GetRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setGet(ProtobufUtil.toGet(get));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a client increment
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param amount
   * @param durability
   * @return a mutate request
   */
  public static MutateRequest buildIncrementRequest(
      final byte[] regionName, final byte[] row, final byte[] family, final byte[] qualifier,
      final long amount, final Durability durability, long nonceGroup, long nonce) {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);

    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(UnsafeByteOperations.unsafeWrap(row));
    mutateBuilder.setMutateType(MutationType.INCREMENT);
    mutateBuilder.setDurability(ProtobufUtil.toDurability(durability));
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    columnBuilder.setFamily(UnsafeByteOperations.unsafeWrap(family));
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    valueBuilder.setValue(UnsafeByteOperations.unsafeWrap(Bytes.toBytes(amount)));
    valueBuilder.setQualifier(UnsafeByteOperations
        .unsafeWrap(qualifier == null ? HConstants.EMPTY_BYTE_ARRAY : qualifier));
    valueBuilder.setTimestamp(HConstants.LATEST_TIMESTAMP);
    columnBuilder.addQualifierValue(valueBuilder.build());
    mutateBuilder.addColumnValue(columnBuilder.build());
    if (nonce != HConstants.NO_NONCE) {
      mutateBuilder.setNonce(nonce);
    }
    builder.setMutation(mutateBuilder.build());
    if (nonceGroup != HConstants.NO_NONCE) {
      builder.setNonceGroup(nonceGroup);
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a conditioned put
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param comparator
   * @param compareType
   * @param put
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final byte[] row, final byte[] family,
      final byte [] qualifier, final ByteArrayComparable comparator,
      final CompareType compareType, TimeRange timeRange, final Put put) throws IOException {
    return buildMutateRequest(regionName, row, family, qualifier, comparator, compareType, timeRange
      , put, MutationType.PUT);
  }

  /**
   * Create a protocol buffer MutateRequest for a conditioned delete
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param comparator
   * @param compareType
   * @param delete
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final byte[] row, final byte[] family,
      final byte [] qualifier, final ByteArrayComparable comparator,
      final CompareType compareType, TimeRange timeRange, final Delete delete) throws IOException {
    return buildMutateRequest(regionName, row, family, qualifier, comparator, compareType, timeRange
      , delete, MutationType.DELETE);
  }

  public static MutateRequest buildMutateRequest(final byte[] regionName, final byte[] row,
    final byte[] family, final byte[] qualifier, final ByteArrayComparable comparator,
    final CompareType compareType, TimeRange timeRange, final Mutation mutation,
    final MutationType type) throws IOException {
    return MutateRequest.newBuilder()
      .setRegion(buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName))
      .setMutation(ProtobufUtil.toMutation(type, mutation))
      .setCondition(buildCondition(row, family, qualifier, comparator, compareType, timeRange))
      .build();
  }
  /**
   * Create a protocol buffer MutateRequest for conditioned row mutations
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param comparator
   * @param compareType
   * @param rowMutations
   * @return a mutate request
   * @throws IOException
   */
  public static ClientProtos.MultiRequest buildMutateRequest(final byte[] regionName,
    final byte[] row, final byte[] family, final byte[] qualifier,
    final ByteArrayComparable comparator, final CompareType compareType, final TimeRange timeRange,
    final RowMutations rowMutations) throws IOException {
    RegionAction.Builder builder =
        getRegionActionBuilderWithRegion(RegionAction.newBuilder(), regionName);
    builder.setAtomic(true);
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    MutationProto.Builder mutationBuilder = MutationProto.newBuilder();
    for (Mutation mutation: rowMutations.getMutations()) {
      MutationType mutateType = null;
      if (mutation instanceof Put) {
        mutateType = MutationType.PUT;
      } else if (mutation instanceof Delete) {
        mutateType = MutationType.DELETE;
      } else {
        throw new DoNotRetryIOException("RowMutations supports only put and delete, not " +
            mutation.getClass().getName());
      }
      mutationBuilder.clear();
      MutationProto mp = ProtobufUtil.toMutation(mutateType, mutation, mutationBuilder);
      actionBuilder.clear();
      actionBuilder.setMutation(mp);
      builder.addAction(actionBuilder.build());
    }
    return ClientProtos.MultiRequest.newBuilder().addRegionAction(builder.build())
        .setCondition(buildCondition(row, family, qualifier, comparator, compareType, timeRange))
        .build();
  }

  /**
   * Create a protocol buffer MutateRequest for a put
   *
   * @param regionName
   * @param put
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final Put put) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutation(ProtobufUtil.toMutation(MutationType.PUT, put, MutationProto.newBuilder()));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for an append
   *
   * @param regionName
   * @param append
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(final byte[] regionName,
      final Append append, long nonceGroup, long nonce) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    if (nonce != HConstants.NO_NONCE && nonceGroup != HConstants.NO_NONCE) {
      builder.setNonceGroup(nonceGroup);
    }
    builder.setMutation(ProtobufUtil.toMutation(MutationType.APPEND, append,
      MutationProto.newBuilder(), nonce));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a client increment
   *
   * @param regionName
   * @param increment
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(final byte[] regionName,
      final Increment increment, final long nonceGroup, final long nonce) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    if (nonce != HConstants.NO_NONCE && nonceGroup != HConstants.NO_NONCE) {
      builder.setNonceGroup(nonceGroup);
    }
    builder.setMutation(ProtobufUtil.toMutation(MutationType.INCREMENT, increment,
            MutationProto.newBuilder(), nonce));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a delete
   *
   * @param regionName
   * @param delete
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final Delete delete) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutation(ProtobufUtil.toMutation(MutationType.DELETE, delete,
      MutationProto.newBuilder()));
    return builder.build();
  }

  /**
   * Create a protocol buffer MultiRequest for row mutations.
   * Does not propagate Action absolute position.  Does not set atomic action on the created
   * RegionAtomic.  Caller should do that if wanted.
   * @param regionName
   * @param rowMutations
   * @return a data-laden RegionMutation.Builder
   * @throws IOException
   */
  public static RegionAction.Builder buildRegionAction(final byte [] regionName,
      final RowMutations rowMutations)
  throws IOException {
    RegionAction.Builder builder =
      getRegionActionBuilderWithRegion(RegionAction.newBuilder(), regionName);
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    MutationProto.Builder mutationBuilder = MutationProto.newBuilder();
    for (Mutation mutation: rowMutations.getMutations()) {
      MutationType mutateType = null;
      if (mutation instanceof Put) {
        mutateType = MutationType.PUT;
      } else if (mutation instanceof Delete) {
        mutateType = MutationType.DELETE;
      } else {
        throw new DoNotRetryIOException("RowMutations supports only put and delete, not " +
          mutation.getClass().getName());
      }
      mutationBuilder.clear();
      MutationProto mp = ProtobufUtil.toMutation(mutateType, mutation, mutationBuilder);
      actionBuilder.clear();
      actionBuilder.setMutation(mp);
      builder.addAction(actionBuilder.build());
    }
    return builder;
  }

  /**
   * Create a protocol buffer MultiRequest for row mutations that does not hold data.  Data/Cells
   * are carried outside of protobuf.  Return references to the Cells in <code>cells</code> param.
    * Does not propagate Action absolute position.  Does not set atomic action on the created
   * RegionAtomic.  Caller should do that if wanted.
   * @param regionName
   * @param rowMutations
   * @param cells Return in here a list of Cells as CellIterable.
   * @return a region mutation minus data
   * @throws IOException
   */
  public static RegionAction.Builder buildNoDataRegionAction(final byte[] regionName,
      final RowMutations rowMutations, final List<CellScannable> cells,
      final RegionAction.Builder regionActionBuilder,
      final ClientProtos.Action.Builder actionBuilder,
      final MutationProto.Builder mutationBuilder)
  throws IOException {
    for (Mutation mutation: rowMutations.getMutations()) {
      MutationType type = null;
      if (mutation instanceof Put) {
        type = MutationType.PUT;
      } else if (mutation instanceof Delete) {
        type = MutationType.DELETE;
      } else {
        throw new DoNotRetryIOException("RowMutations supports only put and delete, not " +
          mutation.getClass().getName());
      }
      mutationBuilder.clear();
      MutationProto mp = ProtobufUtil.toMutationNoData(type, mutation, mutationBuilder);
      cells.add(mutation);
      actionBuilder.clear();
      regionActionBuilder.addAction(actionBuilder.setMutation(mp).build());
    }
    return regionActionBuilder;
  }

  public static RegionAction.Builder getRegionActionBuilderWithRegion(
      final RegionAction.Builder regionActionBuilder, final byte [] regionName) {
    RegionSpecifier region = buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName);
    regionActionBuilder.setRegion(region);
    return regionActionBuilder;
  }

  /**
   * Create a protocol buffer ScanRequest for a client Scan
   *
   * @param regionName
   * @param scan
   * @param numberOfRows
   * @param closeScanner
   * @return a scan request
   * @throws IOException
   */
  public static ScanRequest buildScanRequest(byte[] regionName, Scan scan, int numberOfRows,
      boolean closeScanner) throws IOException {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName);
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setRegion(region);
    builder.setScan(ProtobufUtil.toScan(scan));
    builder.setClientHandlesPartials(true);
    builder.setClientHandlesHeartbeats(true);
    builder.setTrackScanMetrics(scan.isScanMetricsEnabled());
    if (scan.getLimit() > 0) {
      builder.setLimitOfRows(scan.getLimit());
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer ScanRequest for a scanner id
   * @param scannerId
   * @param numberOfRows
   * @param closeScanner
   * @return a scan request
   */
  public static ScanRequest buildScanRequest(long scannerId, int numberOfRows, boolean closeScanner,
      boolean trackMetrics) {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setScannerId(scannerId);
    builder.setClientHandlesPartials(true);
    builder.setClientHandlesHeartbeats(true);
    builder.setTrackScanMetrics(trackMetrics);
    return builder.build();
  }

  /**
   * Create a protocol buffer ScanRequest for a scanner id
   * @param scannerId
   * @param numberOfRows
   * @param closeScanner
   * @param nextCallSeq
   * @return a scan request
   */
  public static ScanRequest buildScanRequest(long scannerId, int numberOfRows, boolean closeScanner,
      long nextCallSeq, boolean trackMetrics, boolean renew, int limitOfRows) {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setScannerId(scannerId);
    builder.setNextCallSeq(nextCallSeq);
    builder.setClientHandlesPartials(true);
    builder.setClientHandlesHeartbeats(true);
    builder.setTrackScanMetrics(trackMetrics);
    builder.setRenew(renew);
    if (limitOfRows > 0) {
      builder.setLimitOfRows(limitOfRows);
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer bulk load request
   *
   * @param familyPaths
   * @param regionName
   * @param assignSeqNum
   * @param userToken
   * @param bulkToken
   * @return a bulk load request
   */
  public static BulkLoadHFileRequest buildBulkLoadHFileRequest(
      final List<Pair<byte[], String>> familyPaths,
      final byte[] regionName, boolean assignSeqNum,
      final Token<?> userToken, final String bulkToken) {
    return buildBulkLoadHFileRequest(familyPaths, regionName, assignSeqNum, userToken, bulkToken,
        false);
  }

  /**
   * Create a protocol buffer bulk load request
   *
   * @param familyPaths
   * @param regionName
   * @param assignSeqNum
   * @param userToken
   * @param bulkToken
   * @param copyFiles
   * @return a bulk load request
   */
  public static BulkLoadHFileRequest buildBulkLoadHFileRequest(
      final List<Pair<byte[], String>> familyPaths,
      final byte[] regionName, boolean assignSeqNum,
      final Token<?> userToken, final String bulkToken, boolean copyFiles) {
    RegionSpecifier region = RequestConverter.buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);

    ClientProtos.DelegationToken protoDT = null;
    if (userToken != null) {
      protoDT =
          ClientProtos.DelegationToken.newBuilder()
            .setIdentifier(UnsafeByteOperations.unsafeWrap(userToken.getIdentifier()))
            .setPassword(UnsafeByteOperations.unsafeWrap(userToken.getPassword()))
            .setKind(userToken.getKind().toString())
            .setService(userToken.getService().toString()).build();
    }

    List<ClientProtos.BulkLoadHFileRequest.FamilyPath> protoFamilyPaths = new ArrayList<>(familyPaths.size());
    if (!familyPaths.isEmpty()) {
      ClientProtos.BulkLoadHFileRequest.FamilyPath.Builder pathBuilder
        = ClientProtos.BulkLoadHFileRequest.FamilyPath.newBuilder();
      for(Pair<byte[], String> el: familyPaths) {
        protoFamilyPaths.add(pathBuilder
          .setFamily(UnsafeByteOperations.unsafeWrap(el.getFirst()))
          .setPath(el.getSecond()).build());
      }
      pathBuilder.clear();
    }

    BulkLoadHFileRequest.Builder request =
        ClientProtos.BulkLoadHFileRequest.newBuilder()
          .setRegion(region)
          .setAssignSeqNum(assignSeqNum)
          .addAllFamilyPath(protoFamilyPaths);
    if (userToken != null) {
      request.setFsToken(protoDT);
    }
    if (bulkToken != null) {
      request.setBulkToken(bulkToken);
    }
    request.setCopyFile(copyFiles);
    return request.build();
  }

  /**
   * Create a protocol buffer multi request for a list of actions. Propagates Actions original
   * index. The passed in multiRequestBuilder will be populated with region actions.
   * @param regionName The region name of the actions.
   * @param actions The actions that are grouped by the same region name.
   * @param multiRequestBuilder The multiRequestBuilder to be populated with region actions.
   * @param regionActionBuilder regionActionBuilder to be used to build region action.
   * @param actionBuilder actionBuilder to be used to build action.
   * @param mutationBuilder mutationBuilder to be used to build mutation.
   * @param nonceGroup nonceGroup to be applied.
   * @param rowMutationsIndexMap Map of created RegionAction to the original index for a
   *          RowMutations within the original list of actions
   * @throws IOException
   */
  public static void buildRegionActions(final byte[] regionName,
      final List<Action> actions, final MultiRequest.Builder multiRequestBuilder,
      final RegionAction.Builder regionActionBuilder,
      final ClientProtos.Action.Builder actionBuilder,
      final MutationProto.Builder mutationBuilder,
      long nonceGroup, final Map<Integer, Integer> rowMutationsIndexMap) throws IOException {
    regionActionBuilder.clear();
    RegionAction.Builder builder = getRegionActionBuilderWithRegion(
      regionActionBuilder, regionName);
    ClientProtos.CoprocessorServiceCall.Builder cpBuilder = null;
    boolean hasNonce = false;
    List<Action> rowMutationsList = new ArrayList<>();

    for (Action action: actions) {
      Row row = action.getAction();
      actionBuilder.clear();
      actionBuilder.setIndex(action.getOriginalIndex());
      mutationBuilder.clear();
      if (row instanceof Get) {
        Get g = (Get)row;
        builder.addAction(actionBuilder.setGet(ProtobufUtil.toGet(g)));
      } else if (row instanceof Put) {
        builder.addAction(actionBuilder.
          setMutation(ProtobufUtil.toMutation(MutationType.PUT, (Put)row, mutationBuilder)));
      } else if (row instanceof Delete) {
        builder.addAction(actionBuilder.
          setMutation(ProtobufUtil.toMutation(MutationType.DELETE, (Delete)row, mutationBuilder)));
      } else if (row instanceof Append) {
        builder.addAction(actionBuilder.setMutation(ProtobufUtil.toMutation(
            MutationType.APPEND, (Append)row, mutationBuilder, action.getNonce())));
        hasNonce = true;
      } else if (row instanceof Increment) {
        builder.addAction(actionBuilder.setMutation(ProtobufUtil.toMutation(
            MutationType.INCREMENT, (Increment)row, mutationBuilder, action.getNonce())));
        hasNonce = true;
      } else if (row instanceof RegionCoprocessorServiceExec) {
        RegionCoprocessorServiceExec exec = (RegionCoprocessorServiceExec) row;
        // DUMB COPY!!! FIX!!! Done to copy from c.g.p.ByteString to shaded ByteString.
        org.apache.hbase.thirdparty.com.google.protobuf.ByteString value =
         org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations.unsafeWrap(
             exec.getRequest().toByteArray());
        if (cpBuilder == null) {
          cpBuilder = ClientProtos.CoprocessorServiceCall.newBuilder();
        } else {
          cpBuilder.clear();
        }
        builder.addAction(actionBuilder.setServiceCall(
            cpBuilder.setRow(UnsafeByteOperations.unsafeWrap(exec.getRow()))
              .setServiceName(exec.getMethod().getService().getFullName())
              .setMethodName(exec.getMethod().getName())
              .setRequest(value)));
      } else if (row instanceof RowMutations) {
        rowMutationsList.add(action);
      } else {
        throw new DoNotRetryIOException("Multi doesn't support " + row.getClass().getName());
      }
    }
    if (!multiRequestBuilder.hasNonceGroup() && hasNonce) {
      multiRequestBuilder.setNonceGroup(nonceGroup);
    }
    multiRequestBuilder.addRegionAction(builder.build());

    // Process RowMutations here. We can not process it in the big loop above because
    // it will corrupt the sequence order maintained in cells.
    // RowMutations is a set of Puts and/or Deletes all to be applied atomically
    // on the one row. We do separate RegionAction for each RowMutations.
    // We maintain a map to keep track of this RegionAction and the original Action index.
    for (Action action : rowMutationsList) {
      RowMutations rms = (RowMutations) action.getAction();
      RegionAction.Builder rowMutationsRegionActionBuilder =
          RequestConverter.buildRegionAction(regionName, rms);
      rowMutationsRegionActionBuilder.setAtomic(true);
      // Put it in the multiRequestBuilder
      multiRequestBuilder.addRegionAction(rowMutationsRegionActionBuilder.build());
      // This rowMutations region action is at (multiRequestBuilder.getRegionActionCount() - 1)
      // in the overall multiRequest.
      rowMutationsIndexMap.put(multiRequestBuilder.getRegionActionCount() - 1,
        action.getOriginalIndex());
    }
  }

  /**
   * Create a protocol buffer multirequest with NO data for a list of actions (data is carried
   * otherwise than via protobuf).  This means it just notes attributes, whether to write the
   * WAL, etc., and the presence in protobuf serves as place holder for the data which is
   * coming along otherwise.  Note that Get is different.  It does not contain 'data' and is always
   * carried by protobuf.  We return references to the data by adding them to the passed in
   * <code>data</code> param.
   * <p> Propagates Actions original index.
   * <p> The passed in multiRequestBuilder will be populated with region actions.
   * @param regionName The region name of the actions.
   * @param actions The actions that are grouped by the same region name.
   * @param cells Place to stuff references to actual data.
   * @param multiRequestBuilder The multiRequestBuilder to be populated with region actions.
   * @param regionActionBuilder regionActionBuilder to be used to build region action.
   * @param actionBuilder actionBuilder to be used to build action.
   * @param mutationBuilder mutationBuilder to be used to build mutation.
   * @param nonceGroup nonceGroup to be applied.
   * @param rowMutationsIndexMap Map of created RegionAction to the original index for a
   *          RowMutations within the original list of actions
   * @throws IOException
   */
  public static void buildNoDataRegionActions(final byte[] regionName,
      final Iterable<Action> actions, final List<CellScannable> cells,
      final MultiRequest.Builder multiRequestBuilder,
      final RegionAction.Builder regionActionBuilder,
      final ClientProtos.Action.Builder actionBuilder,
      final MutationProto.Builder mutationBuilder,
      long nonceGroup, final Map<Integer, Integer> rowMutationsIndexMap) throws IOException {
    regionActionBuilder.clear();
    RegionAction.Builder builder = getRegionActionBuilderWithRegion(
      regionActionBuilder, regionName);
    ClientProtos.CoprocessorServiceCall.Builder cpBuilder = null;
    RegionAction.Builder rowMutationsRegionActionBuilder = null;
    boolean hasNonce = false;
    List<Action> rowMutationsList = new ArrayList<>();

    for (Action action: actions) {
      Row row = action.getAction();
      actionBuilder.clear();
      actionBuilder.setIndex(action.getOriginalIndex());
      mutationBuilder.clear();
      if (row instanceof Get) {
        Get g = (Get)row;
        builder.addAction(actionBuilder.setGet(ProtobufUtil.toGet(g)));
      } else if (row instanceof Put) {
        Put p = (Put)row;
        cells.add(p);
        builder.addAction(actionBuilder.
          setMutation(ProtobufUtil.toMutationNoData(MutationType.PUT, p, mutationBuilder)));
      } else if (row instanceof Delete) {
        Delete d = (Delete)row;
        int size = d.size();
        // Note that a legitimate Delete may have a size of zero; i.e. a Delete that has nothing
        // in it but the row to delete.  In this case, the current implementation does not make
        // a KeyValue to represent a delete-of-all-the-row until we serialize... For such cases
        // where the size returned is zero, we will send the Delete fully pb'd rather than have
        // metadata only in the pb and then send the kv along the side in cells.
        if (size > 0) {
          cells.add(d);
          builder.addAction(actionBuilder.
            setMutation(ProtobufUtil.toMutationNoData(MutationType.DELETE, d, mutationBuilder)));
        } else {
          builder.addAction(actionBuilder.
            setMutation(ProtobufUtil.toMutation(MutationType.DELETE, d, mutationBuilder)));
        }
      } else if (row instanceof Append) {
        Append a = (Append)row;
        cells.add(a);
        builder.addAction(actionBuilder.setMutation(ProtobufUtil.toMutationNoData(
          MutationType.APPEND, a, mutationBuilder, action.getNonce())));
        hasNonce = true;
      } else if (row instanceof Increment) {
        Increment i = (Increment)row;
        cells.add(i);
        builder.addAction(actionBuilder.setMutation(ProtobufUtil.toMutationNoData(
          MutationType.INCREMENT, i, mutationBuilder, action.getNonce())));
        hasNonce = true;
      } else if (row instanceof RegionCoprocessorServiceExec) {
        RegionCoprocessorServiceExec exec = (RegionCoprocessorServiceExec) row;
        // DUMB COPY!!! FIX!!! Done to copy from c.g.p.ByteString to shaded ByteString.
        org.apache.hbase.thirdparty.com.google.protobuf.ByteString value =
         org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations.unsafeWrap(
             exec.getRequest().toByteArray());
        if (cpBuilder == null) {
          cpBuilder = ClientProtos.CoprocessorServiceCall.newBuilder();
        } else {
          cpBuilder.clear();
        }
        builder.addAction(actionBuilder.setServiceCall(
            cpBuilder.setRow(UnsafeByteOperations.unsafeWrap(exec.getRow()))
              .setServiceName(exec.getMethod().getService().getFullName())
              .setMethodName(exec.getMethod().getName())
              .setRequest(value)));
      } else if (row instanceof RowMutations) {
        rowMutationsList.add(action);
      } else {
        throw new DoNotRetryIOException("Multi doesn't support " + row.getClass().getName());
      }
    }
    if (!multiRequestBuilder.hasNonceGroup() && hasNonce) {
      multiRequestBuilder.setNonceGroup(nonceGroup);
    }
    multiRequestBuilder.addRegionAction(builder.build());

    // Process RowMutations here. We can not process it in the big loop above because
    // it will corrupt the sequence order maintained in cells.
    // RowMutations is a set of Puts and/or Deletes all to be applied atomically
    // on the one row. We do separate RegionAction for each RowMutations.
    // We maintain a map to keep track of this RegionAction and the original Action index.
    for (Action action : rowMutationsList) {
      RowMutations rms = (RowMutations) action.getAction();
      if (rowMutationsRegionActionBuilder == null) {
        rowMutationsRegionActionBuilder = ClientProtos.RegionAction.newBuilder();
      } else {
        rowMutationsRegionActionBuilder.clear();
      }
      rowMutationsRegionActionBuilder.setRegion(
        RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName));
      rowMutationsRegionActionBuilder = RequestConverter.buildNoDataRegionAction(regionName, rms,
        cells, rowMutationsRegionActionBuilder, actionBuilder, mutationBuilder);
      rowMutationsRegionActionBuilder.setAtomic(true);
      // Put it in the multiRequestBuilder
      multiRequestBuilder.addRegionAction(rowMutationsRegionActionBuilder.build());
      // This rowMutations region action is at (multiRequestBuilder.getRegionActionCount() - 1)
      // in the overall multiRequest.
      rowMutationsIndexMap.put(multiRequestBuilder.getRegionActionCount() - 1,
        action.getOriginalIndex());
    }
  }

// End utilities for Client
//Start utilities for Admin

  /**
   * Create a protocol buffer GetRegionInfoRequest for a given region name
   *
   * @param regionName the name of the region to get info
   * @return a protocol buffer GetRegionInfoRequest
   */
  public static GetRegionInfoRequest
      buildGetRegionInfoRequest(final byte[] regionName) {
    return buildGetRegionInfoRequest(regionName, false);
  }

  /**
   * Create a protocol buffer GetRegionInfoRequest for a given region name
   *
   * @param regionName the name of the region to get info
   * @param includeCompactionState indicate if the compaction state is requested
   * @return a protocol buffer GetRegionInfoRequest
   */
  public static GetRegionInfoRequest
      buildGetRegionInfoRequest(final byte[] regionName,
        final boolean includeCompactionState) {
    return buildGetRegionInfoRequest(regionName, includeCompactionState, false);
  }

  /**
    *
    * @param regionName the name of the region to get info
    * @param includeCompactionState indicate if the compaction state is requested
    * @param includeBestSplitRow indicate if the bestSplitRow  is requested
   * @return protocol buffer GetRegionInfoRequest
   */
  public static GetRegionInfoRequest buildGetRegionInfoRequest(final byte[] regionName,
      final boolean includeCompactionState, boolean includeBestSplitRow) {
    GetRegionInfoRequest.Builder builder = GetRegionInfoRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    if (includeCompactionState) {
      builder.setCompactionState(includeCompactionState);
    }
    if (includeBestSplitRow) {
      builder.setBestSplitRow(includeBestSplitRow);
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer GetRegionLoadRequest for all regions/regions of a table.
   * @param tableName the table for which regionLoad should be obtained from RS
   * @return a protocol buffer GetRegionLoadRequest
   */
  public static GetRegionLoadRequest buildGetRegionLoadRequest(final TableName tableName) {
    GetRegionLoadRequest.Builder builder = GetRegionLoadRequest.newBuilder();
    if (tableName != null) {
      builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer GetOnlineRegionRequest
   * @return a protocol buffer GetOnlineRegionRequest
   */
  public static GetOnlineRegionRequest buildGetOnlineRegionRequest() {
    return GetOnlineRegionRequest.newBuilder().build();
  }

  /**
   * Create a protocol buffer FlushRegionRequest for a given region name
   * @param regionName the name of the region to get info
   * @return a protocol buffer FlushRegionRequest
   */
  public static FlushRegionRequest buildFlushRegionRequest(final byte[] regionName) {
    return buildFlushRegionRequest(regionName, false);
  }

  /**
   * Create a protocol buffer FlushRegionRequest for a given region name
   * @param regionName the name of the region to get info
   * @return a protocol buffer FlushRegionRequest
   */
  public static FlushRegionRequest buildFlushRegionRequest(final byte[] regionName,
      boolean writeFlushWALMarker) {
    FlushRegionRequest.Builder builder = FlushRegionRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setWriteFlushWalMarker(writeFlushWALMarker);
    return builder.build();
  }

  /**
   * Create a protocol buffer OpenRegionRequest to open a list of regions
   * @param server the serverName for the RPC
   * @param regionOpenInfos info of a list of regions to open
   * @return a protocol buffer OpenRegionRequest
   */
  public static OpenRegionRequest buildOpenRegionRequest(ServerName server,
      final List<Pair<RegionInfo, List<ServerName>>> regionOpenInfos) {
    OpenRegionRequest.Builder builder = OpenRegionRequest.newBuilder();
    for (Pair<RegionInfo, List<ServerName>> regionOpenInfo : regionOpenInfos) {
      builder.addOpenInfo(buildRegionOpenInfo(regionOpenInfo.getFirst(),
        regionOpenInfo.getSecond()));
    }
    if (server != null) {
      builder.setServerStartCode(server.getStartcode());
    }
    // send the master's wall clock time as well, so that the RS can refer to it
    builder.setMasterSystemTime(EnvironmentEdgeManager.currentTime());
    return builder.build();
  }

  /**
   * Create a protocol buffer OpenRegionRequest for a given region
   * @param server the serverName for the RPC
   * @param region the region to open
   * @param favoredNodes a list of favored nodes
   * @return a protocol buffer OpenRegionRequest
   */
  public static OpenRegionRequest buildOpenRegionRequest(ServerName server,
      final RegionInfo region, List<ServerName> favoredNodes) {
    OpenRegionRequest.Builder builder = OpenRegionRequest.newBuilder();
    builder.addOpenInfo(buildRegionOpenInfo(region, favoredNodes));
    if (server != null) {
      builder.setServerStartCode(server.getStartcode());
    }
    builder.setMasterSystemTime(EnvironmentEdgeManager.currentTime());
    return builder.build();
  }

  /**
   * Create a protocol buffer UpdateFavoredNodesRequest to update a list of favorednode mappings
   * @param updateRegionInfos a list of favored node mappings
   * @return a protocol buffer UpdateFavoredNodesRequest
   */
  public static UpdateFavoredNodesRequest buildUpdateFavoredNodesRequest(
      final List<Pair<RegionInfo, List<ServerName>>> updateRegionInfos) {
    UpdateFavoredNodesRequest.Builder ubuilder = UpdateFavoredNodesRequest.newBuilder();
    if (updateRegionInfos != null && !updateRegionInfos.isEmpty()) {
      RegionUpdateInfo.Builder builder = RegionUpdateInfo.newBuilder();
      for (Pair<RegionInfo, List<ServerName>> pair : updateRegionInfos) {
        builder.setRegion(ProtobufUtil.toRegionInfo(pair.getFirst()));
        for (ServerName server : pair.getSecond()) {
          builder.addFavoredNodes(ProtobufUtil.toServerName(server));
        }
        ubuilder.addUpdateInfo(builder.build());
        builder.clear();
      }
    }
    return ubuilder.build();
  }

  /**
   * Create a WarmupRegionRequest for a given region name
   * @param regionInfo Region we are warming up
   */
  public static WarmupRegionRequest buildWarmupRegionRequest(final RegionInfo regionInfo) {
    WarmupRegionRequest.Builder builder = WarmupRegionRequest.newBuilder();
    builder.setRegionInfo(ProtobufUtil.toRegionInfo(regionInfo));
    return builder.build();
  }

  /**
   * Create a CompactRegionRequest for a given region name
   * @param regionName the name of the region to get info
   * @param major indicator if it is a major compaction
   * @param columnFamily
   * @return a CompactRegionRequest
   */
  public static CompactRegionRequest buildCompactRegionRequest(byte[] regionName, boolean major,
      byte[] columnFamily) {
    CompactRegionRequest.Builder builder = CompactRegionRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMajor(major);
    if (columnFamily != null) {
      builder.setFamily(UnsafeByteOperations.unsafeWrap(columnFamily));
    }
    return builder.build();
  }

  /**
   * @see #buildRollWALWriterRequest()
   */
  private static RollWALWriterRequest ROLL_WAL_WRITER_REQUEST = RollWALWriterRequest.newBuilder()
      .build();

  /**
   * Create a new RollWALWriterRequest
   * @return a ReplicateWALEntryRequest
   */
  public static RollWALWriterRequest buildRollWALWriterRequest() {
    return ROLL_WAL_WRITER_REQUEST;
  }

  /**
   * @see #buildGetServerInfoRequest()
   */
  private static GetServerInfoRequest GET_SERVER_INFO_REQUEST = GetServerInfoRequest.newBuilder()
      .build();

  /**
   * Create a new GetServerInfoRequest
   * @return a GetServerInfoRequest
   */
  public static GetServerInfoRequest buildGetServerInfoRequest() {
    return GET_SERVER_INFO_REQUEST;
  }

  /**
   * Create a new StopServerRequest
   * @param reason the reason to stop the server
   * @return a StopServerRequest
   */
  public static StopServerRequest buildStopServerRequest(final String reason) {
    StopServerRequest.Builder builder = StopServerRequest.newBuilder();
    builder.setReason(reason);
    return builder.build();
  }

//End utilities for Admin

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier
   *
   * @param type the region specifier type
   * @param value the region specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static RegionSpecifier buildRegionSpecifier(
      final RegionSpecifierType type, final byte[] value) {
    RegionSpecifier.Builder regionBuilder = RegionSpecifier.newBuilder();
    regionBuilder.setValue(UnsafeByteOperations.unsafeWrap(value));
    regionBuilder.setType(type);
    return regionBuilder.build();
  }

  /**
   * Create a protocol buffer Condition
   *
   * @param row
   * @param family
   * @param qualifier
   * @param comparator
   * @param compareType
   * @return a Condition
   * @throws IOException
   */
  public static Condition buildCondition(final byte[] row, final byte[] family,
    final byte[] qualifier, final ByteArrayComparable comparator, final CompareType compareType,
    final TimeRange timeRange) {
    return Condition.newBuilder().setRow(UnsafeByteOperations.unsafeWrap(row))
      .setFamily(UnsafeByteOperations.unsafeWrap(family))
      .setQualifier(UnsafeByteOperations.unsafeWrap(qualifier == null ?
        HConstants.EMPTY_BYTE_ARRAY : qualifier))
      .setComparator(ProtobufUtil.toComparator(comparator))
      .setCompareType(compareType)
      .setTimeRange(ProtobufUtil.toTimeRange(timeRange))
      .build();
  }

  /**
   * Create a protocol buffer AddColumnRequest
   *
   * @param tableName
   * @param column
   * @return an AddColumnRequest
   */
  public static AddColumnRequest buildAddColumnRequest(
      final TableName tableName,
      final ColumnFamilyDescriptor column,
      final long nonceGroup,
      final long nonce) {
    AddColumnRequest.Builder builder = AddColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setColumnFamilies(ProtobufUtil.toColumnFamilySchema(column));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Create a protocol buffer DeleteColumnRequest
   *
   * @param tableName
   * @param columnName
   * @return a DeleteColumnRequest
   */
  public static DeleteColumnRequest buildDeleteColumnRequest(
      final TableName tableName,
      final byte [] columnName,
      final long nonceGroup,
      final long nonce) {
    DeleteColumnRequest.Builder builder = DeleteColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    builder.setColumnName(UnsafeByteOperations.unsafeWrap(columnName));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Create a protocol buffer ModifyColumnRequest
   *
   * @param tableName
   * @param column
   * @return an ModifyColumnRequest
   */
  public static ModifyColumnRequest buildModifyColumnRequest(
      final TableName tableName,
      final ColumnFamilyDescriptor column,
      final long nonceGroup,
      final long nonce) {
    ModifyColumnRequest.Builder builder = ModifyColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    builder.setColumnFamilies(ProtobufUtil.toColumnFamilySchema(column));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Create a protocol buffer MoveRegionRequest
   * @param encodedRegionName
   * @param destServerName
   * @return A MoveRegionRequest
   */
  public static MoveRegionRequest buildMoveRegionRequest(byte[] encodedRegionName,
      ServerName destServerName) {
    MoveRegionRequest.Builder builder = MoveRegionRequest.newBuilder();
    builder.setRegion(buildRegionSpecifier(RegionSpecifierType.ENCODED_REGION_NAME,
      encodedRegionName));
    if (destServerName != null) {
      builder.setDestServerName(ProtobufUtil.toServerName(destServerName));
    }
    return builder.build();
  }

  public static MergeTableRegionsRequest buildMergeTableRegionsRequest(
      final byte[][] encodedNameOfdaughaterRegions,
      final boolean forcible,
      final long nonceGroup,
      final long nonce) throws DeserializationException {
    MergeTableRegionsRequest.Builder builder = MergeTableRegionsRequest.newBuilder();
    for (int i = 0; i< encodedNameOfdaughaterRegions.length; i++) {
      builder.addRegion(buildRegionSpecifier(
        RegionSpecifierType.ENCODED_REGION_NAME, encodedNameOfdaughaterRegions[i]));
    }
    builder.setForcible(forcible);
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  public static SplitTableRegionRequest buildSplitTableRegionRequest(final RegionInfo regionInfo,
      final byte[] splitRow, final long nonceGroup, final long nonce)
      throws DeserializationException {
    SplitTableRegionRequest.Builder builder = SplitTableRegionRequest.newBuilder();
    builder.setRegionInfo(ProtobufUtil.toRegionInfo(regionInfo));
    if (splitRow != null) {
      builder.setSplitRow(UnsafeByteOperations.unsafeWrap(splitRow));
    }
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Create a protocol buffer AssignRegionRequest
   *
   * @param regionName
   * @return an AssignRegionRequest
   */
  public static AssignRegionRequest buildAssignRegionRequest(final byte [] regionName) {
    AssignRegionRequest.Builder builder = AssignRegionRequest.newBuilder();
    builder.setRegion(buildRegionSpecifier(RegionSpecifierType.REGION_NAME,regionName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer UnassignRegionRequest
   *
   * @param regionName
   * @param force
   * @return an UnassignRegionRequest
   */
  public static UnassignRegionRequest buildUnassignRegionRequest(
      final byte [] regionName, final boolean force) {
    UnassignRegionRequest.Builder builder = UnassignRegionRequest.newBuilder();
    builder.setRegion(buildRegionSpecifier(RegionSpecifierType.REGION_NAME,regionName));
    builder.setForce(force);
    return builder.build();
  }

  /**
   * Creates a protocol buffer OfflineRegionRequest
   *
   * @param regionName
   * @return an OfflineRegionRequest
   */
  public static OfflineRegionRequest buildOfflineRegionRequest(final byte [] regionName) {
    OfflineRegionRequest.Builder builder = OfflineRegionRequest.newBuilder();
    builder.setRegion(buildRegionSpecifier(RegionSpecifierType.REGION_NAME,regionName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DeleteTableRequest
   *
   * @param tableName
   * @return a DeleteTableRequest
   */
  public static DeleteTableRequest buildDeleteTableRequest(
      final TableName tableName,
      final long nonceGroup,
      final long nonce) {
    DeleteTableRequest.Builder builder = DeleteTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Creates a protocol buffer TruncateTableRequest
   *
   * @param tableName name of table to truncate
   * @param preserveSplits True if the splits should be preserved
   * @return a TruncateTableRequest
   */
  public static TruncateTableRequest buildTruncateTableRequest(
      final TableName tableName,
      final boolean preserveSplits,
      final long nonceGroup,
      final long nonce) {
    TruncateTableRequest.Builder builder = TruncateTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setPreserveSplits(preserveSplits);
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Creates a protocol buffer EnableTableRequest
   *
   * @param tableName
   * @return an EnableTableRequest
   */
  public static EnableTableRequest buildEnableTableRequest(
      final TableName tableName,
      final long nonceGroup,
      final long nonce) {
    EnableTableRequest.Builder builder = EnableTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Creates a protocol buffer DisableTableRequest
   *
   * @param tableName
   * @return a DisableTableRequest
   */
  public static DisableTableRequest buildDisableTableRequest(
      final TableName tableName,
      final long nonceGroup,
      final long nonce) {
    DisableTableRequest.Builder builder = DisableTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Creates a protocol buffer CreateTableRequest
   *
   * @param tableDescriptor
   * @param splitKeys
   * @return a CreateTableRequest
   */
  public static CreateTableRequest buildCreateTableRequest(
      final TableDescriptor tableDescriptor,
      final byte [][] splitKeys,
      final long nonceGroup,
      final long nonce) {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setTableSchema(ProtobufUtil.toTableSchema(tableDescriptor));
    if (splitKeys != null) {
      for(byte[] key : splitKeys) {
        builder.addSplitKeys(UnsafeByteOperations.unsafeWrap(key));
      }
    }
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Creates a protocol buffer ModifyTableRequest
   *
   * @param tableName
   * @param tableDesc
   * @return a ModifyTableRequest
   */
  public static ModifyTableRequest buildModifyTableRequest(
      final TableName tableName,
      final TableDescriptor tableDesc,
      final long nonceGroup,
      final long nonce) {
    ModifyTableRequest.Builder builder = ModifyTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    builder.setTableSchema(ProtobufUtil.toTableSchema(tableDesc));
    builder.setNonceGroup(nonceGroup);
    builder.setNonce(nonce);
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetSchemaAlterStatusRequest
   *
   * @param tableName
   * @return a GetSchemaAlterStatusRequest
   */
  public static GetSchemaAlterStatusRequest buildGetSchemaAlterStatusRequest(
      final TableName tableName) {
    GetSchemaAlterStatusRequest.Builder builder = GetSchemaAlterStatusRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetTableDescriptorsRequest
   *
   * @param tableNames
   * @return a GetTableDescriptorsRequest
   */
  public static GetTableDescriptorsRequest buildGetTableDescriptorsRequest(
      final List<TableName> tableNames) {
    GetTableDescriptorsRequest.Builder builder = GetTableDescriptorsRequest.newBuilder();
    if (tableNames != null) {
      for (TableName tableName : tableNames) {
        builder.addTableNames(ProtobufUtil.toProtoTableName(tableName));
      }
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetTableDescriptorsRequest
   *
   * @param pattern The compiled regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return a GetTableDescriptorsRequest
   */
  public static GetTableDescriptorsRequest buildGetTableDescriptorsRequest(final Pattern pattern,
      boolean includeSysTables) {
    GetTableDescriptorsRequest.Builder builder = GetTableDescriptorsRequest.newBuilder();
    if (pattern != null) {
      builder.setRegex(pattern.toString());
    }
    builder.setIncludeSysTables(includeSysTables);
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetTableNamesRequest
   *
   * @param pattern The compiled regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return a GetTableNamesRequest
   */
  public static GetTableNamesRequest buildGetTableNamesRequest(final Pattern pattern,
      boolean includeSysTables) {
    GetTableNamesRequest.Builder builder = GetTableNamesRequest.newBuilder();
    if (pattern != null) {
      builder.setRegex(pattern.toString());
    }
    builder.setIncludeSysTables(includeSysTables);
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetTableStateRequest
   *
   * @param tableName table to get request for
   * @return a GetTableStateRequest
   */
  public static GetTableStateRequest buildGetTableStateRequest(
          final TableName tableName) {
    return GetTableStateRequest.newBuilder()
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .build();
  }

  /**
   * Creates a protocol buffer GetTableDescriptorsRequest for a single table
   *
   * @param tableName the table name
   * @return a GetTableDescriptorsRequest
   */
  public static GetTableDescriptorsRequest buildGetTableDescriptorsRequest(
      final TableName tableName) {
    return GetTableDescriptorsRequest.newBuilder()
      .addTableNames(ProtobufUtil.toProtoTableName(tableName))
      .build();
  }

  /**
   * Creates a protocol buffer IsMasterRunningRequest
   *
   * @return a IsMasterRunningRequest
   */
  public static IsMasterRunningRequest buildIsMasterRunningRequest() {
    return IsMasterRunningRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer BalanceRequest
   *
   * @return a BalanceRequest
   */
  public static BalanceRequest buildBalanceRequest(boolean force) {
    return BalanceRequest.newBuilder().setForce(force).build();
  }

  /**
   * Creates a protocol buffer SetBalancerRunningRequest
   *
   * @param on
   * @param synchronous
   * @return a SetBalancerRunningRequest
   */
  public static SetBalancerRunningRequest buildSetBalancerRunningRequest(
      boolean on,
      boolean synchronous) {
    return SetBalancerRunningRequest.newBuilder().setOn(on).setSynchronous(synchronous).build();
  }

  /**
   * Creates a protocol buffer IsBalancerEnabledRequest
   *
   * @return a IsBalancerEnabledRequest
   */
  public static IsBalancerEnabledRequest buildIsBalancerEnabledRequest() {
    return IsBalancerEnabledRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer ClearRegionBlockCacheRequest
   *
   * @return a ClearRegionBlockCacheRequest
   */
  public static ClearRegionBlockCacheRequest
      buildClearRegionBlockCacheRequest(List<RegionInfo> hris) {
    ClearRegionBlockCacheRequest.Builder builder = ClearRegionBlockCacheRequest.newBuilder();
    hris.forEach(
      hri -> builder.addRegion(
        buildRegionSpecifier(RegionSpecifierType.REGION_NAME, hri.getRegionName())
      ));
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetClusterStatusRequest
   *
   * @return A GetClusterStatusRequest
   */
  public static GetClusterStatusRequest buildGetClusterStatusRequest(EnumSet<Option> options) {
    return GetClusterStatusRequest.newBuilder()
                                  .addAllOptions(ClusterMetricsBuilder.toOptions(options))
                                  .build();
  }

  /**
   * @see #buildCatalogScanRequest
   */
  private static final RunCatalogScanRequest CATALOG_SCAN_REQUEST =
    RunCatalogScanRequest.newBuilder().build();

  /**
   * Creates a request for running a catalog scan
   * @return A {@link RunCatalogScanRequest}
   */
  public static RunCatalogScanRequest buildCatalogScanRequest() {
    return CATALOG_SCAN_REQUEST;
  }

  /**
   * Creates a request for enabling/disabling the catalog janitor
   * @return A {@link EnableCatalogJanitorRequest}
   */
  public static EnableCatalogJanitorRequest buildEnableCatalogJanitorRequest(boolean enable) {
    return EnableCatalogJanitorRequest.newBuilder().setEnable(enable).build();
  }

  /**
   * @see #buildIsCatalogJanitorEnabledRequest()
   */
  private static final IsCatalogJanitorEnabledRequest IS_CATALOG_JANITOR_ENABLED_REQUEST =
    IsCatalogJanitorEnabledRequest.newBuilder().build();

  /**
   * Creates a request for querying the master whether the catalog janitor is enabled
   * @return A {@link IsCatalogJanitorEnabledRequest}
   */
  public static IsCatalogJanitorEnabledRequest buildIsCatalogJanitorEnabledRequest() {
    return IS_CATALOG_JANITOR_ENABLED_REQUEST;
  }

  /**
   * @see #buildRunCleanerChoreRequest()
   */
  private static final RunCleanerChoreRequest CLEANER_CHORE_REQUEST =
    RunCleanerChoreRequest.newBuilder().build();

  /**
   * Creates a request for running cleaner chore
   * @return A {@link RunCleanerChoreRequest}
   */
  public static RunCleanerChoreRequest buildRunCleanerChoreRequest() {
    return CLEANER_CHORE_REQUEST;
  }

  /**
   * Creates a request for enabling/disabling the cleaner chore
   * @return A {@link SetCleanerChoreRunningRequest}
   */
  public static SetCleanerChoreRunningRequest buildSetCleanerChoreRunningRequest(boolean on) {
    return SetCleanerChoreRunningRequest.newBuilder().setOn(on).build();
  }

  /**
   * @see #buildIsCleanerChoreEnabledRequest()
   */
  private static final IsCleanerChoreEnabledRequest IS_CLEANER_CHORE_ENABLED_REQUEST =
    IsCleanerChoreEnabledRequest.newBuilder().build();

  /**
   * Creates a request for querying the master whether the cleaner chore is enabled
   * @return A {@link IsCleanerChoreEnabledRequest}
   */
  public static IsCleanerChoreEnabledRequest buildIsCleanerChoreEnabledRequest() {
    return IS_CLEANER_CHORE_ENABLED_REQUEST;
  }

  /**
   * Creates a request for querying the master the last flushed sequence Id for a region
   * @param regionName
   * @return A {@link GetLastFlushedSequenceIdRequest}
   */
  public static GetLastFlushedSequenceIdRequest buildGetLastFlushedSequenceIdRequest(
      byte[] regionName) {
    return GetLastFlushedSequenceIdRequest.newBuilder().setRegionName(
        UnsafeByteOperations.unsafeWrap(regionName)).build();
  }

  /**
   * Create a RegionOpenInfo based on given region info and version of offline node
   */
  public static RegionOpenInfo buildRegionOpenInfo(
      final RegionInfo region, final List<ServerName> favoredNodes) {
    RegionOpenInfo.Builder builder = RegionOpenInfo.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    if (favoredNodes != null) {
      for (ServerName server : favoredNodes) {
        builder.addFavoredNodes(ProtobufUtil.toServerName(server));
      }
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer NormalizeRequest
   *
   * @return a NormalizeRequest
   */
  public static NormalizeRequest buildNormalizeRequest() {
    return NormalizeRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer IsNormalizerEnabledRequest
   *
   * @return a IsNormalizerEnabledRequest
   */
  public static IsNormalizerEnabledRequest buildIsNormalizerEnabledRequest() {
    return IsNormalizerEnabledRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer SetNormalizerRunningRequest
   *
   * @param on
   * @return a SetNormalizerRunningRequest
   */
  public static SetNormalizerRunningRequest buildSetNormalizerRunningRequest(boolean on) {
    return SetNormalizerRunningRequest.newBuilder().setOn(on).build();
  }

  /**
   * Creates a protocol buffer IsSplitOrMergeEnabledRequest
   *
   * @param switchType see {@link org.apache.hadoop.hbase.client.MasterSwitchType}
   * @return a IsSplitOrMergeEnabledRequest
   */
  public static IsSplitOrMergeEnabledRequest buildIsSplitOrMergeEnabledRequest(
    MasterSwitchType switchType) {
    IsSplitOrMergeEnabledRequest.Builder builder = IsSplitOrMergeEnabledRequest.newBuilder();
    builder.setSwitchType(convert(switchType));
    return builder.build();
  }

  /**
   * Creates a protocol buffer SetSplitOrMergeEnabledRequest
   *
   * @param enabled switch is enabled or not
   * @param synchronous set switch sync?
   * @param switchTypes see {@link org.apache.hadoop.hbase.client.MasterSwitchType}, it is
   *                    a list.
   * @return a SetSplitOrMergeEnabledRequest
   */
  public static SetSplitOrMergeEnabledRequest buildSetSplitOrMergeEnabledRequest(boolean enabled,
    boolean synchronous, MasterSwitchType... switchTypes) {
    SetSplitOrMergeEnabledRequest.Builder builder = SetSplitOrMergeEnabledRequest.newBuilder();
    builder.setEnabled(enabled);
    builder.setSynchronous(synchronous);
    for (MasterSwitchType switchType : switchTypes) {
      builder.addSwitchTypes(convert(switchType));
    }
    return builder.build();
  }

  private static MasterProtos.MasterSwitchType convert(MasterSwitchType switchType) {
    switch (switchType) {
      case SPLIT:
        return MasterProtos.MasterSwitchType.SPLIT;
      case MERGE:
        return MasterProtos.MasterSwitchType.MERGE;
      default:
        break;
    }
    throw new UnsupportedOperationException("Unsupport switch type:" + switchType);
  }

  public static ReplicationProtos.AddReplicationPeerRequest buildAddReplicationPeerRequest(
      String peerId, ReplicationPeerConfig peerConfig, boolean enabled) {
    AddReplicationPeerRequest.Builder builder = AddReplicationPeerRequest.newBuilder();
    builder.setPeerId(peerId);
    builder.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    ReplicationProtos.ReplicationState.Builder stateBuilder =
        ReplicationProtos.ReplicationState.newBuilder();
    stateBuilder.setState(enabled ? ReplicationProtos.ReplicationState.State.ENABLED
        : ReplicationProtos.ReplicationState.State.DISABLED);
    builder.setPeerState(stateBuilder.build());
    return builder.build();
  }

  public static ReplicationProtos.RemoveReplicationPeerRequest buildRemoveReplicationPeerRequest(
      String peerId) {
    RemoveReplicationPeerRequest.Builder builder = RemoveReplicationPeerRequest.newBuilder();
    builder.setPeerId(peerId);
    return builder.build();
  }

  public static ReplicationProtos.EnableReplicationPeerRequest buildEnableReplicationPeerRequest(
      String peerId) {
    EnableReplicationPeerRequest.Builder builder = EnableReplicationPeerRequest.newBuilder();
    builder.setPeerId(peerId);
    return builder.build();
  }

  public static ReplicationProtos.DisableReplicationPeerRequest buildDisableReplicationPeerRequest(
      String peerId) {
    DisableReplicationPeerRequest.Builder builder = DisableReplicationPeerRequest.newBuilder();
    builder.setPeerId(peerId);
    return builder.build();
  }

  public static GetReplicationPeerConfigRequest buildGetReplicationPeerConfigRequest(
      String peerId) {
    GetReplicationPeerConfigRequest.Builder builder = GetReplicationPeerConfigRequest.newBuilder();
    builder.setPeerId(peerId);
    return builder.build();
  }

  public static UpdateReplicationPeerConfigRequest buildUpdateReplicationPeerConfigRequest(
      String peerId, ReplicationPeerConfig peerConfig) {
    UpdateReplicationPeerConfigRequest.Builder builder = UpdateReplicationPeerConfigRequest
        .newBuilder();
    builder.setPeerId(peerId);
    builder.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    return builder.build();
  }

  public static ListReplicationPeersRequest buildListReplicationPeersRequest(Pattern pattern) {
    ListReplicationPeersRequest.Builder builder = ListReplicationPeersRequest.newBuilder();
    if (pattern != null) {
      builder.setRegex(pattern.toString());
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer CreateNamespaceRequest
   * @param descriptor
   * @return a CreateNamespaceRequest
   */
  public static CreateNamespaceRequest buildCreateNamespaceRequest(
      final NamespaceDescriptor descriptor) {
    CreateNamespaceRequest.Builder builder = CreateNamespaceRequest.newBuilder();
    builder.setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(descriptor));
    return builder.build();
  }

  /**
   * Creates a protocol buffer ModifyNamespaceRequest
   * @param descriptor
   * @return a ModifyNamespaceRequest
   */
  public static ModifyNamespaceRequest buildModifyNamespaceRequest(
      final NamespaceDescriptor descriptor) {
    ModifyNamespaceRequest.Builder builder = ModifyNamespaceRequest.newBuilder();
    builder.setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(descriptor));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DeleteNamespaceRequest
   * @param name
   * @return a DeleteNamespaceRequest
   */
  public static DeleteNamespaceRequest buildDeleteNamespaceRequest(final String name) {
    DeleteNamespaceRequest.Builder builder = DeleteNamespaceRequest.newBuilder();
    builder.setNamespaceName(name);
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetNamespaceDescriptorRequest
   * @param name
   * @return a GetNamespaceDescriptorRequest
   */
  public static GetNamespaceDescriptorRequest buildGetNamespaceDescriptorRequest(final String name) {
    GetNamespaceDescriptorRequest.Builder builder = GetNamespaceDescriptorRequest.newBuilder();
    builder.setNamespaceName(name);
    return builder.build();
  }

  public static ClearCompactionQueuesRequest buildClearCompactionQueuesRequest(Set<String> queues) {
    ClearCompactionQueuesRequest.Builder builder = ClearCompactionQueuesRequest.newBuilder();
    for(String name: queues) {
      builder.addQueueName(name);
    }
    return builder.build();
  }

  public static ClearDeadServersRequest buildClearDeadServersRequest(
      Collection<ServerName> deadServers) {
    ClearDeadServersRequest.Builder builder = ClearDeadServersRequest.newBuilder();
    for(ServerName server: deadServers) {
      builder.addServerName(ProtobufUtil.toServerName(server));
    }
    return builder.build();
  }

  private static final GetSpaceQuotaRegionSizesRequest GET_SPACE_QUOTA_REGION_SIZES_REQUEST =
      GetSpaceQuotaRegionSizesRequest.newBuilder().build();

  /**
   * Returns a {@link GetSpaceQuotaRegionSizesRequest} object.
   */
  public static GetSpaceQuotaRegionSizesRequest buildGetSpaceQuotaRegionSizesRequest() {
    return GET_SPACE_QUOTA_REGION_SIZES_REQUEST;
  }

  private static final GetSpaceQuotaSnapshotsRequest GET_SPACE_QUOTA_SNAPSHOTS_REQUEST =
      GetSpaceQuotaSnapshotsRequest.newBuilder().build();

  /**
   * Returns a {@link GetSpaceQuotaSnapshotsRequest} object.
   */
  public static GetSpaceQuotaSnapshotsRequest buildGetSpaceQuotaSnapshotsRequest() {
    return GET_SPACE_QUOTA_SNAPSHOTS_REQUEST;
  }

  private static final GetQuotaStatesRequest GET_QUOTA_STATES_REQUEST =
      GetQuotaStatesRequest.newBuilder().build();

  /**
   * Returns a {@link GetQuotaStatesRequest} object.
   */
  public static GetQuotaStatesRequest buildGetQuotaStatesRequest() {
    return GET_QUOTA_STATES_REQUEST;
  }

  public static DecommissionRegionServersRequest
      buildDecommissionRegionServersRequest(List<ServerName> servers, boolean offload) {
    return DecommissionRegionServersRequest.newBuilder()
        .addAllServerName(toProtoServerNames(servers)).setOffload(offload).build();
  }

  public static RecommissionRegionServerRequest
      buildRecommissionRegionServerRequest(ServerName server, List<byte[]> encodedRegionNames) {
    RecommissionRegionServerRequest.Builder builder = RecommissionRegionServerRequest.newBuilder();
    if (encodedRegionNames != null) {
      for (byte[] name : encodedRegionNames) {
        builder.addRegion(buildRegionSpecifier(RegionSpecifierType.ENCODED_REGION_NAME, name));
      }
    }
    return builder.setServerName(ProtobufUtil.toServerName(server)).build();
  }

  private static List<HBaseProtos.ServerName> toProtoServerNames(List<ServerName> servers) {
    List<HBaseProtos.ServerName> pbServers = new ArrayList<>(servers.size());
    for (ServerName server : servers) {
      pbServers.add(ProtobufUtil.toServerName(server));
    }
    return pbServers;
  }

  public static TransitReplicationPeerSyncReplicationStateRequest
      buildTransitReplicationPeerSyncReplicationStateRequest(String peerId,
          SyncReplicationState state) {
    return TransitReplicationPeerSyncReplicationStateRequest.newBuilder().setPeerId(peerId)
        .setSyncReplicationState(ReplicationPeerConfigUtil.toSyncReplicationState(state)).build();
  }
}
