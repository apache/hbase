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
package org.apache.hadoop.hbase.protobuf;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionCoprocessorServiceExec;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest.RegionUpdateInfo;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableStateRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ByteString;

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
   * Create a new protocol buffer GetRequest to get a row, all columns in a family.
   * If there is no such row, return the closest row before it.
   *
   * @param regionName the name of the region to get
   * @param row the row to get
   * @param family the column family to get
   * should return the immediate row before
   * @return a protocol buffer GetReuqest
   */
  public static GetRequest buildGetRowOrBeforeRequest(
      final byte[] regionName, final byte[] row, final byte[] family) {
    GetRequest.Builder builder = GetRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);

    Column.Builder columnBuilder = Column.newBuilder();
    columnBuilder.setFamily(ByteStringer.wrap(family));
    ClientProtos.Get.Builder getBuilder =
      ClientProtos.Get.newBuilder();
    getBuilder.setRow(ByteStringer.wrap(row));
    getBuilder.addColumn(columnBuilder.build());
    getBuilder.setClosestRowBefore(true);
    builder.setGet(getBuilder.build());
    return builder.build();
  }


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
    mutateBuilder.setRow(ByteStringer.wrap(row));
    mutateBuilder.setMutateType(MutationType.INCREMENT);
    mutateBuilder.setDurability(ProtobufUtil.toDurability(durability));
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    columnBuilder.setFamily(ByteStringer.wrap(family));
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    valueBuilder.setValue(ByteStringer.wrap(Bytes.toBytes(amount)));
    valueBuilder.setQualifier(ByteStringer.wrap(qualifier));
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
      final CompareType compareType, final Put put) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    Condition condition = buildCondition(
      row, family, qualifier, comparator, compareType);
    builder.setMutation(ProtobufUtil.toMutation(MutationType.PUT, put, MutationProto.newBuilder()));
    builder.setCondition(condition);
    return builder.build();
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
      final CompareType compareType, final Delete delete) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    Condition condition = buildCondition(
      row, family, qualifier, comparator, compareType);
    builder.setMutation(ProtobufUtil.toMutation(MutationType.DELETE, delete,
      MutationProto.newBuilder()));
    builder.setCondition(condition);
    return builder.build();
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
  public static ClientProtos.MultiRequest buildMutateRequest(
      final byte[] regionName, final byte[] row, final byte[] family,
      final byte [] qualifier, final ByteArrayComparable comparator,
      final CompareType compareType, final RowMutations rowMutations) throws IOException {
    RegionAction.Builder builder =
        getRegionActionBuilderWithRegion(RegionAction.newBuilder(), regionName);
    builder.setAtomic(true);
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    MutationProto.Builder mutationBuilder = MutationProto.newBuilder();
    Condition condition = buildCondition(
        row, family, qualifier, comparator, compareType);
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
    ClientProtos.MultiRequest request =
        ClientProtos.MultiRequest.newBuilder().addRegionAction(builder.build())
            .setCondition(condition).build();
    return request;
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
      final Increment increment, final long nonceGroup, final long nonce) {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    if (nonce != HConstants.NO_NONCE && nonceGroup != HConstants.NO_NONCE) {
      builder.setNonceGroup(nonceGroup);
    }
    builder.setMutation(ProtobufUtil.toMutation(increment, MutationProto.newBuilder(), nonce));
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

  private static RegionAction.Builder getRegionActionBuilderWithRegion(
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
  public static ScanRequest buildScanRequest(final byte[] regionName, final Scan scan,
      final int numberOfRows, final boolean closeScanner) throws IOException {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setRegion(region);
    builder.setScan(ProtobufUtil.toScan(scan));
    builder.setClientHandlesPartials(true);
    builder.setClientHandlesHeartbeats(true);
    builder.setTrackScanMetrics(scan != null && scan.isScanMetricsEnabled());
    return builder.build();
  }

  /**
   * Create a protocol buffer ScanRequest for a scanner id
   *
   * @param scannerId
   * @param numberOfRows
   * @param closeScanner
   * @return a scan request
   */
  public static ScanRequest buildScanRequest(final long scannerId, final int numberOfRows,
      final boolean closeScanner, final boolean trackMetrics) {
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
   *
   * @param scannerId
   * @param numberOfRows
   * @param closeScanner
   * @param nextCallSeq
   * @return a scan request
   */
  public static ScanRequest buildScanRequest(final long scannerId, final int numberOfRows,
      final boolean closeScanner, final long nextCallSeq, final boolean trackMetrics) {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setScannerId(scannerId);
    builder.setNextCallSeq(nextCallSeq);
    builder.setClientHandlesPartials(true);
    builder.setClientHandlesHeartbeats(true);
    builder.setTrackScanMetrics(trackMetrics);
    return builder.build();
  }

  /**
   * Create a protocol buffer bulk load request
   *
   * @param familyPaths
   * @param regionName
   * @param assignSeqNum
   * @return a bulk load request
   */
  public static BulkLoadHFileRequest buildBulkLoadHFileRequest(
      final List<Pair<byte[], String>> familyPaths,
      final byte[] regionName, boolean assignSeqNum) {
    BulkLoadHFileRequest.Builder builder = BulkLoadHFileRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    FamilyPath.Builder familyPathBuilder = FamilyPath.newBuilder();
    for (Pair<byte[], String> familyPath: familyPaths) {
      familyPathBuilder.setFamily(ByteStringer.wrap(familyPath.getFirst()));
      familyPathBuilder.setPath(familyPath.getSecond());
      builder.addFamilyPath(familyPathBuilder.build());
    }
    builder.setAssignSeqNum(assignSeqNum);
    return builder.build();
  }

  /**
   * Create a protocol buffer multi request for a list of actions.
   * Propagates Actions original index.
   *
   * @param regionName
   * @param actions
   * @return a multi request
   * @throws IOException
   */
  public static <R> RegionAction.Builder buildRegionAction(final byte[] regionName,
      final List<Action<R>> actions, final RegionAction.Builder regionActionBuilder,
      final ClientProtos.Action.Builder actionBuilder,
      final MutationProto.Builder mutationBuilder) throws IOException {
    for (Action<R> action: actions) {
      Row row = action.getAction();
      actionBuilder.clear();
      actionBuilder.setIndex(action.getOriginalIndex());
      mutationBuilder.clear();
      if (row instanceof Get) {
        Get g = (Get)row;
        regionActionBuilder.addAction(actionBuilder.setGet(ProtobufUtil.toGet(g)));
      } else if (row instanceof Put) {
        regionActionBuilder.addAction(actionBuilder.
          setMutation(ProtobufUtil.toMutation(MutationType.PUT, (Put)row, mutationBuilder)));
      } else if (row instanceof Delete) {
        regionActionBuilder.addAction(actionBuilder.
          setMutation(ProtobufUtil.toMutation(MutationType.DELETE, (Delete)row, mutationBuilder)));
      } else if (row instanceof Append) {
        regionActionBuilder.addAction(actionBuilder.setMutation(ProtobufUtil.toMutation(
            MutationType.APPEND, (Append)row, mutationBuilder, action.getNonce())));
      } else if (row instanceof Increment) {
        regionActionBuilder.addAction(actionBuilder.setMutation(
            ProtobufUtil.toMutation((Increment)row, mutationBuilder, action.getNonce())));
      } else if (row instanceof RegionCoprocessorServiceExec) {
        RegionCoprocessorServiceExec exec = (RegionCoprocessorServiceExec) row;
        regionActionBuilder.addAction(actionBuilder.setServiceCall(
            ClientProtos.CoprocessorServiceCall.newBuilder()
              .setRow(ByteStringer.wrap(exec.getRow()))
              .setServiceName(exec.getMethod().getService().getFullName())
              .setMethodName(exec.getMethod().getName())
              .setRequest(exec.getRequest().toByteString())));
      } else if (row instanceof RowMutations) {
        throw new UnsupportedOperationException("No RowMutations in multi calls; use mutateRow");
      } else {
        throw new DoNotRetryIOException("Multi doesn't support " + row.getClass().getName());
      }
    }
    return regionActionBuilder;
  }

  /**
   * Create a protocol buffer multirequest with NO data for a list of actions (data is carried
   * otherwise than via protobuf).  This means it just notes attributes, whether to write the
   * WAL, etc., and the presence in protobuf serves as place holder for the data which is
   * coming along otherwise.  Note that Get is different.  It does not contain 'data' and is always
   * carried by protobuf.  We return references to the data by adding them to the passed in
   * <code>data</code> param.
   *
   * <p>Propagates Actions original index.
   *
   * @param regionName
   * @param actions
   * @param cells Place to stuff references to actual data.
   * @return a multi request that does not carry any data.
   * @throws IOException
   */
  public static <R> RegionAction.Builder buildNoDataRegionAction(final byte[] regionName,
      final List<Action<R>> actions, final List<CellScannable> cells,
      final RegionAction.Builder regionActionBuilder,
      final ClientProtos.Action.Builder actionBuilder,
      final MutationProto.Builder mutationBuilder) throws IOException {
    RegionAction.Builder builder = getRegionActionBuilderWithRegion(
      RegionAction.newBuilder(), regionName);
    for (Action<R> action: actions) {
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
      } else if (row instanceof Increment) {
        Increment i = (Increment)row;
        cells.add(i);
        builder.addAction(actionBuilder.setMutation(ProtobufUtil.toMutationNoData(
          MutationType.INCREMENT, i, mutationBuilder, action.getNonce())));
      } else if (row instanceof RowMutations) {
        continue; // ignore RowMutations
      } else {
        throw new DoNotRetryIOException("Multi doesn't support " + row.getClass().getName());
      }
    }
    return builder;
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
    GetRegionInfoRequest.Builder builder = GetRegionInfoRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    if (includeCompactionState) {
      builder.setCompactionState(includeCompactionState);
    }
    return builder.build();
  }

 /**
  * Create a protocol buffer GetStoreFileRequest for a given region name
  *
  * @param regionName the name of the region to get info
  * @param family the family to get store file list
  * @return a protocol buffer GetStoreFileRequest
  */
 public static GetStoreFileRequest
     buildGetStoreFileRequest(final byte[] regionName, final byte[] family) {
   GetStoreFileRequest.Builder builder = GetStoreFileRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.addFamily(ByteStringer.wrap(family));
   return builder.build();
 }

 /**
  * Create a protocol buffer GetOnlineRegionRequest
  *
  * @return a protocol buffer GetOnlineRegionRequest
  */
 public static GetOnlineRegionRequest buildGetOnlineRegionRequest() {
   return GetOnlineRegionRequest.newBuilder().build();
 }

 /**
  * Create a protocol buffer FlushRegionRequest for a given region name
  *
  * @param regionName the name of the region to get info
  * @return a protocol buffer FlushRegionRequest
  */
 public static FlushRegionRequest
     buildFlushRegionRequest(final byte[] regionName) {
   return buildFlushRegionRequest(regionName, false);
 }

 /**
  * Create a protocol buffer FlushRegionRequest for a given region name
  *
  * @param regionName the name of the region to get info
  * @return a protocol buffer FlushRegionRequest
  */
 public static FlushRegionRequest
     buildFlushRegionRequest(final byte[] regionName, boolean writeFlushWALMarker) {
   FlushRegionRequest.Builder builder = FlushRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.setWriteFlushWalMarker(writeFlushWALMarker);
   return builder.build();
 }

 /**
  * Create a protocol buffer OpenRegionRequest to open a list of regions
  *
  * @param server the serverName for the RPC
  * @param regionOpenInfos info of a list of regions to open
  * @param openForReplay
  * @return a protocol buffer OpenRegionRequest
  */
 public static OpenRegionRequest
     buildOpenRegionRequest(ServerName server, final List<Pair<HRegionInfo,
         List<ServerName>>> regionOpenInfos, Boolean openForReplay) {
   OpenRegionRequest.Builder builder = OpenRegionRequest.newBuilder();
   for (Pair<HRegionInfo, List<ServerName>> regionOpenInfo: regionOpenInfos) {
     builder.addOpenInfo(buildRegionOpenInfo(regionOpenInfo.getFirst(),
       regionOpenInfo.getSecond(), openForReplay));
   }
   if (server != null) {
     builder.setServerStartCode(server.getStartcode());
   }
   return builder.build();
 }

 /**
  * Create a protocol buffer OpenRegionRequest for a given region
  *
  * @param server the serverName for the RPC
  * @param region the region to open
  * @param favoredNodes
  * @param openForReplay
  * @return a protocol buffer OpenRegionRequest
  */
 public static OpenRegionRequest buildOpenRegionRequest(ServerName server,
     final HRegionInfo region, List<ServerName> favoredNodes,
     Boolean openForReplay) {
   OpenRegionRequest.Builder builder = OpenRegionRequest.newBuilder();
   builder.addOpenInfo(buildRegionOpenInfo(region, favoredNodes,
     openForReplay));
   if (server != null) {
     builder.setServerStartCode(server.getStartcode());
   }
   return builder.build();
 }

 /**
  * Create a protocol buffer UpdateFavoredNodesRequest to update a list of favorednode mappings
  * @param updateRegionInfos
  * @return a protocol buffer UpdateFavoredNodesRequest
  */
 public static UpdateFavoredNodesRequest buildUpdateFavoredNodesRequest(
     final List<Pair<HRegionInfo, List<ServerName>>> updateRegionInfos) {
   UpdateFavoredNodesRequest.Builder ubuilder = UpdateFavoredNodesRequest.newBuilder();
   for (Pair<HRegionInfo, List<ServerName>> pair : updateRegionInfos) {
     RegionUpdateInfo.Builder builder = RegionUpdateInfo.newBuilder();
     builder.setRegion(HRegionInfo.convert(pair.getFirst()));
     for (ServerName server : pair.getSecond()) {
       builder.addFavoredNodes(ProtobufUtil.toServerName(server));
     }
     ubuilder.addUpdateInfo(builder.build());
   }
   return ubuilder.build();
 }

 /**
  * Create a CloseRegionRequest for a given region name
  *
  * @param regionName the name of the region to close
  * @return a CloseRegionRequest
  */
 public static CloseRegionRequest buildCloseRegionRequest(ServerName server,
     final byte[] regionName) {
   return buildCloseRegionRequest(server, regionName, null);
 }

  public static CloseRegionRequest buildCloseRegionRequest(ServerName server,
    final byte[] regionName, ServerName destinationServer) {
    CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    if (destinationServer != null){
      builder.setDestinationServer(ProtobufUtil.toServerName(destinationServer));
    }
    if (server != null) {
      builder.setServerStartCode(server.getStartcode());
    }
    return builder.build();
  }

  /**
   *  Create a WarmupRegionRequest for a given region name
   *
   *  @param regionInfo Region we are warming up
   */
  public static WarmupRegionRequest buildWarmupRegionRequest(final HRegionInfo regionInfo) {
    WarmupRegionRequest.Builder builder = WarmupRegionRequest.newBuilder();
    builder.setRegionInfo(HRegionInfo.convert(regionInfo));
    return builder.build();
  }
 /**
  * Create a CloseRegionRequest for a given encoded region name
  *
  * @param encodedRegionName the name of the region to close
  * @return a CloseRegionRequest
  */
 public static CloseRegionRequest
     buildCloseRegionRequest(ServerName server, final String encodedRegionName) {
   CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.ENCODED_REGION_NAME,
     Bytes.toBytes(encodedRegionName));
   builder.setRegion(region);
   if (server != null) {
     builder.setServerStartCode(server.getStartcode());
   }
   return builder.build();
 }

 /**
  * Create a SplitRegionRequest for a given region name
  *
  * @param regionName the name of the region to split
  * @param splitPoint the split point
  * @return a SplitRegionRequest
  */
 public static SplitRegionRequest buildSplitRegionRequest(
     final byte[] regionName, final byte[] splitPoint) {
   SplitRegionRequest.Builder builder = SplitRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   if (splitPoint != null) {
     builder.setSplitPoint(ByteStringer.wrap(splitPoint));
   }
   return builder.build();
 }

  /**
   * Create a MergeRegionsRequest for the given regions
   * @param regionA name of region a
   * @param regionB name of region b
   * @param forcible true if it is a compulsory merge
   * @return a MergeRegionsRequest
   */
  public static MergeRegionsRequest buildMergeRegionsRequest(
      final byte[] regionA, final byte[] regionB, final boolean forcible) {
    MergeRegionsRequest.Builder builder = MergeRegionsRequest.newBuilder();
    RegionSpecifier regionASpecifier = buildRegionSpecifier(
        RegionSpecifierType.REGION_NAME, regionA);
    RegionSpecifier regionBSpecifier = buildRegionSpecifier(
        RegionSpecifierType.REGION_NAME, regionB);
    builder.setRegionA(regionASpecifier);
    builder.setRegionB(regionBSpecifier);
    builder.setForcible(forcible);
    return builder.build();
  }

 /**
  * Create a  CompactRegionRequest for a given region name
  *
  * @param regionName the name of the region to get info
  * @param major indicator if it is a major compaction
  * @return a CompactRegionRequest
  */
 public static CompactRegionRequest buildCompactRegionRequest(
     final byte[] regionName, final boolean major, final byte [] family) {
   CompactRegionRequest.Builder builder = CompactRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.setMajor(major);
   if (family != null) {
     builder.setFamily(ByteStringer.wrap(family));
   }
   return builder.build();
 }

 /**
  * @see {@link #buildRollWALWriterRequest()
  */
 private static RollWALWriterRequest ROLL_WAL_WRITER_REQUEST =
     RollWALWriterRequest.newBuilder().build();

  /**
  * Create a new RollWALWriterRequest
  *
  * @return a ReplicateWALEntryRequest
  */
 public static RollWALWriterRequest buildRollWALWriterRequest() {
   return ROLL_WAL_WRITER_REQUEST;
 }

 /**
  * @see {@link #buildGetServerInfoRequest()}
  */
 private static GetServerInfoRequest GET_SERVER_INFO_REQUEST =
   GetServerInfoRequest.newBuilder().build();

 /**
  * Create a new GetServerInfoRequest
  *
  * @return a GetServerInfoRequest
  */
 public static GetServerInfoRequest buildGetServerInfoRequest() {
   return GET_SERVER_INFO_REQUEST;
 }

 /**
  * Create a new StopServerRequest
  *
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
    regionBuilder.setValue(ByteStringer.wrap(value));
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
  private static Condition buildCondition(final byte[] row,
      final byte[] family, final byte [] qualifier,
      final ByteArrayComparable comparator,
      final CompareType compareType) throws IOException {
    Condition.Builder builder = Condition.newBuilder();
    builder.setRow(ByteStringer.wrap(row));
    builder.setFamily(ByteStringer.wrap(family));
    builder.setQualifier(ByteStringer.wrap(qualifier));
    builder.setComparator(ProtobufUtil.toComparator(comparator));
    builder.setCompareType(compareType);
    return builder.build();
  }

  /**
   * Create a protocol buffer AddColumnRequest
   *
   * @param tableName
   * @param column
   * @return an AddColumnRequest
   */
  public static AddColumnRequest buildAddColumnRequest(
      final TableName tableName, final HColumnDescriptor column) {
    AddColumnRequest.Builder builder = AddColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setColumnFamilies(column.convert());
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
      final TableName tableName, final byte [] columnName) {
    DeleteColumnRequest.Builder builder = DeleteColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    builder.setColumnName(ByteStringer.wrap(columnName));
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
      final TableName tableName, final HColumnDescriptor column) {
    ModifyColumnRequest.Builder builder = ModifyColumnRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    builder.setColumnFamilies(column.convert());
    return builder.build();
  }

  /**
   * Create a protocol buffer MoveRegionRequest
   *
   * @param encodedRegionName
   * @param destServerName
   * @return A MoveRegionRequest
   * @throws DeserializationException
   */
  public static MoveRegionRequest buildMoveRegionRequest(
      final byte [] encodedRegionName, final byte [] destServerName) throws
      DeserializationException {
    MoveRegionRequest.Builder builder = MoveRegionRequest.newBuilder();
    builder.setRegion(
      buildRegionSpecifier(RegionSpecifierType.ENCODED_REGION_NAME,encodedRegionName));
    if (destServerName != null) {
      builder.setDestServerName(
        ProtobufUtil.toServerName(ServerName.valueOf(Bytes.toString(destServerName))));
    }
    return builder.build();
  }

  public static DispatchMergingRegionsRequest buildDispatchMergingRegionsRequest(
      final byte[] encodedNameOfRegionA, final byte[] encodedNameOfRegionB,
      final boolean forcible) throws DeserializationException {
    DispatchMergingRegionsRequest.Builder builder = DispatchMergingRegionsRequest.newBuilder();
    builder.setRegionA(buildRegionSpecifier(
        RegionSpecifierType.ENCODED_REGION_NAME, encodedNameOfRegionA));
    builder.setRegionB(buildRegionSpecifier(
        RegionSpecifierType.ENCODED_REGION_NAME, encodedNameOfRegionB));
    builder.setForcible(forcible);
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
  public static DeleteTableRequest buildDeleteTableRequest(final TableName tableName) {
    DeleteTableRequest.Builder builder = DeleteTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer TruncateTableRequest
   *
   * @param tableName name of table to truncate
   * @param preserveSplits True if the splits should be preserved
   * @return a TruncateTableRequest
   */
  public static TruncateTableRequest buildTruncateTableRequest(final TableName tableName,
        boolean preserveSplits) {
    TruncateTableRequest.Builder builder = TruncateTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    builder.setPreserveSplits(preserveSplits);
    return builder.build();
  }

  /**
   * Creates a protocol buffer EnableTableRequest
   *
   * @param tableName
   * @return an EnableTableRequest
   */
  public static EnableTableRequest buildEnableTableRequest(final TableName tableName) {
    EnableTableRequest.Builder builder = EnableTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DisableTableRequest
   *
   * @param tableName
   * @return a DisableTableRequest
   */
  public static DisableTableRequest buildDisableTableRequest(final TableName tableName) {
    DisableTableRequest.Builder builder = DisableTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    return builder.build();
  }

  /**
   * Creates a protocol buffer CreateTableRequest
   *
   * @param hTableDesc
   * @param splitKeys
   * @return a CreateTableRequest
   */
  public static CreateTableRequest buildCreateTableRequest(
      final HTableDescriptor hTableDesc, final byte [][] splitKeys) {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setTableSchema(hTableDesc.convert());
    if (splitKeys != null) {
      for (byte [] splitKey : splitKeys) {
        builder.addSplitKeys(ByteStringer.wrap(splitKey));
      }
    }
    return builder.build();
  }


  /**
   * Creates a protocol buffer ModifyTableRequest
   *
   * @param tableName
   * @param hTableDesc
   * @return a ModifyTableRequest
   */
  public static ModifyTableRequest buildModifyTableRequest(
      final TableName tableName, final HTableDescriptor hTableDesc) {
    ModifyTableRequest.Builder builder = ModifyTableRequest.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName((tableName)));
    builder.setTableSchema(hTableDesc.convert());
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
    if (pattern != null) builder.setRegex(pattern.toString());
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
    if (pattern != null) builder.setRegex(pattern.toString());
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
  public static BalanceRequest buildBalanceRequest() {
    return BalanceRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer SetBalancerRunningRequest
   *
   * @param on
   * @param synchronous
   * @return a SetBalancerRunningRequest
   */
  public static SetBalancerRunningRequest buildSetBalancerRunningRequest(boolean on, boolean synchronous) {
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
   * @see {@link #buildGetClusterStatusRequest}
   */
  private static final GetClusterStatusRequest GET_CLUSTER_STATUS_REQUEST =
      GetClusterStatusRequest.newBuilder().build();

  /**
   * Creates a protocol buffer GetClusterStatusRequest
   *
   * @return A GetClusterStatusRequest
   */
  public static GetClusterStatusRequest buildGetClusterStatusRequest() {
    return GET_CLUSTER_STATUS_REQUEST;
  }

  /**
   * @see {@link #buildCatalogScanRequest}
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
   * @see {@link #buildIsCatalogJanitorEnabledRequest()}
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
   * Creates a request for querying the master the last flushed sequence Id for a region
   * @param regionName
   * @return A {@link GetLastFlushedSequenceIdRequest}
   */
  public static GetLastFlushedSequenceIdRequest buildGetLastFlushedSequenceIdRequest(
      byte[] regionName) {
    return GetLastFlushedSequenceIdRequest.newBuilder().setRegionName(
        ByteStringer.wrap(regionName)).build();
  }

  /**
   * Create a request to grant user permissions.
   *
   * @param username the short user name who to grant permissions
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(
      String username, AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.GlobalPermission.Builder permissionBuilder =
        AccessControlProtos.GlobalPermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    ret.setType(AccessControlProtos.Permission.Type.Global)
       .setGlobalPermission(permissionBuilder);
    return AccessControlProtos.GrantRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a request to grant user permissions.
   *
   * @param username the short user name who to grant permissions
   * @param tableName optional table name the permissions apply
   * @param family optional column family
   * @param qualifier optional qualifier
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(
      String username, TableName tableName, byte[] family, byte[] qualifier,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.TablePermission.Builder permissionBuilder =
        AccessControlProtos.TablePermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (tableName == null) {
      throw new NullPointerException("TableName cannot be null");
    }
    permissionBuilder.setTableName(ProtobufUtil.toProtoTableName(tableName));

    if (family != null) {
      permissionBuilder.setFamily(ByteStringer.wrap(family));
    }
    if (qualifier != null) {
      permissionBuilder.setQualifier(ByteStringer.wrap(qualifier));
    }
    ret.setType(AccessControlProtos.Permission.Type.Table)
       .setTablePermission(permissionBuilder);
    return AccessControlProtos.GrantRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a request to grant user permissions.
   *
   * @param username the short user name who to grant permissions
   * @param namespace optional table name the permissions apply
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(
      String username, String namespace,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.NamespacePermission.Builder permissionBuilder =
        AccessControlProtos.NamespacePermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (namespace != null) {
      permissionBuilder.setNamespaceName(ByteString.copyFromUtf8(namespace));
    }
    ret.setType(AccessControlProtos.Permission.Type.Namespace)
       .setNamespacePermission(permissionBuilder);
    return AccessControlProtos.GrantRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a request to revoke user permissions.
   *
   * @param username the short user name whose permissions to be revoked
   * @param actions the permissions to be revoked
   * @return A {@link AccessControlProtos} RevokeRequest
   */
  public static AccessControlProtos.RevokeRequest buildRevokeRequest(
      String username, AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.GlobalPermission.Builder permissionBuilder =
        AccessControlProtos.GlobalPermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    ret.setType(AccessControlProtos.Permission.Type.Global)
       .setGlobalPermission(permissionBuilder);
    return AccessControlProtos.RevokeRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a request to revoke user permissions.
   *
   * @param username the short user name whose permissions to be revoked
   * @param tableName optional table name the permissions apply
   * @param family optional column family
   * @param qualifier optional qualifier
   * @param actions the permissions to be revoked
   * @return A {@link AccessControlProtos} RevokeRequest
   */
  public static AccessControlProtos.RevokeRequest buildRevokeRequest(
      String username, TableName tableName, byte[] family, byte[] qualifier,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.TablePermission.Builder permissionBuilder =
        AccessControlProtos.TablePermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (tableName != null) {
      permissionBuilder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    if (family != null) {
      permissionBuilder.setFamily(ByteStringer.wrap(family));
    }
    if (qualifier != null) {
      permissionBuilder.setQualifier(ByteStringer.wrap(qualifier));
    }
    ret.setType(AccessControlProtos.Permission.Type.Table)
       .setTablePermission(permissionBuilder);
    return AccessControlProtos.RevokeRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a request to revoke user permissions.
   *
   * @param username the short user name whose permissions to be revoked
   * @param namespace optional table name the permissions apply
   * @param actions the permissions to be revoked
   * @return A {@link AccessControlProtos} RevokeRequest
   */
  public static AccessControlProtos.RevokeRequest buildRevokeRequest(
      String username, String namespace,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.NamespacePermission.Builder permissionBuilder =
        AccessControlProtos.NamespacePermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (namespace != null) {
      permissionBuilder.setNamespaceName(ByteString.copyFromUtf8(namespace));
    }
    ret.setType(AccessControlProtos.Permission.Type.Namespace)
       .setNamespacePermission(permissionBuilder);
    return AccessControlProtos.RevokeRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a RegionOpenInfo based on given region info and version of offline node
   */
  private static RegionOpenInfo buildRegionOpenInfo(
      final HRegionInfo region,
      final List<ServerName> favoredNodes, Boolean openForReplay) {
    RegionOpenInfo.Builder builder = RegionOpenInfo.newBuilder();
    builder.setRegion(HRegionInfo.convert(region));
    if (favoredNodes != null) {
      for (ServerName server : favoredNodes) {
        builder.addFavoredNodes(ProtobufUtil.toServerName(server));
      }
    }
    if(openForReplay != null) {
      builder.setOpenForDistributedLogReplay(openForReplay);
    }
    return builder.build();
  }
}
