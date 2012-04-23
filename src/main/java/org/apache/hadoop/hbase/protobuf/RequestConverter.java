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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALEdit.ScopeType;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALKey;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALEdit.FamilyScope;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Condition.CompareType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.ColumnValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.DeleteType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.MutateType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

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
    builder.setClosestRowBefore(true);
    builder.setRegion(region);

    Column.Builder columnBuilder = Column.newBuilder();
    columnBuilder.setFamily(ByteString.copyFrom(family));
    ClientProtos.Get.Builder getBuilder =
      ClientProtos.Get.newBuilder();
    getBuilder.setRow(ByteString.copyFrom(row));
    getBuilder.addColumn(columnBuilder.build());
    builder.setGet(getBuilder.build());
    return builder.build();
  }

  /**
   * Create a protocol buffer GetRequest for a client Get
   *
   * @param regionName the name of the region to get
   * @param get the client Get
   * @return a protocol buffer GetReuqest
   */
  public static GetRequest buildGetRequest(final byte[] regionName,
      final Get get) throws IOException {
    return buildGetRequest(regionName, get, false);
  }

  /**
   * Create a protocol buffer GetRequest for a client Get
   *
   * @param regionName the name of the region to get
   * @param get the client Get
   * @param existenceOnly indicate if check row existence only
   * @return a protocol buffer GetReuqest
   */
  public static GetRequest buildGetRequest(final byte[] regionName,
      final Get get, final boolean existenceOnly) throws IOException {
    GetRequest.Builder builder = GetRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setExistenceOnly(existenceOnly);
    builder.setRegion(region);
    builder.setGet(buildGet(get));
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
   * @param writeToWAL
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final byte[] row, final byte[] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL) {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);

    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setRow(ByteString.copyFrom(row));
    mutateBuilder.setMutateType(MutateType.INCREMENT);
    mutateBuilder.setWriteToWAL(writeToWAL);
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    columnBuilder.setFamily(ByteString.copyFrom(family));
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    valueBuilder.setValue(ByteString.copyFrom(Bytes.toBytes(amount)));
    valueBuilder.setQualifier(ByteString.copyFrom(qualifier));
    columnBuilder.addQualifierValue(valueBuilder.build());
    mutateBuilder.addColumnValue(columnBuilder.build());

    builder.setMutate(mutateBuilder.build());
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
      final byte [] qualifier, final WritableByteArrayComparable comparator,
      final CompareType compareType, final Put put) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    Condition condition = buildCondition(
      row, family, qualifier, comparator, compareType);
    builder.setMutate(buildMutate(MutateType.PUT, put));
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
      final byte [] qualifier, final WritableByteArrayComparable comparator,
      final CompareType compareType, final Delete delete) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    Condition condition = buildCondition(
      row, family, qualifier, comparator, compareType);
    builder.setMutate(buildMutate(MutateType.DELETE, delete));
    builder.setCondition(condition);
    return builder.build();
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
    builder.setMutate(buildMutate(MutateType.PUT, put));
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
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final Append append) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutate(buildMutate(MutateType.APPEND, append));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a client increment
   *
   * @param regionName
   * @param increment
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final Increment increment) {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutate(buildMutate(increment));
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
    builder.setMutate(buildMutate(MutateType.DELETE, delete));
    return builder.build();
  }

  /**
   * Create a protocol buffer MultiRequest for a row mutations
   *
   * @param regionName
   * @param rowMutations
   * @return a multi request
   * @throws IOException
   */
  public static MultiRequest buildMultiRequest(final byte[] regionName,
      final RowMutations rowMutations) throws IOException {
    MultiRequest.Builder builder = MultiRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setAtomic(true);
    for (Mutation mutation: rowMutations.getMutations()) {
      MutateType mutateType = null;
      if (mutation instanceof Put) {
        mutateType = MutateType.PUT;
      } else if (mutation instanceof Delete) {
        mutateType = MutateType.DELETE;
      } else {
        throw new DoNotRetryIOException(
          "RowMutations supports only put and delete, not "
            + mutation.getClass().getName());
      }
      Mutate mutate = buildMutate(mutateType, mutation);
      builder.addAction(ProtobufUtil.toParameter(mutate));
    }
    return builder.build();
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
  public static ScanRequest buildScanRequest(final byte[] regionName,
      final Scan scan, final int numberOfRows,
        final boolean closeScanner) throws IOException {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setRegion(region);

    ClientProtos.Scan.Builder scanBuilder =
      ClientProtos.Scan.newBuilder();
    scanBuilder.setCacheBlocks(scan.getCacheBlocks());
    scanBuilder.setBatchSize(scan.getBatch());
    scanBuilder.setMaxVersions(scan.getMaxVersions());
    TimeRange timeRange = scan.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
        HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      scanBuilder.setTimeRange(timeRangeBuilder.build());
    }
    Map<String, byte[]> attributes = scan.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        scanBuilder.addAttribute(attributeBuilder.build());
      }
    }
    byte[] startRow = scan.getStartRow();
    if (startRow != null && startRow.length > 0) {
      scanBuilder.setStartRow(ByteString.copyFrom(startRow));
    }
    byte[] stopRow = scan.getStopRow();
    if (stopRow != null && stopRow.length > 0) {
      scanBuilder.setStopRow(ByteString.copyFrom(stopRow));
    }
    if (scan.hasFilter()) {
      scanBuilder.setFilter(ProtobufUtil.toParameter(scan.getFilter()));
    }
    Column.Builder columnBuilder = Column.newBuilder();
    for (Map.Entry<byte[],NavigableSet<byte []>>
        family: scan.getFamilyMap().entrySet()) {
      columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
      NavigableSet<byte []> columns = family.getValue();
      columnBuilder.clearQualifier();
      if (columns != null && columns.size() > 0) {
        for (byte [] qualifier: family.getValue()) {
          if (qualifier != null) {
            columnBuilder.addQualifier(ByteString.copyFrom(qualifier));
          }
        }
      }
      scanBuilder.addColumn(columnBuilder.build());
    }
    builder.setScan(scanBuilder.build());
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
  public static ScanRequest buildScanRequest(final long scannerId,
      final int numberOfRows, final boolean closeScanner) {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setScannerId(scannerId);
    return builder.build();
  }

  /**
   * Create a protocol buffer LockRowRequest
   *
   * @param regionName
   * @param row
   * @return a lock row request
   */
  public static LockRowRequest buildLockRowRequest(
      final byte[] regionName, final byte[] row) {
    LockRowRequest.Builder builder = LockRowRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.addRow(ByteString.copyFrom(row));
    return builder.build();
  }

  /**
   * Create a protocol buffer UnlockRowRequest
   *
   * @param regionName
   * @param lockId
   * @return a unlock row request
   */
  public static UnlockRowRequest buildUnlockRowRequest(
      final byte[] regionName, final long lockId) {
    UnlockRowRequest.Builder builder = UnlockRowRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setLockId(lockId);
    return builder.build();
  }

  /**
   * Create a protocol buffer bulk load request
   *
   * @param familyPaths
   * @param regionName
   * @return a bulk load request
   */
  public static BulkLoadHFileRequest buildBulkLoadHFileRequest(
      final List<Pair<byte[], String>> familyPaths, final byte[] regionName) {
    BulkLoadHFileRequest.Builder builder = BulkLoadHFileRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    FamilyPath.Builder familyPathBuilder = FamilyPath.newBuilder();
    for (Pair<byte[], String> familyPath: familyPaths) {
      familyPathBuilder.setFamily(ByteString.copyFrom(familyPath.getFirst()));
      familyPathBuilder.setPath(familyPath.getSecond());
      builder.addFamilyPath(familyPathBuilder.build());
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer coprocessor exec request
   *
   * @param regionName
   * @param exec
   * @return a coprocessor exec request
   * @throws IOException
   */
  public static ExecCoprocessorRequest buildExecCoprocessorRequest(
      final byte[] regionName, final Exec exec) throws IOException {
    ExecCoprocessorRequest.Builder builder = ExecCoprocessorRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setCall(buildExec(exec));
    return builder.build();
  }

  /**
   * Create a protocol buffer multi request for a list of actions.
   * RowMutations in the list (if any) will be ignored.
   *
   * @param regionName
   * @param actions
   * @return a multi request
   * @throws IOException
   */
  public static <R> MultiRequest buildMultiRequest(final byte[] regionName,
      final List<Action<R>> actions) throws IOException {
    MultiRequest.Builder builder = MultiRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    for (Action<R> action: actions) {
      Message protoAction = null;
      Row row = action.getAction();
      if (row instanceof Get) {
        protoAction = buildGet((Get)row);
      } else if (row instanceof Put) {
        protoAction = buildMutate(MutateType.PUT, (Put)row);
      } else if (row instanceof Delete) {
        protoAction = buildMutate(MutateType.DELETE, (Delete)row);
      } else if (row instanceof Exec) {
        protoAction = buildExec((Exec)row);
      } else if (row instanceof Append) {
        protoAction = buildMutate(MutateType.APPEND, (Append)row);
      } else if (row instanceof Increment) {
        protoAction = buildMutate((Increment)row);
      } else if (row instanceof RowMutations) {
        continue; // ignore RowMutations
      } else {
        throw new DoNotRetryIOException(
          "multi doesn't support " + row.getClass().getName());
      }
      builder.addAction(ProtobufUtil.toParameter(protoAction));
    }
    return builder.build();
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
   GetRegionInfoRequest.Builder builder = GetRegionInfoRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
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
   builder.addFamily(ByteString.copyFrom(family));
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
   FlushRegionRequest.Builder builder = FlushRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   return builder.build();
 }

 /**
  * Create a protocol buffer OpenRegionRequest to open a list of regions
  *
  * @param regions the list of regions to open
  * @return a protocol buffer OpenRegionRequest
  */
 public static OpenRegionRequest
     buildOpenRegionRequest(final List<HRegionInfo> regions) {
   OpenRegionRequest.Builder builder = OpenRegionRequest.newBuilder();
   for (HRegionInfo region: regions) {
     builder.addRegion(ProtobufUtil.toRegionInfo(region));
   }
   return builder.build();
 }

 /**
  * Create a protocol buffer OpenRegionRequest for a given region
  *
  * @param region the region to open
  * @return a protocol buffer OpenRegionRequest
  */
 public static OpenRegionRequest
     buildOpenRegionRequest(final HRegionInfo region) {
   return buildOpenRegionRequest(region, -1);
 }

 /**
  * Create a protocol buffer OpenRegionRequest for a given region
  *
  * @param region the region to open
  * @param versionOfOfflineNode that needs to be present in the offline node
  * @return a protocol buffer OpenRegionRequest
  */
 public static OpenRegionRequest buildOpenRegionRequest(
     final HRegionInfo region, final int versionOfOfflineNode) {
   OpenRegionRequest.Builder builder = OpenRegionRequest.newBuilder();
   builder.addRegion(ProtobufUtil.toRegionInfo(region));
   if (versionOfOfflineNode >= 0) {
     builder.setVersionOfOfflineNode(versionOfOfflineNode);
   }
   return builder.build();
 }

 /**
  * Create a CloseRegionRequest for a given region name
  *
  * @param regionName the name of the region to close
  * @param transitionInZK indicator if to transition in ZK
  * @return a CloseRegionRequest
  */
 public static CloseRegionRequest buildCloseRegionRequest(
     final byte[] regionName, final boolean transitionInZK) {
   CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.setTransitionInZK(transitionInZK);
   return builder.build();
 }

 /**
  * Create a CloseRegionRequest for a given region name
  *
  * @param regionName the name of the region to close
  * @param versionOfClosingNode
  *   the version of znode to compare when RS transitions the znode from
  *   CLOSING state.
  * @return a CloseRegionRequest
  */
 public static CloseRegionRequest buildCloseRegionRequest(
     final byte[] regionName, final int versionOfClosingNode) {
   CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.setVersionOfClosingNode(versionOfClosingNode);
   return builder.build();
 }

 /**
  * Create a CloseRegionRequest for a given encoded region name
  *
  * @param encodedRegionName the name of the region to close
  * @param transitionInZK indicator if to transition in ZK
  * @return a CloseRegionRequest
  */
 public static CloseRegionRequest
     buildCloseRegionRequest(final String encodedRegionName,
       final boolean transitionInZK) {
   CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.ENCODED_REGION_NAME,
     Bytes.toBytes(encodedRegionName));
   builder.setRegion(region);
   builder.setTransitionInZK(transitionInZK);
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
     builder.setSplitPoint(ByteString.copyFrom(splitPoint));
   }
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
     final byte[] regionName, final boolean major) {
   CompactRegionRequest.Builder builder = CompactRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.setMajor(major);
   return builder.build();
 }

 /**
  * Create a new ReplicateWALEntryRequest from a list of HLog entries
  *
  * @param entries the HLog entries to be replicated
  * @return a ReplicateWALEntryRequest
  */
 public static ReplicateWALEntryRequest
     buildReplicateWALEntryRequest(final HLog.Entry[] entries) {
   FamilyScope.Builder scopeBuilder = FamilyScope.newBuilder();
   WALEntry.Builder entryBuilder = WALEntry.newBuilder();
   ReplicateWALEntryRequest.Builder builder =
     ReplicateWALEntryRequest.newBuilder();
   for (HLog.Entry entry: entries) {
     entryBuilder.clear();
     WALKey.Builder keyBuilder = entryBuilder.getKeyBuilder();
     HLogKey key = entry.getKey();
     keyBuilder.setEncodedRegionName(
       ByteString.copyFrom(key.getEncodedRegionName()));
     keyBuilder.setTableName(ByteString.copyFrom(key.getTablename()));
     keyBuilder.setLogSequenceNumber(key.getLogSeqNum());
     keyBuilder.setWriteTime(key.getWriteTime());
     UUID clusterId = key.getClusterId();
     if (clusterId != null) {
       AdminProtos.UUID.Builder uuidBuilder = keyBuilder.getClusterIdBuilder();
       uuidBuilder.setLeastSigBits(clusterId.getLeastSignificantBits());
       uuidBuilder.setMostSigBits(clusterId.getMostSignificantBits());
     }
     WALEdit edit = entry.getEdit();
     WALEntry.WALEdit.Builder editBuilder = entryBuilder.getEditBuilder();
     NavigableMap<byte[], Integer> scopes = edit.getScopes();
     if (scopes != null && !scopes.isEmpty()) {
       for (Map.Entry<byte[], Integer> scope: scopes.entrySet()) {
         scopeBuilder.setFamily(ByteString.copyFrom(scope.getKey()));
         ScopeType scopeType = ScopeType.valueOf(scope.getValue().intValue());
         scopeBuilder.setScopeType(scopeType);
         editBuilder.addFamilyScope(scopeBuilder.build());
       }
     }
     List<KeyValue> keyValues = edit.getKeyValues();
     for (KeyValue value: keyValues) {
       editBuilder.addKeyValueBytes(ByteString.copyFrom(
         value.getBuffer(), value.getOffset(), value.getLength()));
     }
     builder.addEntry(entryBuilder.build());
   }
   return builder.build();
 }

 /**
  * Create a new RollWALWriterRequest
  *
  * @return a ReplicateWALEntryRequest
  */
 public static RollWALWriterRequest buildRollWALWriterRequest() {
   RollWALWriterRequest.Builder builder = RollWALWriterRequest.newBuilder();
   return builder.build();
 }

 /**
  * Create a new GetServerInfoRequest
  *
  * @return a GetServerInfoRequest
  */
 public static GetServerInfoRequest buildGetServerInfoRequest() {
   GetServerInfoRequest.Builder builder =  GetServerInfoRequest.newBuilder();
   return builder.build();
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
    regionBuilder.setValue(ByteString.copyFrom(value));
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
      final WritableByteArrayComparable comparator,
      final CompareType compareType) throws IOException {
    Condition.Builder builder = Condition.newBuilder();
    builder.setRow(ByteString.copyFrom(row));
    builder.setFamily(ByteString.copyFrom(family));
    builder.setQualifier(ByteString.copyFrom(qualifier));
    builder.setComparator(ProtobufUtil.toParameter(comparator));
    builder.setCompareType(compareType);
    return builder.build();
  }

  /**
   * Create a new protocol buffer Exec based on a client Exec
   *
   * @param exec
   * @return
   * @throws IOException
   */
  private static ClientProtos.Exec buildExec(
      final Exec exec) throws IOException {
    ClientProtos.Exec.Builder
      builder = ClientProtos.Exec.newBuilder();
    Configuration conf = exec.getConf();
    if (conf != null) {
      NameStringPair.Builder propertyBuilder = NameStringPair.newBuilder();
      Iterator<Entry<String, String>> iterator = conf.iterator();
      while (iterator.hasNext()) {
        Entry<String, String> entry = iterator.next();
        propertyBuilder.setName(entry.getKey());
        propertyBuilder.setValue(entry.getValue());
        builder.addProperty(propertyBuilder.build());
      }
    }
    builder.setProtocolName(exec.getProtocolName());
    builder.setMethodName(exec.getMethodName());
    builder.setRow(ByteString.copyFrom(exec.getRow()));
    Object[] parameters = exec.getParameters();
    if (parameters != null && parameters.length > 0) {
      Class<?>[] declaredClasses = exec.getParameterClasses();
      for (int i = 0, n = parameters.length; i < n; i++) {
        builder.addParameter(
          ProtobufUtil.toParameter(declaredClasses[i], parameters[i]));
      }
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer Get based on a client Get.
   *
   * @param get the client Get
   * @return a protocol buffer Get
   * @throws IOException
   */
  private static ClientProtos.Get buildGet(
      final Get get) throws IOException {
    ClientProtos.Get.Builder builder =
      ClientProtos.Get.newBuilder();
    builder.setRow(ByteString.copyFrom(get.getRow()));
    builder.setCacheBlocks(get.getCacheBlocks());
    builder.setMaxVersions(get.getMaxVersions());
    if (get.getLockId() >= 0) {
      builder.setLockId(get.getLockId());
    }
    if (get.getFilter() != null) {
      builder.setFilter(ProtobufUtil.toParameter(get.getFilter()));
    }
    TimeRange timeRange = get.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
        HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      builder.setTimeRange(timeRangeBuilder.build());
    }
    Map<String, byte[]> attributes = get.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    if (get.hasFamilies()) {
      Column.Builder columnBuilder = Column.newBuilder();
      Map<byte[], NavigableSet<byte[]>> families = get.getFamilyMap();
      for (Map.Entry<byte[], NavigableSet<byte[]>> family: families.entrySet()) {
        NavigableSet<byte[]> qualifiers = family.getValue();
        columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte[] qualifier: qualifiers) {
            if (qualifier != null) {
              columnBuilder.addQualifier(ByteString.copyFrom(qualifier));
            }
          }
        }
        builder.addColumn(columnBuilder.build());
      }
    }
    return builder.build();
  }

  private static Mutate buildMutate(final Increment increment) {
    Mutate.Builder builder = Mutate.newBuilder();
    builder.setRow(ByteString.copyFrom(increment.getRow()));
    builder.setMutateType(MutateType.INCREMENT);
    builder.setWriteToWAL(increment.getWriteToWAL());
    if (increment.getLockId() >= 0) {
      builder.setLockId(increment.getLockId());
    }
    TimeRange timeRange = increment.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
        HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      builder.setTimeRange(timeRangeBuilder.build());
    }
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    for (Map.Entry<byte[],NavigableMap<byte[], Long>>
        family: increment.getFamilyMap().entrySet()) {
      columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
      columnBuilder.clearQualifierValue();
      NavigableMap<byte[], Long> values = family.getValue();
      if (values != null && values.size() > 0) {
        for (Map.Entry<byte[], Long> value: values.entrySet()) {
          valueBuilder.setQualifier(ByteString.copyFrom(value.getKey()));
          valueBuilder.setValue(ByteString.copyFrom(
            Bytes.toBytes(value.getValue().longValue())));
          columnBuilder.addQualifierValue(valueBuilder.build());
        }
      }
      builder.addColumnValue(columnBuilder.build());
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer Mutate based on a client Mutation
   *
   * @param mutateType
   * @param mutation
   * @return a mutate
   * @throws IOException
   */
  private static Mutate buildMutate(final MutateType mutateType,
      final Mutation mutation) throws IOException {
    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setRow(ByteString.copyFrom(mutation.getRow()));
    mutateBuilder.setMutateType(mutateType);
    mutateBuilder.setWriteToWAL(mutation.getWriteToWAL());
    if (mutation.getLockId() >= 0) {
      mutateBuilder.setLockId(mutation.getLockId());
    }
    mutateBuilder.setTimestamp(mutation.getTimeStamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        mutateBuilder.addAttribute(attributeBuilder.build());
      }
    }
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    for (Map.Entry<byte[],List<KeyValue>>
        family: mutation.getFamilyMap().entrySet()) {
      columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
      columnBuilder.clearQualifierValue();
      for (KeyValue value: family.getValue()) {
        valueBuilder.setQualifier(ByteString.copyFrom(value.getQualifier()));
        valueBuilder.setValue(ByteString.copyFrom(value.getValue()));
        valueBuilder.setTimestamp(value.getTimestamp());
        if (mutateType == MutateType.DELETE) {
          KeyValue.Type keyValueType = KeyValue.Type.codeToType(value.getType());
          valueBuilder.setDeleteType(toDeleteType(keyValueType));
        }
        columnBuilder.addQualifierValue(valueBuilder.build());
      }
      mutateBuilder.addColumnValue(columnBuilder.build());
    }
    return mutateBuilder.build();
  }

  /**
   * Convert a delete KeyValue type to protocol buffer DeleteType.
   *
   * @param type
   * @return
   * @throws IOException
   */
  private static DeleteType toDeleteType(
      KeyValue.Type type) throws IOException {
    switch (type) {
    case Delete:
      return DeleteType.DELETE_ONE_VERSION;
    case DeleteColumn:
      return DeleteType.DELETE_MULTIPLE_VERSIONS;
    case DeleteFamily:
      return DeleteType.DELETE_FAMILY;
      default:
        throw new IOException("Unknown delete type: " + type);
    }
  }
}
