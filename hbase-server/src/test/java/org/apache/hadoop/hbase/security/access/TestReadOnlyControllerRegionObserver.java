/*
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
package org.apache.hadoop.hbase.security.access;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.WriteAttemptedOnReadOnlyClusterException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

// Tests methods of Region Observer interface which are implemented in ReadOnlyController,
// by mocking the coprocessor environment and dependencies.
// V1 and V2 means version 1 and version 2 of the coprocessor method signature.
// For example, prePut has 2 versions:
// V1: prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit)
// V2: prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestReadOnlyControllerRegionObserver {

  RegionReadOnlyController regionReadOnlyController;

  // Region Coprocessor mocking variables
  ObserverContext<RegionCoprocessorEnvironment> c, ctx;
  RegionCoprocessorEnvironment env;
  RegionInfo regionInfo;
  Store store;
  InternalScanner scanner;
  ScanOptions options;
  FlushLifeCycleTracker flushLifeCycleTracker;
  List<StoreFile> candidates;
  CompactionLifeCycleTracker compactionLifeCycleTracker;
  ScanType scanType;
  CompactionRequest compactionRequest;
  TableName tableName;
  Put put;
  WALEdit edit;
  Durability durability;
  Delete delete;
  MiniBatchOperationInProgress<Mutation> miniBatchOp;
  byte[] row;
  byte[] family;
  byte[] qualifier;
  Filter filter;
  CompareOperator op;
  ByteArrayComparable comparator;
  boolean result;
  CheckAndMutate checkAndMutate;
  CheckAndMutateResult checkAndMutateResult;
  Append append;
  Increment increment;
  RegionInfo info;
  Path edits;
  List<Pair<byte[], String>> familyPaths;
  List<Pair<Path, Path>> pairs;
  WALKey key;

  @BeforeEach
  public void setup() throws Exception {
    regionReadOnlyController = new RegionReadOnlyController();

    // mocking variables initialization
    c = mock(ObserverContext.class);
    // ctx is created to make naming variable in sync with the Observer interface
    // methods where 'ctx' is used as the ObserverContext variable name instead of 'c'.
    // otherwise both are one and the same
    ctx = c;
    env = mock(RegionCoprocessorEnvironment.class);
    regionInfo = mock(RegionInfo.class);
    store = mock(Store.class);
    scanner = mock(InternalScanner.class);
    options = mock(ScanOptions.class);
    flushLifeCycleTracker = mock(FlushLifeCycleTracker.class);
    compactionLifeCycleTracker = mock(CompactionLifeCycleTracker.class);
    StoreFile sf1 = mock(StoreFile.class);
    StoreFile sf2 = mock(StoreFile.class);
    candidates = List.of(sf1, sf2);
    scanType = ScanType.COMPACT_DROP_DELETES;
    compactionRequest = mock(CompactionRequest.class);
    tableName = TableName.valueOf("testTable");
    put = mock(Put.class);
    edit = mock(WALEdit.class);
    durability = Durability.USE_DEFAULT;
    delete = mock(Delete.class);
    miniBatchOp = mock(MiniBatchOperationInProgress.class);
    row = Bytes.toBytes("test-row");
    family = Bytes.toBytes("test-family");
    qualifier = Bytes.toBytes("test-qualifier");
    filter = mock(Filter.class);
    op = CompareOperator.NO_OP;
    comparator = mock(ByteArrayComparable.class);
    result = false;
    checkAndMutate = CheckAndMutate
      .newBuilder(Bytes.toBytes("test-row")).ifEquals(Bytes.toBytes("test-family"),
        Bytes.toBytes("test-qualifier"), Bytes.toBytes("test-value"))
      .build(new Put(Bytes.toBytes("test-row")));
    checkAndMutateResult = mock(CheckAndMutateResult.class);
    append = mock(Append.class);
    increment = mock(Increment.class);
    edits = mock(Path.class);
    familyPaths = List.of(new Pair<>(Bytes.toBytes("test-family"), "/path/to/hfile1"),
      new Pair<>(Bytes.toBytes("test-family"), "/path/to/hfile2"));
    pairs = List.of(new Pair<>(mock(Path.class), mock(Path.class)),
      new Pair<>(mock(Path.class), mock(Path.class)));
    key = mock(WALKey.class);

    // Linking the mocks:
    when(c.getEnvironment()).thenReturn(env);
    when(env.getRegionInfo()).thenReturn(regionInfo);
    when(regionInfo.getTable()).thenReturn(tableName);
    when(key.getTableName()).thenReturn(tableName);
  }

  @AfterEach
  public void tearDown() throws Exception {

  }

  private void mockOperationForMetaTable() {
    when(regionInfo.getTable()).thenReturn(TableName.META_TABLE_NAME);
  }

  private void mockOperationMasterStoreTable() {
    when(regionInfo.getTable()).thenReturn(MasterRegionFactory.TABLE_NAME);
  }

  @Test
  public void testPreFlushV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preFlush(c, flushLifeCycleTracker);
    });
  }

  @Test
  public void testPreFlushV1ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preFlush(c, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV1ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preFlush(c, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preFlush(c, store, scanner, flushLifeCycleTracker);
    });
  }

  @Test
  public void testPreFlushV2ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preFlush(c, store, scanner, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV2ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preFlush(c, store, scanner, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushScannerOpenReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preFlushScannerOpen(c, store, options, flushLifeCycleTracker);
    });
  }

  @Test
  public void testPreFlushScannerOpenReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preFlushScannerOpen(c, store, options, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushScannerOpenReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preFlushScannerOpen(c, store, options, flushLifeCycleTracker);
  }

  @Test
  public void testPreMemStoreCompactionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preMemStoreCompaction(c, store);
    });
  }

  @Test
  public void testPreMemStoreCompactionCompactScannerOpenReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preMemStoreCompactionCompactScannerOpen(c, store, options);
    });
  }

  @Test
  public void testPreMemStoreCompactionCompactReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preMemStoreCompactionCompact(c, store, scanner);
    });
  }

  @Test
  public void testPreCompactSelectionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCompactSelection(c, store, candidates,
        compactionLifeCycleTracker);
    });
  }

  @Test
  public void testPreCompactSelectionReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCompactSelection(c, store, candidates, compactionLifeCycleTracker);
  }

  @Test
  public void testPreCompactSelectionReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCompactSelection(c, store, candidates, compactionLifeCycleTracker);
  }

  @Test
  public void testPreCompactScannerOpenReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCompactScannerOpen(c, store, scanType, options,
        compactionLifeCycleTracker, compactionRequest);
    });
  }

  @Test
  public void testPreCompactScannerOpenReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCompactScannerOpen(c, store, scanType, options,
      compactionLifeCycleTracker, compactionRequest);
  }

  @Test
  public void testPreCompactScannerOpenReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCompactScannerOpen(c, store, scanType, options,
      compactionLifeCycleTracker, compactionRequest);
  }

  @Test
  public void testPreCompactReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCompact(c, store, scanner, scanType, compactionLifeCycleTracker,
        compactionRequest);
    });
  }

  @Test
  public void testPreCompactReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCompact(c, store, scanner, scanType, compactionLifeCycleTracker,
      compactionRequest);
  }

  @Test
  public void testPreCompactReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCompact(c, store, scanner, scanType, compactionLifeCycleTracker,
      compactionRequest);
  }

  @Test
  public void testPrePutV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.prePut(c, put, edit);
    });
  }

  @Test
  public void testPrePutV1ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.prePut(c, put, edit);
  }

  @Test
  public void testPrePutV1ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.prePut(c, put, edit);
  }

  @Test
  public void testPrePutV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.prePut(c, put, edit, durability);
    });
  }

  @Test
  public void testPrePutV2ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.prePut(c, put, edit, durability);
  }

  @Test
  public void testPrePutV2ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.prePut(c, put, edit, durability);
  }

  @Test
  public void testPreDeleteV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preDelete(c, delete, edit);
    });
  }

  @Test
  public void testPreDeleteV1ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preDelete(c, delete, edit);
  }

  @Test
  public void testPreDeleteV1ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preDelete(c, delete, edit);
  }

  @Test
  public void testPreDeleteV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preDelete(c, delete, edit, durability);
    });
  }

  @Test
  public void testPreDeleteV2ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preDelete(c, delete, edit, durability);
  }

  @Test
  public void testPreDeleteV2ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preDelete(c, delete, edit, durability);
  }

  @Test
  public void testPreBatchMutateReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preBatchMutate(c, miniBatchOp);
    });
  }

  @Test
  public void testPreBatchMutateReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preBatchMutate(c, miniBatchOp);
  }

  @Test
  public void testPreBatchMutateReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preBatchMutate(c, miniBatchOp);
  }

  @Test
  public void testPreCheckAndPutV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndPut(c, row, family, qualifier, op, comparator, put,
        result);
    });
  }

  @Test
  public void testPreCheckAndPutV1ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPut(c, row, family, qualifier, op, comparator, put, result);
  }

  @Test
  public void testPreCheckAndPutV1ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCheckAndPut(c, row, family, qualifier, op, comparator, put, result);
  }

  @Test
  public void testPreCheckAndPutV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndPut(c, row, filter, put, result);
    });
  }

  @Test
  public void testPreCheckAndPutV2ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPut(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutV2ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCheckAndPut(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, family, qualifier, op, comparator,
        put, result);
    });
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV1ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, family, qualifier, op, comparator,
      put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV1ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, family, qualifier, op, comparator,
      put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, filter, put, result);
    });
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV2ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV2ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndDeleteV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
        result);
    });
  }

  @Test
  public void testPreCheckAndDeleteV1ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Test
  public void testPreCheckAndDeleteV1ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Test
  public void testPreCheckAndDeleteV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndDelete(c, row, filter, delete, result);
    });
  }

  @Test
  public void testPreCheckAndDeleteV2ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteV2ReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
        comparator, delete, result);
    });
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV1ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV1ReadOnlyMasterStoreNoException()
    throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
    });
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV2ReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV2ReadOnlyMasterStoreNoException()
    throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndMutateReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndMutate(c, checkAndMutate, checkAndMutateResult);
    });
  }

  @Test
  public void testPreCheckAndMutateAfterRowLockReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCheckAndMutateAfterRowLock(c, checkAndMutate,
        checkAndMutateResult);
    });
  }

  @Test
  public void testPreAppendV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preAppend(c, append);
    });
  }

  @Test
  public void testPreAppendV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preAppend(c, append, edit);
    });
  }

  @Test
  public void testPreAppendAfterRowLockReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preAppendAfterRowLock(c, append);
    });
  }

  @Test
  public void testPreIncrementV1ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preIncrement(c, increment);
    });
  }

  @Test
  public void testPreIncrementV2ReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preIncrement(c, increment, edit);
    });
  }

  @Test
  public void testPreIncrementAfterRowLockReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preIncrementAfterRowLock(c, increment);
    });
  }

  @Test
  public void testPreReplayWALsReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preReplayWALs(ctx, info, edits);
    });
  }

  @Test
  public void testPreReplayWALsReadOnlyMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preReplayWALs(ctx, info, edits);
  }

  @Test
  public void testPreReplayWALsReadOnlyMasterStoreNoException() throws IOException {
    mockOperationMasterStoreTable();
    regionReadOnlyController.preReplayWALs(ctx, info, edits);
  }

  @Test
  public void testPreBulkLoadHFileReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preBulkLoadHFile(ctx, familyPaths);
    });
  }

  @Test
  public void testPreCommitStoreFileReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preCommitStoreFile(ctx, family, pairs);
    });
  }

  @Test
  public void testPreWALAppendReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      regionReadOnlyController.preWALAppend(ctx, key, edit);
    });
  }

  @Test
  public void testPreWALAppendReadOnlyMetaNoException() throws IOException {
    when(key.getTableName()).thenReturn(TableName.META_TABLE_NAME);
    regionReadOnlyController.preWALAppend(ctx, key, edit);
  }

  @Test
  public void testPreWALAppendReadOnlyMasterStoreNoException() throws IOException {
    when(key.getTableName()).thenReturn(MasterRegionFactory.TABLE_NAME);
    regionReadOnlyController.preWALAppend(ctx, key, edit);
  }
}
