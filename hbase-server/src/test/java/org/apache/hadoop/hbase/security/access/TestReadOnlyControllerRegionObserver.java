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

import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
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
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// Tests methods of Region Observer interface which are implemented in ReadOnlyController,
// by mocking the coprocessor environment and dependencies.
// V1 and V2 means version 1 and version 2 of the coprocessor method signature.
// For example, prePut has 2 versions:
// V1: prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit)
// V2: prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)

@Category({ SecurityTests.class, SmallTests.class })
public class TestReadOnlyControllerRegionObserver {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyControllerRegionObserver.class);

  RegionReadOnlyController regionReadOnlyController;
  HBaseConfiguration readOnlyConf;

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

  @Before
  public void setup() throws Exception {
    regionReadOnlyController = new RegionReadOnlyController();
    readOnlyConf = new HBaseConfiguration();
    readOnlyConf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);

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

  @After
  public void tearDown() throws Exception {

  }

  private void mockOperationForMetaTable() {
    when(regionInfo.getTable()).thenReturn(TableName.META_TABLE_NAME);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreFlushV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preFlush(c, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV1NoException() throws IOException {
    regionReadOnlyController.preFlush(c, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV1ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preFlush(c, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV1MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preFlush(c, flushLifeCycleTracker);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreFlushV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preFlush(c, store, scanner, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV2NoException() throws IOException {
    regionReadOnlyController.preFlush(c, store, scanner, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV2ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preFlush(c, store, scanner, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushV2MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preFlush(c, store, scanner, flushLifeCycleTracker);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreFlushScannerOpenReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preFlushScannerOpen(c, store, options, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushScannerOpenNoException() throws IOException {
    regionReadOnlyController.preFlushScannerOpen(c, store, options, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushScannerOpenReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preFlushScannerOpen(c, store, options, flushLifeCycleTracker);
  }

  @Test
  public void testPreFlushScannerOpenMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preFlushScannerOpen(c, store, options, flushLifeCycleTracker);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMemStoreCompactionReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preMemStoreCompaction(c, store);
  }

  @Test
  public void testPreMemStoreCompactionNoException() throws IOException {
    regionReadOnlyController.preMemStoreCompaction(c, store);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMemStoreCompactionCompactScannerOpenReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preMemStoreCompactionCompactScannerOpen(c, store, options);
  }

  @Test
  public void testPreMemStoreCompactionCompactScannerOpenNoException() throws IOException {
    regionReadOnlyController.preMemStoreCompactionCompactScannerOpen(c, store, options);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMemStoreCompactionCompactReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preMemStoreCompactionCompact(c, store, scanner);
  }

  @Test
  public void testPreMemStoreCompactionCompactNoException() throws IOException {
    regionReadOnlyController.preMemStoreCompactionCompact(c, store, scanner);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCompactSelectionReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCompactSelection(c, store, candidates, compactionLifeCycleTracker);
  }

  @Test
  public void testPreCompactSelectionNoException() throws IOException {
    regionReadOnlyController.preCompactSelection(c, store, candidates, compactionLifeCycleTracker);
  }

  @Test
  public void testPreCompactSelectionReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCompactSelection(c, store, candidates, compactionLifeCycleTracker);
  }

  @Test
  public void testPreCompactSelectionMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCompactSelection(c, store, candidates, compactionLifeCycleTracker);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCompactScannerOpenReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCompactScannerOpen(c, store, scanType, options,
      compactionLifeCycleTracker, compactionRequest);
  }

  @Test
  public void testPreCompactScannerOpenNoException() throws IOException {
    regionReadOnlyController.preCompactScannerOpen(c, store, scanType, options,
      compactionLifeCycleTracker, compactionRequest);
  }

  @Test
  public void testPreCompactScannerOpenReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCompactScannerOpen(c, store, scanType, options,
      compactionLifeCycleTracker, compactionRequest);
  }

  @Test
  public void testPreCompactScannerOpenMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCompactScannerOpen(c, store, scanType, options,
      compactionLifeCycleTracker, compactionRequest);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCompactReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCompact(c, store, scanner, scanType, compactionLifeCycleTracker,
      compactionRequest);
  }

  @Test
  public void testPreCompactNoException() throws IOException {
    regionReadOnlyController.preCompact(c, store, scanner, scanType, compactionLifeCycleTracker,
      compactionRequest);
  }

  @Test
  public void testPreCompactReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCompact(c, store, scanner, scanType, compactionLifeCycleTracker,
      compactionRequest);
  }

  @Test
  public void testPreCompactMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCompact(c, store, scanner, scanType, compactionLifeCycleTracker,
      compactionRequest);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPrePutV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.prePut(c, put, edit);
  }

  @Test
  public void testPrePutV1NoException() throws IOException {
    regionReadOnlyController.prePut(c, put, edit);
  }

  @Test
  public void testPrePutV1ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.prePut(c, put, edit);
  }

  @Test
  public void testPrePutV1MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.prePut(c, put, edit);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPrePutV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.prePut(c, put, edit, durability);
  }

  @Test
  public void testPrePutV2NoException() throws IOException {
    regionReadOnlyController.prePut(c, put, edit, durability);
  }

  @Test
  public void testPrePutV2ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.prePut(c, put, edit, durability);
  }

  @Test
  public void testPrePutV2MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.prePut(c, put, edit, durability);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreDeleteV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preDelete(c, delete, edit);
  }

  @Test
  public void testPreDeleteV1NoException() throws IOException {
    regionReadOnlyController.preDelete(c, delete, edit);
  }

  @Test
  public void testPreDeleteV1ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preDelete(c, delete, edit);
  }

  @Test
  public void testPreDeleteV1MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preDelete(c, delete, edit);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreDeleteV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preDelete(c, delete, edit, durability);
  }

  @Test
  public void testPreDeleteV2NoException() throws IOException {
    regionReadOnlyController.preDelete(c, delete, edit, durability);
  }

  @Test
  public void testPreDeleteV2ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preDelete(c, delete, edit, durability);
  }

  @Test
  public void testPreDeleteV2MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preDelete(c, delete, edit, durability);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreBatchMutateReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preBatchMutate(c, miniBatchOp);
  }

  @Test
  public void testPreBatchMutateNoException() throws IOException {
    regionReadOnlyController.preBatchMutate(c, miniBatchOp);
  }

  @Test
  public void testPreBatchMutateReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preBatchMutate(c, miniBatchOp);
  }

  @Test
  public void testPreBatchMutateMetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preBatchMutate(c, miniBatchOp);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndPutV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndPut(c, row, family, qualifier, op, comparator, put, result);
  }

  @Test
  public void testPreCheckAndPutV1NoException() throws IOException {
    regionReadOnlyController.preCheckAndPut(c, row, family, qualifier, op, comparator, put, result);
  }

  @Test
  public void testPreCheckAndPutV1ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPut(c, row, family, qualifier, op, comparator, put, result);
  }

  @Test
  public void testPreCheckAndPutV1MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPut(c, row, family, qualifier, op, comparator, put, result);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndPutV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndPut(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutV2NoException() throws IOException {
    regionReadOnlyController.preCheckAndPut(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutV2ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPut(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutV2MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPut(c, row, filter, put, result);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndPutAfterRowLockV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, family, qualifier, op, comparator,
      put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV1NoException() throws IOException {
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, family, qualifier, op, comparator,
      put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV1ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, family, qualifier, op, comparator,
      put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV1MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, family, qualifier, op, comparator,
      put, result);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndPutAfterRowLockV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV2NoException() throws IOException {
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV2ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Test
  public void testPreCheckAndPutAfterRowLockV2MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndDeleteV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Test
  public void testPreCheckAndDeleteV1NoException() throws IOException {
    regionReadOnlyController.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Test
  public void testPreCheckAndDeleteV1ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Test
  public void testPreCheckAndDeleteV1MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndDeleteV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteV2NoException() throws IOException {
    regionReadOnlyController.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteV2ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteV2MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndDeleteAfterRowLockV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV1NoException() throws IOException {
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV1ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV1MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndDeleteAfterRowLockV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV2NoException() throws IOException {
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV2ReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
  }

  @Test
  public void testPreCheckAndDeleteAfterRowLockV2MetaNoException() throws IOException {
    mockOperationForMetaTable();
    regionReadOnlyController.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndMutateReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndMutate(c, checkAndMutate, checkAndMutateResult);
  }

  @Test
  public void testPreCheckAndMutateNoException() throws IOException {
    regionReadOnlyController.preCheckAndMutate(c, checkAndMutate, checkAndMutateResult);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCheckAndMutateAfterRowLockReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCheckAndMutateAfterRowLock(c, checkAndMutate, checkAndMutateResult);
  }

  @Test
  public void testPreCheckAndMutateAfterRowLockNoException() throws IOException {
    regionReadOnlyController.preCheckAndMutateAfterRowLock(c, checkAndMutate, checkAndMutateResult);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreAppendV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preAppend(c, append);
  }

  @Test
  public void testPreAppendV1NoException() throws IOException {
    regionReadOnlyController.preAppend(c, append);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreAppendV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preAppend(c, append, edit);
  }

  @Test
  public void testPreAppendV2NoException() throws IOException {
    regionReadOnlyController.preAppend(c, append, edit);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreAppendAfterRowLockReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preAppendAfterRowLock(c, append);
  }

  @Test
  public void testPreAppendAfterRowLockNoException() throws IOException {
    regionReadOnlyController.preAppendAfterRowLock(c, append);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreIncrementV1ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preIncrement(c, increment);
  }

  @Test
  public void testPreIncrementV1NoException() throws IOException {
    regionReadOnlyController.preIncrement(c, increment);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreIncrementV2ReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preIncrement(c, increment, edit);
  }

  @Test
  public void testPreIncrementV2NoException() throws IOException {
    regionReadOnlyController.preIncrement(c, increment, edit);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreIncrementAfterRowLockReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preIncrementAfterRowLock(c, increment);
  }

  @Test
  public void testPreIncrementAfterRowLockNoException() throws IOException {
    regionReadOnlyController.preIncrementAfterRowLock(c, increment);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreReplayWALsReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preReplayWALs(ctx, info, edits);
  }

  @Test
  public void testPreReplayWALsNoException() throws IOException {
    regionReadOnlyController.preReplayWALs(ctx, info, edits);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreBulkLoadHFileReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preBulkLoadHFile(ctx, familyPaths);
  }

  @Test
  public void testPreBulkLoadHFileNoException() throws IOException {
    regionReadOnlyController.preBulkLoadHFile(ctx, familyPaths);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCommitStoreFileReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preCommitStoreFile(ctx, family, pairs);
  }

  @Test
  public void testPreCommitStoreFileNoException() throws IOException {
    regionReadOnlyController.preCommitStoreFile(ctx, family, pairs);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreWALAppendReadOnlyException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    regionReadOnlyController.preWALAppend(ctx, key, edit);
  }

  @Test
  public void testPreWALAppendNoException() throws IOException {
    regionReadOnlyController.preWALAppend(ctx, key, edit);
  }

  @Test
  public void testPreWALAppendReadOnlyMetaNoException() throws IOException {
    regionReadOnlyController.onConfigurationChange(readOnlyConf);
    when(key.getTableName()).thenReturn(TableName.META_TABLE_NAME);
    regionReadOnlyController.preWALAppend(ctx, key, edit);
  }

  @Test
  public void testPreWALAppendMetaNoException() throws IOException {
    when(key.getTableName()).thenReturn(TableName.META_TABLE_NAME);
    regionReadOnlyController.preWALAppend(ctx, key, edit);
  }
}
