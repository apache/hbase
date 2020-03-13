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

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * A sample region observer that tests the RegionObserver interface.
 * It works with TestRegionObserverInterface to provide the test case.
 */
public class SimpleRegionObserver implements RegionCoprocessor, RegionObserver {

  final AtomicInteger ctBeforeDelete = new AtomicInteger(1);
  final AtomicInteger ctPreOpen = new AtomicInteger(0);
  final AtomicInteger ctPostOpen = new AtomicInteger(0);
  final AtomicInteger ctPreClose = new AtomicInteger(0);
  final AtomicInteger ctPostClose = new AtomicInteger(0);
  final AtomicInteger ctPreFlush = new AtomicInteger(0);
  final AtomicInteger ctPostFlush = new AtomicInteger(0);
  final AtomicInteger ctPreCompactSelect = new AtomicInteger(0);
  final AtomicInteger ctPostCompactSelect = new AtomicInteger(0);
  final AtomicInteger ctPreCompact = new AtomicInteger(0);
  final AtomicInteger ctPostCompact = new AtomicInteger(0);
  final AtomicInteger ctPreGet = new AtomicInteger(0);
  final AtomicInteger ctPostGet = new AtomicInteger(0);
  final AtomicInteger ctPrePut = new AtomicInteger(0);
  final AtomicInteger ctPostPut = new AtomicInteger(0);
  final AtomicInteger ctPreDeleted = new AtomicInteger(0);
  final AtomicInteger ctPrePrepareDeleteTS = new AtomicInteger(0);
  final AtomicInteger ctPostDeleted = new AtomicInteger(0);
  final AtomicInteger ctPreIncrement = new AtomicInteger(0);
  final AtomicInteger ctPreIncrementAfterRowLock = new AtomicInteger(0);
  final AtomicInteger ctPreAppend = new AtomicInteger(0);
  final AtomicInteger ctPreAppendAfterRowLock = new AtomicInteger(0);
  final AtomicInteger ctPostIncrement = new AtomicInteger(0);
  final AtomicInteger ctPostAppend = new AtomicInteger(0);
  final AtomicInteger ctPreCheckAndPut = new AtomicInteger(0);
  final AtomicInteger ctPreCheckAndPutWithFilter = new AtomicInteger(0);
  final AtomicInteger ctPreCheckAndPutAfterRowLock = new AtomicInteger(0);
  final AtomicInteger ctPreCheckAndPutWithFilterAfterRowLock = new AtomicInteger(0);
  final AtomicInteger ctPostCheckAndPut = new AtomicInteger(0);
  final AtomicInteger ctPostCheckAndPutWithFilter = new AtomicInteger(0);
  final AtomicInteger ctPreCheckAndDelete = new AtomicInteger(0);
  final AtomicInteger ctPreCheckAndDeleteWithFilter = new AtomicInteger(0);
  final AtomicInteger ctPreCheckAndDeleteAfterRowLock = new AtomicInteger(0);
  final AtomicInteger ctPreCheckAndDeleteWithFilterAfterRowLock = new AtomicInteger(0);
  final AtomicInteger ctPostCheckAndDelete = new AtomicInteger(0);
  final AtomicInteger ctPostCheckAndDeleteWithFilter = new AtomicInteger(0);
  final AtomicInteger ctPreScannerNext = new AtomicInteger(0);
  final AtomicInteger ctPostScannerNext = new AtomicInteger(0);
  final AtomicInteger ctPostScannerFilterRow = new AtomicInteger(0);
  final AtomicInteger ctPreScannerClose = new AtomicInteger(0);
  final AtomicInteger ctPostScannerClose = new AtomicInteger(0);
  final AtomicInteger ctPreScannerOpen = new AtomicInteger(0);
  final AtomicInteger ctPostScannerOpen = new AtomicInteger(0);
  final AtomicInteger ctPreBulkLoadHFile = new AtomicInteger(0);
  final AtomicInteger ctPostBulkLoadHFile = new AtomicInteger(0);
  final AtomicInteger ctPreBatchMutate = new AtomicInteger(0);
  final AtomicInteger ctPostBatchMutate = new AtomicInteger(0);
  final AtomicInteger ctPreReplayWALs = new AtomicInteger(0);
  final AtomicInteger ctPostReplayWALs = new AtomicInteger(0);
  final AtomicInteger ctPreWALRestore = new AtomicInteger(0);
  final AtomicInteger ctPostWALRestore = new AtomicInteger(0);
  final AtomicInteger ctPreStoreFileReaderOpen = new AtomicInteger(0);
  final AtomicInteger ctPostStoreFileReaderOpen = new AtomicInteger(0);
  final AtomicInteger ctPostBatchMutateIndispensably = new AtomicInteger(0);
  final AtomicInteger ctPostStartRegionOperation = new AtomicInteger(0);
  final AtomicInteger ctPostCloseRegionOperation = new AtomicInteger(0);
  final AtomicBoolean throwOnPostFlush = new AtomicBoolean(false);
  final AtomicInteger ctPreWALAppend = new AtomicInteger(0);

  static final String TABLE_SKIPPED = "SKIPPED_BY_PREWALRESTORE";
  Map<String, byte[]> extendedAttributes = new HashMap<String,byte[]>();
  static final byte[] WAL_EXTENDED_ATTRIBUTE_BYTES = Bytes.toBytes("foo");

  public void setThrowOnPostFlush(Boolean val){
    throwOnPostFlush.set(val);
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
  }

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    ctPreOpen.incrementAndGet();
  }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    ctPostOpen.incrementAndGet();
  }

  public boolean wasOpened() {
    return ctPreOpen.get() > 0 && ctPostOpen.get() > 0;
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
    ctPreClose.incrementAndGet();
  }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
    ctPostClose.incrementAndGet();
  }

  public boolean wasClosed() {
    return ctPreClose.get() > 0 && ctPostClose.get() > 0;
  }

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
    ctPreFlush.incrementAndGet();
    return scanner;
  }

  @Override
  public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, StoreFile resultFile, FlushLifeCycleTracker tracker) throws IOException {
    ctPostFlush.incrementAndGet();
    if (throwOnPostFlush.get()){
      throw new IOException("throwOnPostFlush is true in postFlush");
    }
  }

  public boolean wasFlushed() {
    return ctPreFlush.get() > 0 && ctPostFlush.get() > 0;
  }

  @Override
  public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      List<? extends StoreFile> candidates, CompactionLifeCycleTracker tracker) throws IOException {
    ctPreCompactSelect.incrementAndGet();
  }

  @Override
  public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      List<? extends StoreFile> selected, CompactionLifeCycleTracker tracker,
      CompactionRequest request) {
    ctPostCompactSelect.incrementAndGet();
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
      CompactionRequest request) throws IOException {
    ctPreCompact.incrementAndGet();
    return scanner;
  }

  @Override
  public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      StoreFile resultFile, CompactionLifeCycleTracker tracker,
      CompactionRequest request) throws IOException {
    ctPostCompact.incrementAndGet();
  }

  public boolean wasCompacted() {
    return ctPreCompact.get() > 0 && ctPostCompact.get() > 0;
  }

  @Override
  public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan)
      throws IOException {
    ctPreScannerOpen.incrementAndGet();
  }

  @Override
  public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s)
      throws IOException {
    ctPostScannerOpen.incrementAndGet();
    return s;
  }

  @Override
  public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> results,
      final int limit, final boolean hasMore) throws IOException {
    ctPreScannerNext.incrementAndGet();
    return hasMore;
  }

  @Override
  public boolean postScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> results, final int limit,
      final boolean hasMore) throws IOException {
    ctPostScannerNext.incrementAndGet();
    return hasMore;
  }

  @Override
  public boolean postScannerFilterRow(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final Cell currentRow, final boolean hasMore) throws IOException {
    ctPostScannerFilterRow.incrementAndGet();
    return hasMore;
  }

  @Override
  public void preScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    ctPreScannerClose.incrementAndGet();
  }

  @Override
  public void postScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    ctPostScannerClose.incrementAndGet();
  }

  @Override
  public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
      final List<Cell> results) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(get);
    assertNotNull(results);
    ctPreGet.incrementAndGet();
  }

  @Override
  public void postGetOp(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
      final List<Cell> results) {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(get);
    assertNotNull(results);
    if (e.getRegion().getTableDescriptor().getTableName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      boolean foundA = false;
      boolean foundB = false;
      boolean foundC = false;
      for (Cell kv: results) {
        if (CellUtil.matchingFamily(kv, TestRegionObserverInterface.A)) {
          foundA = true;
        }
        if (CellUtil.matchingFamily(kv, TestRegionObserverInterface.B)) {
          foundB = true;
        }
        if (CellUtil.matchingFamily(kv, TestRegionObserverInterface.C)) {
          foundC = true;
        }
      }
      assertTrue(foundA);
      assertTrue(foundB);
      assertTrue(foundC);
    }
    ctPostGet.incrementAndGet();
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit,
      final Durability durability) throws IOException {
    Map<byte[], List<Cell>> familyMap  = put.getFamilyCellMap();
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
    if (e.getRegion().getTableDescriptor().getTableName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      List<Cell> cells = familyMap.get(TestRegionObserverInterface.A);
      assertNotNull(cells);
      assertNotNull(cells.get(0));
      Cell cell = cells.get(0);
      assertTrue(Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength(), TestRegionObserverInterface.A, 0,
        TestRegionObserverInterface.A.length));
      cells = familyMap.get(TestRegionObserverInterface.B);
      assertNotNull(cells);
      assertNotNull(cells.get(0));
      cell = cells.get(0);
      assertTrue(Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength(), TestRegionObserverInterface.B, 0,
        TestRegionObserverInterface.B.length));
      cells = familyMap.get(TestRegionObserverInterface.C);
      assertNotNull(cells);
      assertNotNull(cells.get(0));
      cell = cells.get(0);
      assertTrue(Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength(), TestRegionObserverInterface.C, 0,
        TestRegionObserverInterface.C.length));
    }
    ctPrePut.incrementAndGet();
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit,
      final Durability durability) throws IOException {
    Map<byte[], List<Cell>> familyMap  = put.getFamilyCellMap();
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
    List<Cell> cells = familyMap.get(TestRegionObserverInterface.A);
    if (e.getRegion().getTableDescriptor().getTableName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      assertNotNull(cells);
      assertNotNull(cells.get(0));
      // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
      Cell cell = cells.get(0);
      assertTrue(Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength(), TestRegionObserverInterface.A, 0,
        TestRegionObserverInterface.A.length));
      cells = familyMap.get(TestRegionObserverInterface.B);
      assertNotNull(cells);
      assertNotNull(cells.get(0));
      // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
      cell = cells.get(0);
      assertTrue(Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength(), TestRegionObserverInterface.B, 0,
        TestRegionObserverInterface.B.length));
      cells = familyMap.get(TestRegionObserverInterface.C);
      assertNotNull(cells);
      assertNotNull(cells.get(0));
      // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
      cell = cells.get(0);
      assertTrue(Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength(), TestRegionObserverInterface.C, 0,
        TestRegionObserverInterface.C.length));
    }
    ctPostPut.incrementAndGet();
  }

  @Override
  public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit,
      final Durability durability) throws IOException {
    Map<byte[], List<Cell>> familyMap  = delete.getFamilyCellMap();
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
    if (ctBeforeDelete.get() > 0) {
      ctPreDeleted.incrementAndGet();
    }
  }

  @Override
  public void prePrepareTimeStampForDeleteVersion(ObserverContext<RegionCoprocessorEnvironment> e,
      Mutation delete, Cell cell, byte[] byteNow, Get get) throws IOException {
    ctPrePrepareDeleteTS.incrementAndGet();
  }

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit,
      final Durability durability) throws IOException {
    Map<byte[], List<Cell>> familyMap  = delete.getFamilyCellMap();
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
    ctBeforeDelete.set(0);
    ctPostDeleted.incrementAndGet();
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(miniBatchOp);
    ctPreBatchMutate.incrementAndGet();
  }

  @Override
  public void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(miniBatchOp);
    ctPostBatchMutate.incrementAndGet();
  }

  @Override
  public void postStartRegionOperation(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      Operation op) throws IOException {
    ctPostStartRegionOperation.incrementAndGet();
  }

  @Override
  public void postCloseRegionOperation(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      Operation op) throws IOException {
    if (ctPostStartRegionOperation.get() > 0) {
      ctPostCloseRegionOperation.incrementAndGet();
    }
  }

  @Override
  public void postBatchMutateIndispensably(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      MiniBatchOperationInProgress<Mutation> miniBatchOp, final boolean success) throws IOException {
    ctPostBatchMutateIndispensably.incrementAndGet();
  }

  @Override
  public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment) throws IOException {
    ctPreIncrement.incrementAndGet();
    return null;
  }

  @Override
  public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> e,
      Increment increment) throws IOException {
    ctPreIncrementAfterRowLock.incrementAndGet();
    return null;
  }

  @Override
  public Result postIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment, final Result result) throws IOException {
    ctPostIncrement.incrementAndGet();
    return result;
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
                                byte[] family, byte[] qualifier, CompareOperator compareOp, ByteArrayComparable comparator,
                                Put put, boolean result) throws IOException {
    ctPreCheckAndPut.incrementAndGet();
    return true;
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
    Filter filter, Put put, boolean result) throws IOException {
    ctPreCheckAndPutWithFilter.incrementAndGet();
    return true;
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> e,
      byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp,
      ByteArrayComparable comparator, Put put, boolean result) throws IOException {
    ctPreCheckAndPutAfterRowLock.incrementAndGet();
    return true;
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
    byte[] row, Filter filter, Put put, boolean result) throws IOException {
    ctPreCheckAndPutWithFilterAfterRowLock.incrementAndGet();
    return true;
  }

  @Override
  public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
                                 byte[] family, byte[] qualifier, CompareOperator compareOp, ByteArrayComparable comparator,
                                 Put put, boolean result) throws IOException {
    ctPostCheckAndPut.incrementAndGet();
    return true;
  }

  @Override
  public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
    Filter filter, Put put, boolean result) throws IOException {
    ctPostCheckAndPutWithFilter.incrementAndGet();
    return true;
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
                                   byte[] family, byte[] qualifier, CompareOperator compareOp, ByteArrayComparable comparator,
                                   Delete delete, boolean result) throws IOException {
    ctPreCheckAndDelete.incrementAndGet();
    return true;
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
    Filter filter, Delete delete, boolean result) throws IOException {
    ctPreCheckAndDeleteWithFilter.incrementAndGet();
    return true;
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> e,
      byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp,
      ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
    ctPreCheckAndDeleteAfterRowLock.incrementAndGet();
    return true;
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
    byte[] row, Filter filter, Delete delete, boolean result) throws IOException {
    ctPreCheckAndDeleteWithFilterAfterRowLock.incrementAndGet();
    return true;
  }

  @Override
  public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
                                    byte[] family, byte[] qualifier, CompareOperator compareOp, ByteArrayComparable comparator,
                                    Delete delete, boolean result) throws IOException {
    ctPostCheckAndDelete.incrementAndGet();
    return true;
  }

  @Override
  public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
    Filter filter, Delete delete, boolean result) throws IOException {
    ctPostCheckAndDeleteWithFilter.incrementAndGet();
    return true;
  }

  @Override
  public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> e,
      Append append) throws IOException {
    ctPreAppendAfterRowLock.incrementAndGet();
    return null;
  }

  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append)
      throws IOException {
    ctPreAppend.incrementAndGet();
    return null;
  }

  @Override
  public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append,
      Result result) throws IOException {
    ctPostAppend.incrementAndGet();
    return null;
  }

  @Override
  public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
                               List<Pair<byte[], String>> familyPaths) throws IOException {
    RegionCoprocessorEnvironment e = ctx.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    if (e.getRegion().getTableDescriptor().getTableName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      assertNotNull(familyPaths);
      assertEquals(1,familyPaths.size());
      assertArrayEquals(TestRegionObserverInterface.A, familyPaths.get(0).getFirst());
      String familyPath = familyPaths.get(0).getSecond();
      String familyName = Bytes.toString(TestRegionObserverInterface.A);
      assertEquals(familyPath.substring(familyPath.length()-familyName.length()-1),"/"+familyName);
    }
    ctPreBulkLoadHFile.incrementAndGet();
  }

  @Override
  public void postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Pair<byte[], String>> familyPaths, Map<byte[], List<Path>> map)
          throws IOException {
    RegionCoprocessorEnvironment e = ctx.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    if (e.getRegion().getTableDescriptor().getTableName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      assertNotNull(familyPaths);
      assertEquals(1,familyPaths.size());
      assertArrayEquals(TestRegionObserverInterface.A, familyPaths.get(0).getFirst());
      String familyPath = familyPaths.get(0).getSecond();
      String familyName = Bytes.toString(TestRegionObserverInterface.A);
      assertEquals(familyPath.substring(familyPath.length()-familyName.length()-1),"/"+familyName);
    }
    ctPostBulkLoadHFile.incrementAndGet();
  }

  @Override
  public void preReplayWALs(ObserverContext<? extends RegionCoprocessorEnvironment> env,
      RegionInfo info, Path edits) throws IOException {
    ctPreReplayWALs.incrementAndGet();
  }

  @Override
  public void postReplayWALs(ObserverContext<? extends RegionCoprocessorEnvironment> env,
      RegionInfo info, Path edits) throws IOException {
    ctPostReplayWALs.incrementAndGet();
  }

  @Override
  public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env,
      RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    String tableName = logKey.getTableName().getNameAsString();
    if (tableName.equals(TABLE_SKIPPED)) {
      // skip recovery of TABLE_SKIPPED for testing purpose
      env.bypass();
      return;
    }
    ctPreWALRestore.incrementAndGet();
  }

  @Override
  public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env,
                             RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    ctPostWALRestore.incrementAndGet();
  }

  @Override
  public StoreFileReader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
      FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
      Reference r, StoreFileReader reader) throws IOException {
    ctPreStoreFileReaderOpen.incrementAndGet();
    return null;
  }

  @Override
  public StoreFileReader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
      FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
      Reference r, StoreFileReader reader) throws IOException {
    ctPostStoreFileReaderOpen.incrementAndGet();
    return reader;
  }

  @Override
  public void preWALAppend(ObserverContext<RegionCoprocessorEnvironment> ctx,
                                 WALKey key, WALEdit edit) throws IOException {
    ctPreWALAppend.incrementAndGet();

    key.addExtendedAttribute(Integer.toString(ctPreWALAppend.get()),
        Bytes.toBytes("foo"));
  }

  public boolean hadPreGet() {
    return ctPreGet.get() > 0;
  }

  public boolean hadPostGet() {
    return ctPostGet.get() > 0;
  }

  public boolean hadPrePut() {
    return ctPrePut.get() > 0;
  }

  public boolean hadPostPut() {
    return ctPostPut.get() > 0;
  }

  public boolean hadPreBatchMutate() {
    return ctPreBatchMutate.get() > 0;
  }

  public boolean hadPostBatchMutate() {
    return ctPostBatchMutate.get() > 0;
  }

  public boolean hadPostBatchMutateIndispensably() {
    return ctPostBatchMutateIndispensably.get() > 0;
  }

  public boolean hadPostStartRegionOperation() {
    return ctPostStartRegionOperation.get() > 0;
  }

  public boolean hadPostCloseRegionOperation() {
    return ctPostCloseRegionOperation.get() > 0;
  }

  public boolean hadDelete() {
    return !(ctBeforeDelete.get() > 0);
  }

  public int getCtPostStartRegionOperation() {
    return ctPostStartRegionOperation.get();
  }

  public int getCtPostCloseRegionOperation() {
    return ctPostCloseRegionOperation.get();
  }

  public int getPreCheckAndPut() {
    return ctPreCheckAndPut.get();
  }

  public int getPreCheckAndPutWithFilter() {
    return ctPreCheckAndPutWithFilter.get();
  }

  public int getPreCheckAndPutAfterRowLock() {
    return ctPreCheckAndPutAfterRowLock.get();
  }

  public int getPreCheckAndPutWithFilterAfterRowLock() {
    return ctPreCheckAndPutWithFilterAfterRowLock.get();
  }

  public int getPostCheckAndPut() {
    return ctPostCheckAndPut.get();
  }

  public int getPostCheckAndPutWithFilter() {
    return ctPostCheckAndPutWithFilter.get();
  }

  public int getPreCheckAndDelete() {
    return ctPreCheckAndDelete.get();
  }

  public int getPreCheckAndDeleteWithFilter() {
    return ctPreCheckAndDeleteWithFilter.get();
  }

  public int getPreCheckAndDeleteAfterRowLock() {
    return ctPreCheckAndDeleteAfterRowLock.get();
  }

  public int getPreCheckAndDeleteWithFilterAfterRowLock() {
    return ctPreCheckAndDeleteWithFilterAfterRowLock.get();
  }

  public int getPostCheckAndDelete() {
    return ctPostCheckAndDelete.get();
  }

  public int getPostCheckAndDeleteWithFilter() {
    return ctPostCheckAndDeleteWithFilter.get();
  }

  public boolean hadPreIncrement() {
    return ctPreIncrement.get() > 0;
  }

  public boolean hadPreIncrementAfterRowLock() {
    return ctPreIncrementAfterRowLock.get() > 0;
  }

  public boolean hadPostIncrement() {
    return ctPostIncrement.get() > 0;
  }

  public boolean hadPreAppend() {
    return ctPreAppend.get() > 0;
  }

  public boolean hadPreAppendAfterRowLock() {
    return ctPreAppendAfterRowLock.get() > 0;
  }

  public boolean hadPostAppend() {
    return ctPostAppend.get() > 0;
  }

  public boolean hadPrePreparedDeleteTS() {
    return ctPrePrepareDeleteTS.get() > 0;
  }

  public boolean hadPreReplayWALs() {
    return ctPreReplayWALs.get() > 0;
  }

  public boolean hadPostReplayWALs() {
    return ctPostReplayWALs.get() > 0;
  }

  public boolean hadPreWALRestore() {
    return ctPreWALRestore.get() > 0;
  }

  public boolean hadPostWALRestore() {
    return ctPostWALRestore.get() > 0;
  }

  public boolean wasScannerNextCalled() {
    return ctPreScannerNext.get() > 0 && ctPostScannerNext.get() > 0;
  }
  public boolean wasScannerFilterRowCalled() {
    return ctPostScannerFilterRow.get() > 0;
  }
  public boolean wasScannerCloseCalled() {
    return ctPreScannerClose.get() > 0 && ctPostScannerClose.get() > 0;
  }
  public boolean wasScannerOpenCalled() {
    return ctPreScannerOpen.get() > 0 && ctPostScannerOpen.get() > 0;
  }
  public boolean hadDeleted() {
    return ctPreDeleted.get() > 0 && ctPostDeleted.get() > 0;
  }

  public boolean hadPostBulkLoadHFile() {
    return ctPostBulkLoadHFile.get() > 0;
  }

  public boolean hadPreBulkLoadHFile() {
    return ctPreBulkLoadHFile.get() > 0;
  }


  public int getCtBeforeDelete() {
    return ctBeforeDelete.get();
  }

  public int getCtPreOpen() {
    return ctPreOpen.get();
  }

  public int getCtPostOpen() {
    return ctPostOpen.get();
  }

  public int getCtPreClose() {
    return ctPreClose.get();
  }

  public int getCtPostClose() {
    return ctPostClose.get();
  }

  public int getCtPreFlush() {
    return ctPreFlush.get();
  }

  public int getCtPostFlush() {
    return ctPostFlush.get();
  }

  public int getCtPreCompactSelect() {
    return ctPreCompactSelect.get();
  }

  public int getCtPostCompactSelect() {
    return ctPostCompactSelect.get();
  }

  public int getCtPreCompact() {
    return ctPreCompact.get();
  }

  public int getCtPostCompact() {
    return ctPostCompact.get();
  }

  public int getCtPreGet() {
    return ctPreGet.get();
  }

  public int getCtPostGet() {
    return ctPostGet.get();
  }

  public int getCtPrePut() {
    return ctPrePut.get();
  }

  public int getCtPostPut() {
    return ctPostPut.get();
  }

  public int getCtPreDeleted() {
    return ctPreDeleted.get();
  }

  public int getCtPostDeleted() {
    return ctPostDeleted.get();
  }

  public int getCtPreIncrement() {
    return ctPreIncrement.get();
  }

  public int getCtPostIncrement() {
    return ctPostIncrement.get();
  }

  public int getCtPreReplayWALs() {
    return ctPreReplayWALs.get();
  }

  public int getCtPostReplayWALs() {
    return ctPostReplayWALs.get();
  }

  public int getCtPreWALRestore() {
    return ctPreWALRestore.get();
  }

  public int getCtPostWALRestore() {
    return ctPostWALRestore.get();
  }

  public int getCtPreWALAppend() {
    return ctPreWALAppend.get();
  }

  public boolean wasStoreFileReaderOpenCalled() {
    return ctPreStoreFileReaderOpen.get() > 0 && ctPostStoreFileReaderOpen.get() > 0;
  }
}
