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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * compacted memstore test case
 */
@Category({RegionServerTests.class, LargeTests.class})
@RunWith(Parameterized.class)
public class TestCompactingToCellFlatMapMemStore extends TestCompactingMemStore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactingToCellFlatMapMemStore.class);

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] { "CHUNK_MAP", "ARRAY_MAP" }; // test different immutable indexes
  }
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCompactingToCellFlatMapMemStore.class);
  public final boolean toCellChunkMap;
  Configuration conf;
  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  public TestCompactingToCellFlatMapMemStore(String type){
    if (type == "CHUNK_MAP") {
      toCellChunkMap = true;
    } else {
      toCellChunkMap = false;
    }
  }

  @Override public void tearDown() throws Exception {
    chunkCreator.clearChunksInPool();
  }

  @Override public void setUp() throws Exception {

    compactingSetUp();
    this.conf = HBaseConfiguration.create();

    // set memstore to do data compaction
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(MemoryCompactionPolicy.EAGER));
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.02);
    this.memstore =
        new MyCompactingMemStore(conf, CellComparatorImpl.COMPARATOR, store,
            regionServicesForStores, MemoryCompactionPolicy.EAGER);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction tests
  //////////////////////////////////////////////////////////////////////////////
  @Override
  public void testCompaction1Bucket() throws IOException {
    int counter = 0;
    String[] keys1 = { "A", "A", "B", "C" }; //A1, A2, B3, C4
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);
    } else {
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.ARRAY_MAP);
    }

    // test 1 bucket
    long totalCellsLen = addRowsByKeysDataSize(memstore, keys1);
    long cellBeforeFlushSize = cellBeforeFlushSize();
    long cellAfterFlushSize  = cellAfterFlushSize();
    long totalHeapSize = MutableSegment.DEEP_OVERHEAD + 4 * cellBeforeFlushSize;

    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());

    assertEquals(4, memstore.getActive().getCellsCount());
    ((CompactingMemStore) memstore).flushInMemory();    // push keys to pipeline and compact
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // One cell is duplicated and the compaction will remove it. All cells of same size so adjusting
    // totalCellsLen
    totalCellsLen = (totalCellsLen * 3) / 4;
    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());

    totalHeapSize =
        3 * cellAfterFlushSize + MutableSegment.DEEP_OVERHEAD
            + (toCellChunkMap ?
            CellChunkImmutableSegment.DEEP_OVERHEAD_CCM :
            CellArrayImmutableSegment.DEEP_OVERHEAD_CAM);
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(3, counter);
    MemStoreSize mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.decrMemStoreSize(mss);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(3, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  @Override
  public void testCompaction2Buckets() throws IOException {
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);
    } else {
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.ARRAY_MAP);
    }
    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };

    long totalCellsLen1 = addRowsByKeysDataSize(memstore, keys1);     // INSERT 4
    long cellBeforeFlushSize = cellBeforeFlushSize();
    long cellAfterFlushSize = cellAfterFlushSize();
    long totalHeapSize1 = MutableSegment.DEEP_OVERHEAD + 4 * cellBeforeFlushSize;
    assertEquals(totalCellsLen1, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1, ((CompactingMemStore) memstore).heapSize());

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    int counter = 0;                                          // COMPACT 4->3
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(3,counter);
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // One cell is duplicated and the compaction will remove it. All cells of same size so adjusting
    // totalCellsLen
    totalCellsLen1 = (totalCellsLen1 * 3) / 4;
    totalHeapSize1 = 3 * cellAfterFlushSize + MutableSegment.DEEP_OVERHEAD
        + (toCellChunkMap ?
        CellChunkImmutableSegment.DEEP_OVERHEAD_CCM :
        CellArrayImmutableSegment.DEEP_OVERHEAD_CAM);
    assertEquals(totalCellsLen1, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1, ((CompactingMemStore) memstore).heapSize());

    long totalCellsLen2 = addRowsByKeysDataSize(memstore, keys2);   // INSERT 3 (3+3=6)
    long totalHeapSize2 = 3 * cellBeforeFlushSize;
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1 + totalHeapSize2, ((CompactingMemStore) memstore).heapSize());

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    assertEquals(0, memstore.getSnapshot().getCellsCount());// COMPACT 6->4
    counter = 0;
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(4,counter);
    totalCellsLen2 = totalCellsLen2 / 3;// 2 cells duplicated in set 2
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    totalHeapSize2 = 1 * cellAfterFlushSize;
    assertEquals(totalHeapSize1 + totalHeapSize2, ((CompactingMemStore) memstore).heapSize());

    MemStoreSize mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    // simulate flusher
    region.decrMemStoreSize(mss);
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  @Override
  public void testCompaction3Buckets() throws IOException {
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);
    } else {
      // set to CellArrayMap as CCM is configured by default due to MSLAB usage
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.ARRAY_MAP);
    }
    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };
    String[] keys3 = { "D", "B", "B" };

    long totalCellsLen1 = addRowsByKeysDataSize(memstore, keys1);
    long cellBeforeFlushSize = cellBeforeFlushSize();
    long cellAfterFlushSize = cellAfterFlushSize();
    long totalHeapSize1 = MutableSegment.DEEP_OVERHEAD + 4 * cellBeforeFlushSize;
    assertEquals(totalCellsLen1, region.getMemStoreDataSize());
    assertEquals(totalHeapSize1, ((CompactingMemStore) memstore).heapSize());

    MemStoreSize mss = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact

    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // One cell is duplicated and the compaction will remove it. All cells of same size so adjusting
    // totalCellsLen
    totalCellsLen1 = (totalCellsLen1 * 3) / 4;
    totalHeapSize1 = 3 * cellAfterFlushSize + MutableSegment.DEEP_OVERHEAD
        + (toCellChunkMap ?
        CellChunkImmutableSegment.DEEP_OVERHEAD_CCM :
        CellArrayImmutableSegment.DEEP_OVERHEAD_CAM);
    assertEquals(totalCellsLen1, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1, ((CompactingMemStore) memstore).heapSize());

    long totalCellsLen2 = addRowsByKeysDataSize(memstore, keys2);
    long totalHeapSize2 = 3 * cellBeforeFlushSize;

    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1 + totalHeapSize2, ((CompactingMemStore) memstore).heapSize());

    ((MyCompactingMemStore) memstore).disableCompaction();
    mss = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline without compaction
    totalHeapSize2 = totalHeapSize2 + CSLMImmutableSegment.DEEP_OVERHEAD_CSLM;
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1 + totalHeapSize2, ((CompactingMemStore) memstore).heapSize());

    long totalCellsLen3 = addRowsByKeysDataSize(memstore, keys3);
    long totalHeapSize3 = 3 * cellBeforeFlushSize;
    assertEquals(totalCellsLen1 + totalCellsLen2 + totalCellsLen3,
        regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1 + totalHeapSize2 + totalHeapSize3,
        ((CompactingMemStore) memstore).heapSize());

    ((MyCompactingMemStore) memstore).enableCompaction();
    mss = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // active flushed to pipeline and all 3 segments compacted. Will get rid of duplicated cells.
    // Out of total 10, only 4 cells are unique
    totalCellsLen2 = totalCellsLen2 / 3;// 2 out of 3 cells are duplicated
    totalCellsLen3 = 0;// All duplicated cells.
    assertEquals(totalCellsLen1 + totalCellsLen2 + totalCellsLen3,
        regionServicesForStores.getMemStoreSize());
    // Only 4 unique cells left
    long totalHeapSize4 = 4 * cellAfterFlushSize + MutableSegment.DEEP_OVERHEAD
        + (toCellChunkMap ?
        CellChunkImmutableSegment.DEEP_OVERHEAD_CCM :
        CellArrayImmutableSegment.DEEP_OVERHEAD_CAM);
    assertEquals(totalHeapSize4, ((CompactingMemStore) memstore).heapSize());

    mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    // simulate flusher
    region.decrMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(),
      mss.getCellsCount());
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  //////////////////////////////////////////////////////////////////////////////
  // Merging tests
  //////////////////////////////////////////////////////////////////////////////
  @Test
  public void testMerging() throws IOException {
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);
    }
    String[] keys1 = { "A", "A", "B", "C", "F", "H"};
    String[] keys2 = { "A", "B", "D", "G", "I", "J"};
    String[] keys3 = { "D", "B", "B", "E" };

    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((MyCompactingMemStore)memstore).initiateType(compactionType, memstore.getConfiguration());
    addRowsByKeysDataSize(memstore, keys1);

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline should not compact

    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    addRowsByKeysDataSize(memstore, keys2); // also should only flatten

    int counter2 = 0;
    for ( Segment s : memstore.getSegments()) {
      counter2 += s.getCellsCount();
    }
    assertEquals(12, counter2);

    ((MyCompactingMemStore) memstore).disableCompaction();

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline without flattening
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    int counter3 = 0;
    for ( Segment s : memstore.getSegments()) {
      counter3 += s.getCellsCount();
    }
    assertEquals(12, counter3);

    addRowsByKeysDataSize(memstore, keys3);

    int counter4 = 0;
    for ( Segment s : memstore.getSegments()) {
      counter4 += s.getCellsCount();
    }
    assertEquals(16, counter4);

    ((MyCompactingMemStore) memstore).enableCompaction();


    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    int counter = 0;
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(16,counter);

    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    ImmutableSegment s = memstore.getSnapshot();
    memstore.clearSnapshot(snapshot.getId());
  }

  @Test
  public void testTimeRangeAfterCompaction() throws IOException {
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);
    }
    testTimeRange(true);
  }

  @Test
  public void testTimeRangeAfterMerge() throws IOException {
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);
    }
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((MyCompactingMemStore)memstore).initiateType(compactionType, memstore.getConfiguration());
    testTimeRange(false);
  }

  private void testTimeRange(boolean isCompaction) throws IOException {
    final long initTs = 100;
    long currentTs = initTs;
    byte[] row = Bytes.toBytes("row");
    byte[] family = Bytes.toBytes("family");
    byte[] qf1 = Bytes.toBytes("qf1");

    // first segment in pipeline
    this.memstore.add(new KeyValue(row, family, qf1, ++currentTs, (byte[])null), null);
    long minTs = currentTs;
    this.memstore.add(new KeyValue(row, family, qf1, ++currentTs, (byte[])null), null);

    long numberOfCell = 2;
    assertEquals(numberOfCell, memstore.getSegments().stream().mapToInt(Segment::getCellsCount).sum());
    assertEquals(minTs, memstore.getSegments().stream().mapToLong(
        m -> m.getTimeRangeTracker().getMin()).min().getAsLong());
    assertEquals(currentTs, memstore.getSegments().stream().mapToLong(
        m -> m.getTimeRangeTracker().getMax()).max().getAsLong());

    ((CompactingMemStore) memstore).flushInMemory();

    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    if (isCompaction) {
      // max version = 1, so one cell will be dropped.
      numberOfCell = 1;
      minTs = currentTs;
    }
    // second segment in pipeline
    this.memstore.add(new KeyValue(row, family, qf1, ++currentTs, (byte[])null), null);
    this.memstore.add(new KeyValue(row, family, qf1, ++currentTs, (byte[])null), null);
    numberOfCell += 2;
    assertEquals(numberOfCell, memstore.getSegments().stream().mapToInt(Segment::getCellsCount).sum());
    assertEquals(minTs, memstore.getSegments().stream().mapToLong(
        m -> m.getTimeRangeTracker().getMin()).min().getAsLong());
    assertEquals(currentTs, memstore.getSegments().stream().mapToLong(
        m -> m.getTimeRangeTracker().getMax()).max().getAsLong());

    ((CompactingMemStore) memstore).flushInMemory(); // trigger the merge

    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    if (isCompaction) {
      // max version = 1, so one cell will be dropped.
      numberOfCell = 1;
      minTs = currentTs;
    }

    assertEquals(numberOfCell, memstore.getSegments().stream().mapToInt(Segment::getCellsCount).sum());
    assertEquals(minTs, memstore.getSegments().stream().mapToLong(
        m -> m.getTimeRangeTracker().getMin()).min().getAsLong());
    assertEquals(currentTs, memstore.getSegments().stream().mapToLong(
        m -> m.getTimeRangeTracker().getMax()).max().getAsLong());
  }

  @Test
  public void testCountOfCellsAfterFlatteningByScan() throws IOException {
    String[] keys1 = { "A", "B", "C" }; // A, B, C
    addRowsByKeysWith50Cols(memstore, keys1);
    // this should only flatten as there are no duplicates
    ((CompactingMemStore) memstore).flushInMemory();
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    List<KeyValueScanner> scanners = memstore.getScanners(Long.MAX_VALUE);
    // seek
    int count = 0;
    for(int i = 0; i < scanners.size(); i++) {
      scanners.get(i).seek(KeyValue.LOWESTKEY);
      while (scanners.get(i).next() != null) {
        count++;
      }
    }
    assertEquals("the count should be ", 150, count);
    for(int i = 0; i < scanners.size(); i++) {
      scanners.get(i).close();
    }
  }

  @Test
  public void testCountOfCellsAfterFlatteningByIterator() throws IOException {
    String[] keys1 = { "A", "B", "C" }; // A, B, C
    addRowsByKeysWith50Cols(memstore, keys1);
    // this should only flatten as there are no duplicates
    ((CompactingMemStore) memstore).flushInMemory();
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    // Just doing the cnt operation here
    MemStoreSegmentsIterator itr = new MemStoreMergerSegmentsIterator(
        ((CompactingMemStore) memstore).getImmutableSegments().getStoreSegments(),
        CellComparatorImpl.COMPARATOR, 10);
    int cnt = 0;
    try {
      while (itr.next() != null) {
        cnt++;
      }
    } finally {
      itr.close();
    }
    assertEquals("the count should be ", 150, cnt);
  }

  private void addRowsByKeysWith50Cols(AbstractMemStore hmc, String[] keys) {
    byte[] fam = Bytes.toBytes("testfamily");
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      for(int  j =0 ;j < 50; j++) {
        byte[] qf = Bytes.toBytes("testqualifier"+j);
        byte[] val = Bytes.toBytes(keys[i] + j);
        KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
        hmc.add(kv, null);
      }
    }
  }

  @Override
  @Test
  public void testPuttingBackChunksWithOpeningScanner() throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] qf6 = Bytes.toBytes("testqualifier6");
    byte[] qf7 = Bytes.toBytes("testqualifier7");
    byte[] val = Bytes.toBytes("testval");

    // Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, val), null);
    memstore.add(new KeyValue(row, fam, qf2, val), null);
    memstore.add(new KeyValue(row, fam, qf3, val), null);

    // Creating a snapshot
    MemStoreSnapshot snapshot = memstore.snapshot();
    assertEquals(3, memstore.getSnapshot().getCellsCount());

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf4, val), null);
    memstore.add(new KeyValue(row, fam, qf5, val), null);
    assertEquals(2, memstore.getActive().getCellsCount());

    // opening scanner before clear the snapshot
    List<KeyValueScanner> scanners = memstore.getScanners(0);
    // Shouldn't putting back the chunks to pool,since some scanners are opening
    // based on their data
    // close the scanners
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
    memstore.clearSnapshot(snapshot.getId());

    assertTrue(chunkCreator.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkCreator.getPoolSize() > 0);

    // clear chunks
    chunkCreator.clearChunksInPool();

    // Creating another snapshot

    snapshot = memstore.snapshot();
    // Adding more value
    memstore.add(new KeyValue(row, fam, qf6, val), null);
    memstore.add(new KeyValue(row, fam, qf7, val), null);
    // opening scanners
    scanners = memstore.getScanners(0);
    // close scanners before clear the snapshot
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    // Since no opening scanner, the chunks of snapshot should be put back to
    // pool
    // close the scanners
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
    memstore.clearSnapshot(snapshot.getId());
    assertTrue(chunkCreator.getPoolSize() > 0);
  }

  @Override
  @Test
  public void testPuttingBackChunksAfterFlushing() throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] val = Bytes.toBytes("testval");

    // Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, val), null);
    memstore.add(new KeyValue(row, fam, qf2, val), null);
    memstore.add(new KeyValue(row, fam, qf3, val), null);

    // Creating a snapshot
    MemStoreSnapshot snapshot = memstore.snapshot();
    assertEquals(3, memstore.getSnapshot().getCellsCount());

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf4, val), null);
    memstore.add(new KeyValue(row, fam, qf5, val), null);
    assertEquals(2, memstore.getActive().getCellsCount());
    // close the scanners
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
    memstore.clearSnapshot(snapshot.getId());

    int chunkCount = chunkCreator.getPoolSize();
    assertTrue(chunkCount > 0);
  }

  @Test
  public void testFlatteningToCellChunkMap() throws IOException {
    if(!toCellChunkMap) {
      return;
    }
    // set memstore to flat into CellChunkMap
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((MyCompactingMemStore)memstore).initiateType(compactionType, memstore.getConfiguration());
    ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);
    int numOfCells = 8;
    String[] keys1 = { "A", "A", "B", "C", "D", "D", "E", "F" }; //A1, A2, B3, C4, D5, D6, E7, F8

    // make one cell
    byte[] row = Bytes.toBytes(keys1[0]);
    byte[] val = Bytes.toBytes(keys1[0] + 0);
    KeyValue kv =
        new KeyValue(row, Bytes.toBytes("testfamily"), Bytes.toBytes("testqualifier"),
            System.currentTimeMillis(), val);

    // test 1 bucket
    int totalCellsLen = addRowsByKeys(memstore, keys1);
    long oneCellOnCSLMHeapSize = ClassSize.align(
      ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + KeyValue.FIXED_OVERHEAD + kv.getSerializedSize());

    long totalHeapSize = numOfCells * oneCellOnCSLMHeapSize + MutableSegment.DEEP_OVERHEAD;
    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore) memstore).heapSize());

    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and flatten
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    long oneCellOnCCMHeapSize =
        ClassSize.CELL_CHUNK_MAP_ENTRY + ClassSize.align(kv.getSerializedSize());
    totalHeapSize = MutableSegment.DEEP_OVERHEAD + CellChunkImmutableSegment.DEEP_OVERHEAD_CCM
        + numOfCells * oneCellOnCCMHeapSize;

    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore) memstore).heapSize());

    MemStoreSize mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    // simulate flusher
    region.decrMemStoreSize(mss);
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(numOfCells, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  /**
   * CellChunkMap Segment index requires all cell data to be written in the MSLAB Chunks.
   * Even though MSLAB is enabled, cells bigger than maxAlloc
   * (even if smaller than the size of a chunk) are not written in the MSLAB Chunks.
   * If such cells are found in the process of flattening into CellChunkMap
   * (in-memory-flush) they need to be copied into MSLAB.
   * testFlatteningToBigCellChunkMap checks that the process of flattening into
   * CellChunkMap succeeds, even when such big cells are allocated.
   */
  @Test
  public void testFlatteningToBigCellChunkMap() throws IOException {

    if (toCellChunkMap == false) {
      return;
    }
    // set memstore to flat into CellChunkMap
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
            String.valueOf(compactionType));
    ((MyCompactingMemStore)memstore).initiateType(compactionType, memstore.getConfiguration());
    ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);
    int numOfCells = 4;
    char[] chars = new char[MemStoreLAB.MAX_ALLOC_DEFAULT];
    for (int i = 0; i < chars.length; i++) {
      chars[i] = 'A';
    }
    String bigVal = new String(chars);
    String[] keys1 = { "A", "B", "C", "D"};

    // make one cell
    byte[] row = Bytes.toBytes(keys1[0]);
    byte[] val = Bytes.toBytes(bigVal);
    KeyValue kv =
            new KeyValue(row, Bytes.toBytes("testfamily"), Bytes.toBytes("testqualifier"),
                    System.currentTimeMillis(), val);

    // test 1 bucket
    int totalCellsLen = addRowsByKeys(memstore, keys1, val);

    long oneCellOnCSLMHeapSize =
            ClassSize.align(
                    ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + kv.heapSize());

    long totalHeapSize = numOfCells * oneCellOnCSLMHeapSize + MutableSegment.DEEP_OVERHEAD;
    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore) memstore).heapSize());

    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and flatten
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // One cell is duplicated, but it shouldn't be compacted because we are in BASIC mode.
    // totalCellsLen should remain the same
    long oneCellOnCCMHeapSize =
            ClassSize.CELL_CHUNK_MAP_ENTRY + ClassSize.align(kv.getSerializedSize());
    totalHeapSize = MutableSegment.DEEP_OVERHEAD + CellChunkImmutableSegment.DEEP_OVERHEAD_CCM
            + numOfCells * oneCellOnCCMHeapSize;

    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore) memstore).heapSize());

    MemStoreSize mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    // simulate flusher
    region.decrMemStoreSize(mss);
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(numOfCells, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  /**
   * CellChunkMap Segment index requires all cell data to be written in the MSLAB Chunks.
   * Even though MSLAB is enabled, cells bigger than the size of a chunk are not
   * written in the MSLAB Chunks.
   * If such cells are found in the process of flattening into CellChunkMap
   * (in-memory-flush) they need to be copied into MSLAB.
   * testFlatteningToJumboCellChunkMap checks that the process of flattening
   * into CellChunkMap succeeds, even when such big cells are allocated.
   */
  @Test
  public void testFlatteningToJumboCellChunkMap() throws IOException {

    if (toCellChunkMap == false) {
      return;
    }
    // set memstore to flat into CellChunkMap
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((MyCompactingMemStore) memstore).initiateType(compactionType, memstore.getConfiguration());
    ((CompactingMemStore) memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);

    int numOfCells = 1;
    char[] chars = new char[MemStoreLAB.CHUNK_SIZE_DEFAULT];
    for (int i = 0; i < chars.length; i++) {
      chars[i] = 'A';
    }
    String bigVal = new String(chars);
    String[] keys1 = {"A"};

    // make one cell
    byte[] row = Bytes.toBytes(keys1[0]);
    byte[] val = Bytes.toBytes(bigVal);
    KeyValue kv =
            new KeyValue(row, Bytes.toBytes("testfamily"), Bytes.toBytes("testqualifier"),
                    System.currentTimeMillis(), val);

    // test 1 bucket
    int totalCellsLen = addRowsByKeys(memstore, keys1, val);

    long oneCellOnCSLMHeapSize =
            ClassSize.align(
                    ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + kv.heapSize());

    long totalHeapSize = numOfCells * oneCellOnCSLMHeapSize + MutableSegment.DEEP_OVERHEAD;
    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore) memstore).heapSize());

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and flatten
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    // One cell is duplicated, but it shouldn't be compacted because we are in BASIC mode.
    // totalCellsLen should remain the same
    long oneCellOnCCMHeapSize =
        (long) ClassSize.CELL_CHUNK_MAP_ENTRY + ClassSize.align(kv.getSerializedSize());
    totalHeapSize = MutableSegment.DEEP_OVERHEAD + CellChunkImmutableSegment.DEEP_OVERHEAD_CCM
            + numOfCells * oneCellOnCCMHeapSize;

    assertEquals(totalCellsLen, regionServicesForStores
        .getMemStoreSize());

    assertEquals(totalHeapSize, ((CompactingMemStore) memstore).heapSize());

    MemStoreSize mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    // simulate flusher
    region.decrMemStoreSize(mss);
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(numOfCells, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());

    // Allocating two big cells (too big for being copied into a regular chunk).
    String[] keys2 = {"C", "D"};
    addRowsByKeys(memstore, keys2, val);
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }

    // The in-memory flush size is bigger than the size of a single cell,
    // but smaller than the size of two cells.
    // Therefore, the two created cells are flushed together as a single CSLMImmutableSegment and
    // flattened.
    totalHeapSize = MutableSegment.DEEP_OVERHEAD
        + CellChunkImmutableSegment.DEEP_OVERHEAD_CCM
        + 2 * oneCellOnCCMHeapSize;
    assertEquals(totalHeapSize, ((CompactingMemStore) memstore).heapSize());
  }

  /**
   * CellChunkMap Segment index requires all cell data to be written in the MSLAB Chunks.
   * Even though MSLAB is enabled, cells bigger than the size of a chunk are not
   * written in the MSLAB Chunks.
   * If such cells are found in the process of a merge they need to be copied into MSLAB.
   * testForceCopyOfBigCellIntoImmutableSegment checks that the
   * ImmutableMemStoreLAB's forceCopyOfBigCellInto does what it's supposed to do.
   */
  @org.junit.Ignore @Test // Flakey. Disabled by HBASE-24128. HBASE-24129 is for reenable.
  // TestCompactingToCellFlatMapMemStore.testForceCopyOfBigCellIntoImmutableSegment:902 i=1
  //   expected:<8389924> but was:<8389992>
  public void testForceCopyOfBigCellIntoImmutableSegment() throws IOException {

    if (toCellChunkMap == false) {
      return;
    }

    // set memstore to flat into CellChunkMap
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().setInt(MemStoreCompactionStrategy
        .COMPACTING_MEMSTORE_THRESHOLD_KEY, 4);
    memstore.getConfiguration()
        .setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.014);
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((MyCompactingMemStore) memstore).initiateType(compactionType, memstore.getConfiguration());
    ((CompactingMemStore) memstore).setIndexType(CompactingMemStore.IndexType.CHUNK_MAP);

    char[] chars = new char[MemStoreLAB.CHUNK_SIZE_DEFAULT];
    for (int i = 0; i < chars.length; i++) {
      chars[i] = 'A';
    }
    String bigVal = new String(chars);
    byte[] val = Bytes.toBytes(bigVal);

    // We need to add two cells, three times, in order to guarantee a merge
    List<String[]> keysList = new ArrayList<>();
    keysList.add(new String[]{"A", "B"});
    keysList.add(new String[]{"C", "D"});
    keysList.add(new String[]{"E", "F"});
    keysList.add(new String[]{"G", "H"});

    // Measuring the size of a single kv
    KeyValue kv = new KeyValue(Bytes.toBytes("A"), Bytes.toBytes("testfamily"),
            Bytes.toBytes("testqualifier"), System.currentTimeMillis(), val);
    long oneCellOnCCMHeapSize =
        (long) ClassSize.CELL_CHUNK_MAP_ENTRY + ClassSize.align(kv.getSerializedSize());
    long oneCellOnCSLMHeapSize =
        ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + kv.heapSize());
    long totalHeapSize = MutableSegment.DEEP_OVERHEAD;
    for (int i = 0; i < keysList.size(); i++) {
      addRowsByKeys(memstore, keysList.get(i), val);
      while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
        Threads.sleep(10);
      }

      if(i==0) {
        totalHeapSize += CellChunkImmutableSegment.DEEP_OVERHEAD_CCM
            + oneCellOnCCMHeapSize + oneCellOnCSLMHeapSize;
      } else {
        // The in-memory flush size is bigger than the size of a single cell,
        // but smaller than the size of two cells.
        // Therefore, the two created cells are flattened in a seperate segment.
        totalHeapSize += 2 * (CellChunkImmutableSegment.DEEP_OVERHEAD_CCM + oneCellOnCCMHeapSize);
      }
      if (i == 2) {
        // Four out of the five segments are merged into one
        totalHeapSize -= (4 * CellChunkImmutableSegment.DEEP_OVERHEAD_CCM);
        totalHeapSize = ClassSize.align(totalHeapSize);
      }
      assertEquals("i="+i, totalHeapSize, ((CompactingMemStore) memstore).heapSize());
    }
  }

  /**
   * Test big cell size after in memory compaction. (HBASE-26467)
   */
  @Test
  public void testBigCellSizeAfterInMemoryCompaction() throws IOException {
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().setInt(MemStoreCompactionStrategy
      .COMPACTING_MEMSTORE_THRESHOLD_KEY, 1);
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
      String.valueOf(compactionType));
    ((MyCompactingMemStore) memstore).initiateType(compactionType, memstore.getConfiguration());

    byte[] val = new byte[MemStoreLAB.CHUNK_SIZE_DEFAULT];

    long size = addRowsByKeys(memstore, new String[]{"A"}, val);
    ((MyCompactingMemStore) memstore).flushInMemory();

    for(KeyValueScanner scanner : memstore.getScanners(Long.MAX_VALUE)) {
      Cell cell;
      while ((cell = scanner.next()) != null) {
        assertEquals(size, cell.getSerializedSize());
      }
    }
  }


  private long addRowsByKeysDataSize(final AbstractMemStore hmc, String[] keys) {
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    MemStoreSizing memstoreSizing = new NonThreadSafeMemStoreSizing();
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      byte[] val = Bytes.toBytes(keys[i] + i);
      KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
      hmc.add(kv, memstoreSizing);
      LOG.debug("added kv: " + kv.getKeyString() + ", timestamp" + kv.getTimestamp());
    }
    MemStoreSize mss = memstoreSizing.getMemStoreSize();
    regionServicesForStores.addMemStoreSize(mss.getDataSize(), mss.getHeapSize(),
      mss.getOffHeapSize(), mss.getCellsCount());
    return mss.getDataSize();
  }

  private long cellBeforeFlushSize() {
    // make one cell
    byte[] row = Bytes.toBytes("A");
    byte[] val = Bytes.toBytes("A" + 0);
    KeyValue kv =
        new KeyValue(row, Bytes.toBytes("testfamily"), Bytes.toBytes("testqualifier"),
            System.currentTimeMillis(), val);
    return ClassSize.align(
        ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + KeyValue.FIXED_OVERHEAD + kv.getSerializedSize());
  }

  private long cellAfterFlushSize() {
    // make one cell
    byte[] row = Bytes.toBytes("A");
    byte[] val = Bytes.toBytes("A" + 0);
    KeyValue kv =
        new KeyValue(row, Bytes.toBytes("testfamily"), Bytes.toBytes("testqualifier"),
            System.currentTimeMillis(), val);

    return toCellChunkMap ?
        ClassSize.align(
        ClassSize.CELL_CHUNK_MAP_ENTRY + kv.getSerializedSize()) :
        ClassSize.align(
        ClassSize.CELL_ARRAY_MAP_ENTRY + KeyValue.FIXED_OVERHEAD + kv.getSerializedSize());
  }
}
