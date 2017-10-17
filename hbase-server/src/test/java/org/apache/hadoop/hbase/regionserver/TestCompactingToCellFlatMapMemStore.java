/*
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.Threads;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;

/**
 * compacted memstore test case
 */
@Category({RegionServerTests.class, MediumTests.class})
@RunWith(Parameterized.class)
public class TestCompactingToCellFlatMapMemStore extends TestCompactingMemStore {
  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] { "CHUNK_MAP", "ARRAY_MAP" }; // test different immutable indexes
  }
  private static final Log LOG = LogFactory.getLog(TestCompactingToCellFlatMapMemStore.class);
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

    this.memstore =
        new CompactingMemStore(conf, CellComparatorImpl.COMPARATOR, store,
            regionServicesForStores, MemoryCompactionPolicy.EAGER);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction tests
  //////////////////////////////////////////////////////////////////////////////
  public void testCompaction1Bucket() throws IOException {
    int counter = 0;
    String[] keys1 = { "A", "A", "B", "C" }; //A1, A2, B3, C4
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      conf.set(CompactingMemStore.COMPACTING_MEMSTORE_INDEX_KEY,
          String.valueOf(CompactingMemStore.IndexType.CHUNK_MAP));
      ((CompactingMemStore)memstore).setIndexType();
    }

    // test 1 bucket
    long totalCellsLen = addRowsByKeys(memstore, keys1);
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
    MemStoreSize size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.decrMemStoreSize(size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(3, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  public void testCompaction2Buckets() throws IOException {
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      conf.set(CompactingMemStore.COMPACTING_MEMSTORE_INDEX_KEY,
          String.valueOf(CompactingMemStore.IndexType.CHUNK_MAP));
      ((CompactingMemStore)memstore).setIndexType();
    }
    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };

    long totalCellsLen1 = addRowsByKeys(memstore, keys1);     // INSERT 4
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

    long totalCellsLen2 = addRowsByKeys(memstore, keys2);   // INSERT 3 (3+3=6)
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

    MemStoreSize size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.decrMemStoreSize(size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  public void testCompaction3Buckets() throws IOException {
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      conf.set(CompactingMemStore.COMPACTING_MEMSTORE_INDEX_KEY,
          String.valueOf(CompactingMemStore.IndexType.CHUNK_MAP));
      ((CompactingMemStore)memstore).setIndexType();
    }
    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };
    String[] keys3 = { "D", "B", "B" };

    long totalCellsLen1 = addRowsByKeys(memstore, keys1);
    long cellBeforeFlushSize = cellBeforeFlushSize();
    long cellAfterFlushSize = cellAfterFlushSize();
    long totalHeapSize1 = MutableSegment.DEEP_OVERHEAD + 4 * cellBeforeFlushSize;
    assertEquals(totalCellsLen1, region.getMemStoreSize());
    assertEquals(totalHeapSize1, ((CompactingMemStore) memstore).heapSize());

    MemStoreSize size = memstore.getFlushableSize();
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

    long totalCellsLen2 = addRowsByKeys(memstore, keys2);
    long totalHeapSize2 = 3 * cellBeforeFlushSize;

    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1 + totalHeapSize2, ((CompactingMemStore) memstore).heapSize());

    ((CompactingMemStore) memstore).disableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline without compaction
    totalHeapSize2 = totalHeapSize2 + CSLMImmutableSegment.DEEP_OVERHEAD_CSLM;
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1 + totalHeapSize2, ((CompactingMemStore) memstore).heapSize());

    long totalCellsLen3 = addRowsByKeys(memstore, keys3);
    long totalHeapSize3 = 3 * cellBeforeFlushSize;
    assertEquals(totalCellsLen1 + totalCellsLen2 + totalCellsLen3,
        regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize1 + totalHeapSize2 + totalHeapSize3,
        ((CompactingMemStore) memstore).heapSize());

    ((CompactingMemStore) memstore).enableCompaction();
    size = memstore.getFlushableSize();
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

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.decrMemStoreSize(size);  // simulate flusher
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
      conf.set(CompactingMemStore.COMPACTING_MEMSTORE_INDEX_KEY,
          String.valueOf(CompactingMemStore.IndexType.CHUNK_MAP));
      ((CompactingMemStore)memstore).setIndexType();
    }
    String[] keys1 = { "A", "A", "B", "C", "F", "H"};
    String[] keys2 = { "A", "B", "D", "G", "I", "J"};
    String[] keys3 = { "D", "B", "B", "E" };

    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((CompactingMemStore)memstore).initiateType(compactionType);
    addRowsByKeys(memstore, keys1);

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline should not compact

    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    addRowsByKeys(memstore, keys2); // also should only flatten

    int counter2 = 0;
    for ( Segment s : memstore.getSegments()) {
      counter2 += s.getCellsCount();
    }
    assertEquals(12, counter2);

    ((CompactingMemStore) memstore).disableCompaction();

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline without flattening
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    int counter3 = 0;
    for ( Segment s : memstore.getSegments()) {
      counter3 += s.getCellsCount();
    }
    assertEquals(12, counter3);

    addRowsByKeys(memstore, keys3);

    int counter4 = 0;
    for ( Segment s : memstore.getSegments()) {
      counter4 += s.getCellsCount();
    }
    assertEquals(16, counter4);

    ((CompactingMemStore) memstore).enableCompaction();


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
      conf.set(CompactingMemStore.COMPACTING_MEMSTORE_INDEX_KEY,
          String.valueOf(CompactingMemStore.IndexType.CHUNK_MAP));
      ((CompactingMemStore)memstore).setIndexType();
    }
    testTimeRange(true);
  }

  @Test
  public void testTimeRangeAfterMerge() throws IOException {
    if (toCellChunkMap) {
      // set memstore to flat into CellChunkMap
      conf.set(CompactingMemStore.COMPACTING_MEMSTORE_INDEX_KEY,
          String.valueOf(CompactingMemStore.IndexType.CHUNK_MAP));
      ((CompactingMemStore)memstore).setIndexType();
    }
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((CompactingMemStore)memstore).initiateType(compactionType);
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
    assertEquals("the count should be ", count, 150);
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
    assertEquals("the count should be ", cnt, 150);
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


  private long addRowsByKeys(final AbstractMemStore hmc, String[] keys) {
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    MemStoreSize memstoreSize = new MemStoreSize();
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      byte[] val = Bytes.toBytes(keys[i] + i);
      KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
      hmc.add(kv, memstoreSize);
      LOG.debug("added kv: " + kv.getKeyString() + ", timestamp" + kv.getTimestamp());
    }
    regionServicesForStores.addMemStoreSize(memstoreSize);
    return memstoreSize.getDataSize();
  }

  private long cellBeforeFlushSize() {
    // make one cell
    byte[] row = Bytes.toBytes("A");
    byte[] val = Bytes.toBytes("A" + 0);
    KeyValue kv =
        new KeyValue(row, Bytes.toBytes("testfamily"), Bytes.toBytes("testqualifier"),
            System.currentTimeMillis(), val);
    return ClassSize.align(
        ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + KeyValue.FIXED_OVERHEAD + KeyValueUtil.length(kv));
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
        ClassSize.CELL_CHUNK_MAP_ENTRY + KeyValueUtil.length(kv)) :
        ClassSize.align(
        ClassSize.CELL_ARRAY_MAP_ENTRY + KeyValue.FIXED_OVERHEAD + KeyValueUtil.length(kv));
  }
}
