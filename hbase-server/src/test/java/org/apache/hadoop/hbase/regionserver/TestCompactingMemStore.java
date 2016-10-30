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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * compacted memstore test case
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestCompactingMemStore extends TestDefaultMemStore {

  private static final Log LOG = LogFactory.getLog(TestCompactingMemStore.class);
  protected static MemStoreChunkPool chunkPool;
  protected HRegion region;
  protected RegionServicesForStores regionServicesForStores;
  protected HStore store;

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  protected static byte[] makeQualifier(final int i1, final int i2) {
    return Bytes.toBytes(Integer.toString(i1) + ";" +
        Integer.toString(i2));
  }

  @After
  public void tearDown() throws Exception {
    chunkPool.clearChunks();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    compactingSetUp();
    this.memstore = new CompactingMemStore(HBaseConfiguration.create(), CellComparator.COMPARATOR,
        store, regionServicesForStores);
  }

  protected void compactingSetUp() throws Exception {
    super.internalSetUp();
    Configuration conf = new Configuration();
    conf.setBoolean(SegmentFactory.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreChunkPool.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 1000);
    HBaseTestingUtility hbaseUtility = HBaseTestingUtility.createLocalHTU(conf);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    this.region = hbaseUtility.createTestRegion("foobar", hcd);
    this.regionServicesForStores = region.getRegionServicesForStores();
    this.store = new HStore(region, hcd, conf);

    chunkPool = MemStoreChunkPool.getPool(conf);
    assertTrue(chunkPool != null);
  }

  /**
   * A simple test which verifies the 3 possible states when scanning across snapshot.
   *
   * @throws IOException
   * @throws CloneNotSupportedException
   */
  @Override
  @Test
  public void testScanAcrossSnapshot2() throws IOException, CloneNotSupportedException {
    // we are going to the scanning across snapshot with two kvs
    // kv1 should always be returned before kv2
    final byte[] one = Bytes.toBytes(1);
    final byte[] two = Bytes.toBytes(2);
    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes(3);

    final KeyValue kv1 = new KeyValue(one, f, q, 10, v);
    final KeyValue kv2 = new KeyValue(two, f, q, 10, v);

    // use case 1: both kvs in kvset
    this.memstore.add(kv1.clone(), null);
    this.memstore.add(kv2.clone(), null);
    verifyScanAcrossSnapshot2(kv1, kv2);

    // use case 2: both kvs in snapshot
    this.memstore.snapshot();
    verifyScanAcrossSnapshot2(kv1, kv2);

    // use case 3: first in snapshot second in kvset
    this.memstore = new CompactingMemStore(HBaseConfiguration.create(),
        CellComparator.COMPARATOR, store, regionServicesForStores);
    this.memstore.add(kv1.clone(), null);
    // As compaction is starting in the background the repetition
    // of the k1 might be removed BUT the scanners created earlier
    // should look on the OLD MutableCellSetSegment, so this should be OK...
    this.memstore.snapshot();
    this.memstore.add(kv2.clone(), null);
    verifyScanAcrossSnapshot2(kv1,kv2);
  }

  /**
   * Test memstore snapshots
   * @throws IOException
   */
  @Override
  @Test
  public void testSnapshotting() throws IOException {
    final int snapshotCount = 5;
    // Add some rows, run a snapshot. Do it a few times.
    for (int i = 0; i < snapshotCount; i++) {
      addRows(this.memstore);
      runSnapshot(this.memstore, true);
      assertEquals("History not being cleared", 0, this.memstore.getSnapshot().getCellsCount());
    }
  }


  //////////////////////////////////////////////////////////////////////////////
  // Get tests
  //////////////////////////////////////////////////////////////////////////////

  /** Test getNextRow from memstore
   * @throws InterruptedException
   */
  @Override
  @Test
  public void testGetNextRow() throws Exception {
    addRows(this.memstore);
    // Add more versions to make it a little more interesting.
    Thread.sleep(1);
    addRows(this.memstore);
    Cell closestToEmpty = ((CompactingMemStore)this.memstore).getNextRow(KeyValue.LOWESTKEY);
    assertTrue(CellComparator.COMPARATOR.compareRows(closestToEmpty,
        new KeyValue(Bytes.toBytes(0), System.currentTimeMillis())) == 0);
    for (int i = 0; i < ROW_COUNT; i++) {
      Cell nr = ((CompactingMemStore)this.memstore).getNextRow(new KeyValue(Bytes.toBytes(i),
          System.currentTimeMillis()));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(CellComparator.COMPARATOR.compareRows(nr,
            new KeyValue(Bytes.toBytes(i + 1), System.currentTimeMillis())) == 0);
      }
    }
    //starting from each row, validate results should contain the starting row
    Configuration conf = HBaseConfiguration.create();
    for (int startRowId = 0; startRowId < ROW_COUNT; startRowId++) {
      ScanInfo scanInfo = new ScanInfo(conf, FAMILY, 0, 1, Integer.MAX_VALUE,
        KeepDeletedCells.FALSE, 0, this.memstore.getComparator());
      ScanType scanType = ScanType.USER_SCAN;
      InternalScanner scanner = new StoreScanner(new Scan(
          Bytes.toBytes(startRowId)), scanInfo, scanType, null,
          memstore.getScanners(0));
      List<Cell> results = new ArrayList<Cell>();
      for (int i = 0; scanner.next(results); i++) {
        int rowId = startRowId + i;
        Cell left = results.get(0);
        byte[] row1 = Bytes.toBytes(rowId);
        assertTrue("Row name",
            CellComparator.COMPARATOR.compareRows(left, row1, 0, row1.length) == 0);
        assertEquals("Count of columns", QUALIFIER_COUNT, results.size());
        List<Cell> row = new ArrayList<Cell>();
        for (Cell kv : results) {
          row.add(kv);
        }
        isExpectedRowWithoutTimestamps(rowId, row);
        // Clear out set.  Otherwise row results accumulate.
        results.clear();
      }
    }
  }

  @Override
  @Test
  public void testGet_memstoreAndSnapShot() throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] val = Bytes.toBytes("testval");

    //Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, val), null);
    memstore.add(new KeyValue(row, fam, qf2, val), null);
    memstore.add(new KeyValue(row, fam, qf3, val), null);
    //Pushing to pipeline
    ((CompactingMemStore)memstore).flushInMemory();
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    //Creating a snapshot
    memstore.snapshot();
    assertEquals(3, memstore.getSnapshot().getCellsCount());
    //Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf4, val), null);
    memstore.add(new KeyValue(row, fam, qf5, val), null);
    assertEquals(2, memstore.getActive().getCellsCount());
  }

  ////////////////////////////////////
  // Test for periodic memstore flushes
  // based on time of oldest edit
  ////////////////////////////////////

  /**
   * Add keyvalues with a fixed memstoreTs, and checks that memstore size is decreased
   * as older keyvalues are deleted from the memstore.
   *
   * @throws Exception
   */
  @Override
  @Test
  public void testUpsertMemstoreSize() throws Exception {
    MemstoreSize oldSize = memstore.size();

    List<Cell> l = new ArrayList<Cell>();
    KeyValue kv1 = KeyValueTestUtil.create("r", "f", "q", 100, "v");
    KeyValue kv2 = KeyValueTestUtil.create("r", "f", "q", 101, "v");
    KeyValue kv3 = KeyValueTestUtil.create("r", "f", "q", 102, "v");

    kv1.setSequenceId(1);
    kv2.setSequenceId(1);
    kv3.setSequenceId(1);
    l.add(kv1);
    l.add(kv2);
    l.add(kv3);

    this.memstore.upsert(l, 2, null);// readpoint is 2
    MemstoreSize newSize = this.memstore.size();
    assert (newSize.getDataSize() > oldSize.getDataSize());
    //The kv1 should be removed.
    assert (memstore.getActive().getCellsCount() == 2);

    KeyValue kv4 = KeyValueTestUtil.create("r", "f", "q", 104, "v");
    kv4.setSequenceId(1);
    l.clear();
    l.add(kv4);
    this.memstore.upsert(l, 3, null);
    assertEquals(newSize, this.memstore.size());
    //The kv2 should be removed.
    assert (memstore.getActive().getCellsCount() == 2);
    //this.memstore = null;
  }

  /**
   * Tests that the timeOfOldestEdit is updated correctly for the
   * various edit operations in memstore.
   * @throws Exception
   */
  @Override
  @Test
  public void testUpdateToTimeOfOldestEdit() throws Exception {
    try {
      EnvironmentEdgeForMemstoreTest edge = new EnvironmentEdgeForMemstoreTest();
      EnvironmentEdgeManager.injectEdge(edge);
      long t = memstore.timeOfOldestEdit();
      assertEquals(t, Long.MAX_VALUE);

      // test the case that the timeOfOldestEdit is updated after a KV add
      memstore.add(KeyValueTestUtil.create("r", "f", "q", 100, "v"), null);
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
      // The method will also assert
      // the value is reset to Long.MAX_VALUE
      t = runSnapshot(memstore, true);

      // test the case that the timeOfOldestEdit is updated after a KV delete
      memstore.add(KeyValueTestUtil.create("r", "f", "q", 100, KeyValue.Type.Delete, "v"), null);
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
     t = runSnapshot(memstore, true);

      // test the case that the timeOfOldestEdit is updated after a KV upsert
      List<Cell> l = new ArrayList<Cell>();
      KeyValue kv1 = KeyValueTestUtil.create("r", "f", "q", 100, "v");
      kv1.setSequenceId(100);
      l.add(kv1);
      memstore.upsert(l, 1000, null);
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  private long runSnapshot(final AbstractMemStore hmc, boolean useForce)
      throws IOException {
    // Save off old state.
    long oldHistorySize = hmc.getSnapshot().keySize();
    long prevTimeStamp = hmc.timeOfOldestEdit();

    hmc.snapshot();
    MemStoreSnapshot snapshot = hmc.snapshot();
    if (useForce) {
      // Make some assertions about what just happened.
      assertTrue("History size has not increased", oldHistorySize < snapshot.getDataSize());
      long t = hmc.timeOfOldestEdit();
      assertTrue("Time of oldest edit is not Long.MAX_VALUE", t == Long.MAX_VALUE);
      hmc.clearSnapshot(snapshot.getId());
    } else {
      long t = hmc.timeOfOldestEdit();
      assertTrue("Time of oldest edit didn't remain the same", t == prevTimeStamp);
    }
    return prevTimeStamp;
  }

  private void isExpectedRowWithoutTimestamps(final int rowIndex,
      List<Cell> kvs) {
    int i = 0;
    for (Cell kv : kvs) {
      byte[] expectedColname = makeQualifier(rowIndex, i++);
      assertTrue("Column name", CellUtil.matchingQualifier(kv, expectedColname));
      // Value is column name as bytes.  Usually result is
      // 100 bytes in size at least. This is the default size
      // for BytesWriteable.  For comparison, convert bytes to
      // String and trim to remove trailing null bytes.
      assertTrue("Content", CellUtil.matchingValue(kv, expectedColname));
    }
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
    memstore.clearSnapshot(snapshot.getId());

    int chunkCount = chunkPool.getPoolSize();
    assertTrue(chunkCount > 0);

  }

  @Test
  public void testPuttingBackChunksWithOpeningScanner()
      throws IOException {
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
    memstore.clearSnapshot(snapshot.getId());

    assertTrue(chunkPool.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkPool.getPoolSize() > 0);

    // clear chunks
    chunkPool.clearChunks();

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
    memstore.clearSnapshot(snapshot.getId());
    assertTrue(chunkPool.getPoolSize() > 0);
  }

  @Test
  public void testPuttingBackChunksWithOpeningPipelineScanner()
      throws IOException {

    // set memstore to do data compaction and not to use the speculative scan
    memstore.getConfiguration().set("hbase.hregion.compacting.memstore.type", "data-compaction");
    ((CompactingMemStore)memstore).initiateType();

    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] val = Bytes.toBytes("testval");

    // Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, 1, val), null);
    memstore.add(new KeyValue(row, fam, qf2, 1, val), null);
    memstore.add(new KeyValue(row, fam, qf3, 1, val), null);

    // Creating a pipeline
    ((CompactingMemStore)memstore).disableCompaction();
    ((CompactingMemStore)memstore).flushInMemory();

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf1, 2, val), null);
    memstore.add(new KeyValue(row, fam, qf2, 2, val), null);
    assertEquals(2, memstore.getActive().getCellsCount());

    // pipeline bucket 2
    ((CompactingMemStore)memstore).flushInMemory();
    // opening scanner before force flushing
    List<KeyValueScanner> scanners = memstore.getScanners(0);
    // Shouldn't putting back the chunks to pool,since some scanners are opening
    // based on their data
    ((CompactingMemStore)memstore).enableCompaction();
    // trigger compaction
    ((CompactingMemStore)memstore).flushInMemory();

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf3, 3, val), null);
    memstore.add(new KeyValue(row, fam, qf2, 3, val), null);
    memstore.add(new KeyValue(row, fam, qf1, 3, val), null);
    assertEquals(3, memstore.getActive().getCellsCount());

    assertTrue(chunkPool.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkPool.getPoolSize() > 0);

    // clear chunks
    chunkPool.clearChunks();

    // Creating another snapshot

    MemStoreSnapshot snapshot = memstore.snapshot();
    memstore.clearSnapshot(snapshot.getId());

    snapshot = memstore.snapshot();
    // Adding more value
    memstore.add(new KeyValue(row, fam, qf2, 4, val), null);
    memstore.add(new KeyValue(row, fam, qf3, 4, val), null);
    // opening scanners
    scanners = memstore.getScanners(0);
    // close scanners before clear the snapshot
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    // Since no opening scanner, the chunks of snapshot should be put back to
    // pool
    memstore.clearSnapshot(snapshot.getId());
    assertTrue(chunkPool.getPoolSize() > 0);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction tests
  //////////////////////////////////////////////////////////////////////////////
  @Test
  public void testCompaction1Bucket() throws IOException {

    // set memstore to do data compaction and not to use the speculative scan
    memstore.getConfiguration().set("hbase.hregion.compacting.memstore.type", "data-compaction");
    ((CompactingMemStore)memstore).initiateType();

    String[] keys1 = { "A", "A", "B", "C" }; //A1, A2, B3, C4

    // test 1 bucket
    int totalCellsLen = addRowsByKeys(memstore, keys1);
    long totalHeapOverhead = 4 * (KeyValue.FIXED_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);
    assertEquals(totalCellsLen, regionServicesForStores.getMemstoreSize());
    assertEquals(totalHeapOverhead, ((CompactingMemStore)memstore).heapOverhead());

    MemstoreSize size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // One cell is duplicated and the compaction will remove it. All cells of same size so adjusting
    // totalCellsLen
    totalCellsLen = (totalCellsLen * 3) / 4;
    totalHeapOverhead = 3 * (KeyValue.FIXED_OVERHEAD + ClassSize.CELL_ARRAY_MAP_ENTRY);
    assertEquals(totalCellsLen, regionServicesForStores.getMemstoreSize());
    assertEquals(totalHeapOverhead, ((CompactingMemStore)memstore).heapOverhead());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.decrMemstoreSize(size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(3, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemstoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  @Test
  public void testCompaction2Buckets() throws IOException {

    // set memstore to do data compaction and not to use the speculative scan
    memstore.getConfiguration().set("hbase.hregion.compacting.memstore.type", "data-compaction");
    ((CompactingMemStore)memstore).initiateType();
    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };

    int totalCellsLen1 = addRowsByKeys(memstore, keys1);
    long totalHeapOverhead = 4 * (KeyValue.FIXED_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);

    assertEquals(totalCellsLen1, regionServicesForStores.getMemstoreSize());
    assertEquals(totalHeapOverhead, ((CompactingMemStore)memstore).heapOverhead());

    MemstoreSize size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    int counter = 0;
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(3, counter);
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // One cell is duplicated and the compaction will remove it. All cells of same time so adjusting
    // totalCellsLen
    totalCellsLen1 = (totalCellsLen1 * 3) / 4;
    assertEquals(totalCellsLen1, regionServicesForStores.getMemstoreSize());
    totalHeapOverhead = 3 * (KeyValue.FIXED_OVERHEAD + ClassSize.CELL_ARRAY_MAP_ENTRY);
    assertEquals(totalHeapOverhead, ((CompactingMemStore)memstore).heapOverhead());

    int totalCellsLen2 = addRowsByKeys(memstore, keys2);
    totalHeapOverhead += 3 * (KeyValue.FIXED_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemstoreSize());
    assertEquals(totalHeapOverhead, ((CompactingMemStore)memstore).heapOverhead());

    size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    totalCellsLen2 = totalCellsLen2 / 3;// 2 cells duplicated in set 2
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemstoreSize());
    totalHeapOverhead = 4 * (KeyValue.FIXED_OVERHEAD + ClassSize.CELL_ARRAY_MAP_ENTRY);
    assertEquals(totalHeapOverhead, ((CompactingMemStore)memstore).heapOverhead());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.decrMemstoreSize(size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemstoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  @Test
  public void testCompaction3Buckets() throws IOException {

    // set memstore to do data compaction and not to use the speculative scan
    memstore.getConfiguration().set("hbase.hregion.compacting.memstore.type", "data-compaction");
    ((CompactingMemStore)memstore).initiateType();
    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };
    String[] keys3 = { "D", "B", "B" };

    int totalCellsLen1 = addRowsByKeys(memstore, keys1);// Adding 4 cells.
    assertEquals(totalCellsLen1, region.getMemstoreSize());
    long totalHeapOverhead = 4 * (KeyValue.FIXED_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);
    assertEquals(totalHeapOverhead, ((CompactingMemStore)memstore).heapOverhead());

    MemstoreSize size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact

    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // One cell is duplicated and the compaction will remove it. All cells of same time so adjusting
    // totalCellsLen
    totalCellsLen1 = (totalCellsLen1 * 3) / 4;
    assertEquals(totalCellsLen1, regionServicesForStores.getMemstoreSize());
    // In memory flush to make a CellArrayMap instead of CSLM. See the overhead diff.
    totalHeapOverhead = 3 * (KeyValue.FIXED_OVERHEAD + ClassSize.CELL_ARRAY_MAP_ENTRY);
    assertEquals(totalHeapOverhead, ((CompactingMemStore)memstore).heapOverhead());

    int totalCellsLen2 = addRowsByKeys(memstore, keys2);// Adding 3 more cells.
    long totalHeapOverhead2 = 3
        * (KeyValue.FIXED_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);

    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemstoreSize());
    assertEquals(totalHeapOverhead + totalHeapOverhead2,
        ((CompactingMemStore) memstore).heapOverhead());

    ((CompactingMemStore) memstore).disableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline without compaction
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // No change in the cells data size. ie. memstore size. as there is no compaction.
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemstoreSize());
    assertEquals(totalHeapOverhead + totalHeapOverhead2,
        ((CompactingMemStore) memstore).heapOverhead());

    int totalCellsLen3 = addRowsByKeys(memstore, keys3);// 3 more cells added
    assertEquals(totalCellsLen1 + totalCellsLen2 + totalCellsLen3,
        regionServicesForStores.getMemstoreSize());
    long totalHeapOverhead3 = 3
        * (KeyValue.FIXED_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);
    assertEquals(totalHeapOverhead + totalHeapOverhead2 + totalHeapOverhead3,
        ((CompactingMemStore) memstore).heapOverhead());

    ((CompactingMemStore)memstore).enableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore)memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // active flushed to pipeline and all 3 segments compacted. Will get rid of duplicated cells.
    // Out of total 10, only 4 cells are unique
    totalCellsLen2 = totalCellsLen2 / 3;// 2 out of 3 cells are duplicated
    totalCellsLen3 = 0;// All duplicated cells.
    assertEquals(totalCellsLen1 + totalCellsLen2 + totalCellsLen3,
        regionServicesForStores.getMemstoreSize());
    // Only 4 unique cells left
    assertEquals(4 * (KeyValue.FIXED_OVERHEAD + ClassSize.CELL_ARRAY_MAP_ENTRY),
        ((CompactingMemStore) memstore).heapOverhead());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.decrMemstoreSize(size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemstoreSize());

    memstore.clearSnapshot(snapshot.getId());

    //assertTrue(tstStr, false);
  }

  private int addRowsByKeys(final AbstractMemStore hmc, String[] keys) {
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    long size = hmc.getActive().keySize();
    long heapOverhead = hmc.getActive().heapOverhead();
    int totalLen = 0;
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      byte[] val = Bytes.toBytes(keys[i] + i);
      KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
      totalLen += kv.getLength();
      hmc.add(kv, null);
      LOG.debug("added kv: " + kv.getKeyString() + ", timestamp:" + kv.getTimestamp());
    }
    regionServicesForStores.addMemstoreSize(new MemstoreSize(hmc.getActive().keySize() - size,
        hmc.getActive().heapOverhead() - heapOverhead));
    return totalLen;
  }

  private class EnvironmentEdgeForMemstoreTest implements EnvironmentEdge {
    long t = 1234;

    @Override
    public long currentTime() {
            return t;
        }
      public void setCurrentTimeMillis(long t) {
        this.t = t;
      }
    }

}
