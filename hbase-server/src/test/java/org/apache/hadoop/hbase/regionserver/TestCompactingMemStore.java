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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
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
  private static MemStoreChunkPool chunkPool;
  private HRegion region;
  private RegionServicesForStores regionServicesForStores;
  private HStore store;

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  private static byte[] makeQualifier(final int i1, final int i2) {
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
    this.memstore = new CompactingMemStore(HBaseConfiguration.create(), CellComparator.COMPARATOR,
        store, regionServicesForStores);
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
    this.memstore.add(kv1.clone());
    this.memstore.add(kv2.clone());
    verifyScanAcrossSnapshot2(kv1, kv2);

    // use case 2: both kvs in snapshot
    this.memstore.snapshot();
    verifyScanAcrossSnapshot2(kv1, kv2);

    // use case 3: first in snapshot second in kvset
    this.memstore = new CompactingMemStore(HBaseConfiguration.create(),
        CellComparator.COMPARATOR, store, regionServicesForStores);
    this.memstore.add(kv1.clone());
    // As compaction is starting in the background the repetition
    // of the k1 might be removed BUT the scanners created earlier
    // should look on the OLD MutableCellSetSegment, so this should be OK...
    this.memstore.snapshot();
    this.memstore.add(kv2.clone());
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
    assertTrue(KeyValue.COMPARATOR.compareRows(closestToEmpty,
        new KeyValue(Bytes.toBytes(0), System.currentTimeMillis())) == 0);
    for (int i = 0; i < ROW_COUNT; i++) {
      Cell nr = ((CompactingMemStore)this.memstore).getNextRow(new KeyValue(Bytes.toBytes(i),
          System.currentTimeMillis()));
      if (i + 1 == ROW_COUNT) {
        assertEquals(nr, null);
      } else {
        assertTrue(KeyValue.COMPARATOR.compareRows(nr,
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
    memstore.add(new KeyValue(row, fam, qf1, val));
    memstore.add(new KeyValue(row, fam, qf2, val));
    memstore.add(new KeyValue(row, fam, qf3, val));
    //Pushing to pipeline
    ((CompactingMemStore)memstore).flushInMemory();
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    //Creating a snapshot
    memstore.snapshot();
    assertEquals(3, memstore.getSnapshot().getCellsCount());
    //Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf4, val));
    memstore.add(new KeyValue(row, fam, qf5, val));
    assertEquals(2, memstore.getActive().getCellsCount());
  }


  ////////////////////////////////////
  //Test for upsert with MSLAB
  ////////////////////////////////////

  /**
   * Test a pathological pattern that shows why we can't currently
   * use the MSLAB for upsert workloads. This test inserts data
   * in the following pattern:
   *
   * - row0001 through row1000 (fills up one 2M Chunk)
   * - row0002 through row1001 (fills up another 2M chunk, leaves one reference
   *   to the first chunk
   * - row0003 through row1002 (another chunk, another dangling reference)
   *
   * This causes OOME pretty quickly if we use MSLAB for upsert
   * since each 2M chunk is held onto by a single reference.
   */
  @Override
  @Test
  public void testUpsertMSLAB() throws Exception {

    int ROW_SIZE = 2048;
    byte[] qualifier = new byte[ROW_SIZE - 4];

    MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
    for (int i = 0; i < 3; i++) { System.gc(); }
    long usageBefore = bean.getHeapMemoryUsage().getUsed();

    long size = 0;
    long ts=0;

    for (int newValue = 0; newValue < 1000; newValue++) {
      for (int row = newValue; row < newValue + 1000; row++) {
        byte[] rowBytes = Bytes.toBytes(row);
        size += memstore.updateColumnValue(rowBytes, FAMILY, qualifier, newValue, ++ts);
      }
    }
    System.out.println("Wrote " + ts + " vals");
    for (int i = 0; i < 3; i++) { System.gc(); }
    long usageAfter = bean.getHeapMemoryUsage().getUsed();
    System.out.println("Memory used: " + (usageAfter - usageBefore)
        + " (heapsize: " + memstore.heapSize() +
        " size: " + size + ")");
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
    long oldSize = memstore.size();

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

    this.memstore.upsert(l, 2);// readpoint is 2
    long newSize = this.memstore.size();
    assert (newSize > oldSize);
    //The kv1 should be removed.
    assert (memstore.getActive().getCellsCount() == 2);

    KeyValue kv4 = KeyValueTestUtil.create("r", "f", "q", 104, "v");
    kv4.setSequenceId(1);
    l.clear();
    l.add(kv4);
    this.memstore.upsert(l, 3);
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
      memstore.add(KeyValueTestUtil.create("r", "f", "q", 100, "v"));
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
      // The method will also assert
      // the value is reset to Long.MAX_VALUE
      t = runSnapshot(memstore, true);

      // test the case that the timeOfOldestEdit is updated after a KV delete
      memstore.delete(KeyValueTestUtil.create("r", "f", "q", 100, "v"));
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
     t = runSnapshot(memstore, true);

      // test the case that the timeOfOldestEdit is updated after a KV upsert
      List<Cell> l = new ArrayList<Cell>();
      KeyValue kv1 = KeyValueTestUtil.create("r", "f", "q", 100, "v");
      kv1.setSequenceId(100);
      l.add(kv1);
      memstore.upsert(l, 1000);
      t = memstore.timeOfOldestEdit();
      assertTrue(t == 1234);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  private long runSnapshot(final AbstractMemStore hmc, boolean useForce)
      throws IOException {
    // Save off old state.
    long oldHistorySize = hmc.getSnapshot().getSize();
    long prevTimeStamp = hmc.timeOfOldestEdit();

    hmc.snapshot();
    MemStoreSnapshot snapshot = hmc.snapshot();
    if (useForce) {
      // Make some assertions about what just happened.
      assertTrue("History size has not increased", oldHistorySize < snapshot.getSize());
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
    memstore.add(new KeyValue(row, fam, qf1, val));
    memstore.add(new KeyValue(row, fam, qf2, val));
    memstore.add(new KeyValue(row, fam, qf3, val));

    // Creating a snapshot
    MemStoreSnapshot snapshot = memstore.snapshot();
    assertEquals(3, memstore.getSnapshot().getCellsCount());

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf4, val));
    memstore.add(new KeyValue(row, fam, qf5, val));
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
    memstore.add(new KeyValue(row, fam, qf1, val));
    memstore.add(new KeyValue(row, fam, qf2, val));
    memstore.add(new KeyValue(row, fam, qf3, val));

    // Creating a snapshot
    MemStoreSnapshot snapshot = memstore.snapshot();
    assertEquals(3, memstore.getSnapshot().getCellsCount());

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf4, val));
    memstore.add(new KeyValue(row, fam, qf5, val));
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
    memstore.add(new KeyValue(row, fam, qf6, val));
    memstore.add(new KeyValue(row, fam, qf7, val));
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
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] val = Bytes.toBytes("testval");

    // Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, 1, val));
    memstore.add(new KeyValue(row, fam, qf2, 1, val));
    memstore.add(new KeyValue(row, fam, qf3, 1, val));

    // Creating a pipeline
    ((CompactingMemStore)memstore).disableCompaction();
    ((CompactingMemStore)memstore).flushInMemory();

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf1, 2, val));
    memstore.add(new KeyValue(row, fam, qf2, 2, val));
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
    memstore.add(new KeyValue(row, fam, qf3, 3, val));
    memstore.add(new KeyValue(row, fam, qf2, 3, val));
    memstore.add(new KeyValue(row, fam, qf1, 3, val));
    assertEquals(3, memstore.getActive().getCellsCount());

    while (((CompactingMemStore)memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }

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
    memstore.add(new KeyValue(row, fam, qf2, 4, val));
    memstore.add(new KeyValue(row, fam, qf3, 4, val));
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

    String[] keys1 = { "A", "A", "B", "C" }; //A1, A2, B3, C4

    // test 1 bucket
    addRowsByKeys(memstore, keys1);
    assertEquals(496, regionServicesForStores.getGlobalMemstoreTotalSize());

    long size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore)memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(376, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(3, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  @Test
  public void testCompaction2Buckets() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };

    addRowsByKeys(memstore, keys1);

    assertEquals(496, regionServicesForStores.getGlobalMemstoreTotalSize());

    long size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore)memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(1000);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(376, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys2);
    assertEquals(752, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore)memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(496, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  @Test
  public void testCompaction3Buckets() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };
    String[] keys3 = { "D", "B", "B" };

    addRowsByKeys(memstore, keys1);
    assertEquals(496, region.getMemstoreSize());

    long size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact

    String tstStr = "\n\nFlushable size after first flush in memory:" + size
        + ". Is MemmStore in compaction?:" + ((CompactingMemStore)memstore).isMemStoreFlushingInMemory();
    while (((CompactingMemStore)memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(376, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys2);

    tstStr += " After adding second part of the keys. Memstore size: " +
        region.getMemstoreSize() + ", Memstore Total Size: " +
        regionServicesForStores.getGlobalMemstoreTotalSize() + "\n\n";

    assertEquals(752, regionServicesForStores.getGlobalMemstoreTotalSize());

    ((CompactingMemStore)memstore).disableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline without compaction
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(752, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys3);
    assertEquals(1128, regionServicesForStores.getGlobalMemstoreTotalSize());

    ((CompactingMemStore)memstore).enableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore)memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(496, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());

    //assertTrue(tstStr, false);
  }

  private void addRowsByKeys(final AbstractMemStore hmc, String[] keys) {
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    long size = hmc.getActive().getSize();
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      byte[] val = Bytes.toBytes(keys[i] + i);
      KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
      hmc.add(kv);
      LOG.debug("added kv: " + kv.getKeyString() + ", timestamp:" + kv.getTimestamp());
    }
    regionServicesForStores.addAndGetGlobalMemstoreSize(hmc.getActive().getSize() - size);
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
