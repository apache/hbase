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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * compacted memstore test case
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestCompactingMemStore extends TestDefaultMemStore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactingMemStore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactingMemStore.class);
  protected static ChunkCreator chunkCreator;
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
    chunkCreator.clearChunksInPool();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    compactingSetUp();
    this.memstore = new MyCompactingMemStore(HBaseConfiguration.create(), CellComparator.getInstance(),
        store, regionServicesForStores, MemoryCompactionPolicy.EAGER);
    ((CompactingMemStore)memstore).setIndexType(CompactingMemStore.IndexType.ARRAY_MAP);
  }

  protected void compactingSetUp() throws Exception {
    super.internalSetUp();
    Configuration conf = new Configuration();
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 1000);
    HBaseTestingUtility hbaseUtility = HBaseTestingUtility.createLocalHTU(conf);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("foobar"));
    htd.addFamily(hcd);
    HRegionInfo info =
        new HRegionInfo(TableName.valueOf("foobar"), null, null, false);
    WAL wal = hbaseUtility.createWal(conf, hbaseUtility.getDataTestDir(), info);
    this.region = HRegion.createHRegion(info, hbaseUtility.getDataTestDir(), conf, htd, wal, true);
    this.regionServicesForStores = Mockito.spy(region.getRegionServicesForStores());
    ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    Mockito.when(regionServicesForStores.getInMemoryCompactionPool()).thenReturn(pool);
    this.store = new HStore(region, hcd, conf, false);

    long globalMemStoreLimit = (long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
        .getMax() * MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false));
    chunkCreator = ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false,
        globalMemStoreLimit, 0.4f, MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT,
            null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    assertNotNull(chunkCreator);
  }

  /**
   * A simple test which flush in memory affect timeOfOldestEdit
   */
  @Test
  public void testTimeOfOldestEdit() {
    assertEquals(Long.MAX_VALUE,  memstore.timeOfOldestEdit());
    final byte[] r = Bytes.toBytes("r");
    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes("v");
    final KeyValue kv = new KeyValue(r, f, q, v);
    memstore.add(kv, null);
    long timeOfOldestEdit = memstore.timeOfOldestEdit();
    assertNotEquals(Long.MAX_VALUE, timeOfOldestEdit);

    ((CompactingMemStore)memstore).flushInMemory();
    assertEquals(timeOfOldestEdit, memstore.timeOfOldestEdit());
    memstore.add(kv, null);
    assertEquals(timeOfOldestEdit, memstore.timeOfOldestEdit());
    memstore.snapshot();
    assertEquals(Long.MAX_VALUE, memstore.timeOfOldestEdit());
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
    // snapshot is empty,active segment is not empty,
    // empty segment is skipped.
    verifyOneScanAcrossSnapshot2(kv1, kv2);

    // use case 2: both kvs in snapshot
    this.memstore.snapshot();
    // active segment is empty,snapshot is not empty,
    // empty segment is skipped.
    verifyOneScanAcrossSnapshot2(kv1, kv2);

    // use case 3: first in snapshot second in kvset
    this.memstore = new CompactingMemStore(HBaseConfiguration.create(),
        CellComparator.getInstance(), store, regionServicesForStores,
        MemoryCompactionPolicy.EAGER);
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
    assertTrue(CellComparator.getInstance().compareRows(closestToEmpty,
        new KeyValue(Bytes.toBytes(0), System.currentTimeMillis())) == 0);
    for (int i = 0; i < ROW_COUNT; i++) {
      Cell nr = ((CompactingMemStore)this.memstore).getNextRow(new KeyValue(Bytes.toBytes(i),
          System.currentTimeMillis()));
      if (i + 1 == ROW_COUNT) {
        assertNull(nr);
      } else {
        assertTrue(CellComparator.getInstance().compareRows(nr,
            new KeyValue(Bytes.toBytes(i + 1), System.currentTimeMillis())) == 0);
      }
    }
    //starting from each row, validate results should contain the starting row
    Configuration conf = HBaseConfiguration.create();
    for (int startRowId = 0; startRowId < ROW_COUNT; startRowId++) {
      ScanInfo scanInfo = new ScanInfo(conf, FAMILY, 0, 1, Integer.MAX_VALUE,
          KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE, 0, this.memstore.getComparator(), false);
      try (InternalScanner scanner =
          new StoreScanner(new Scan().withStartRow(Bytes.toBytes(startRowId)), scanInfo, null,
              memstore.getScanners(0))) {
        List<Cell> results = new ArrayList<>();
        for (int i = 0; scanner.next(results); i++) {
          int rowId = startRowId + i;
          Cell left = results.get(0);
          byte[] row1 = Bytes.toBytes(rowId);
          assertTrue("Row name",
            CellComparator.getInstance().compareRows(left, row1, 0, row1.length) == 0);
          assertEquals("Count of columns", QUALIFIER_COUNT, results.size());
          List<Cell> row = new ArrayList<>();
          for (Cell kv : results) {
            row.add(kv);
          }
          isExpectedRowWithoutTimestamps(rowId, row);
          // Clear out set. Otherwise row results accumulate.
          results.clear();
        }
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
    MemStoreSize oldSize = memstore.size();

    List<Cell> l = new ArrayList<>();
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
    MemStoreSize newSize = this.memstore.size();
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
      assertEquals(Long.MAX_VALUE, t);

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
      List<Cell> l = new ArrayList<>();
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
    long oldHistorySize = hmc.getSnapshot().getDataSize();
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
    // close the scanners
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
    memstore.clearSnapshot(snapshot.getId());

    int chunkCount = chunkCreator.getPoolSize();
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
  public void testPuttingBackChunksWithOpeningPipelineScanner()
      throws IOException {

    // set memstore to do data compaction and not to use the speculative scan
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.EAGER;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((MyCompactingMemStore)memstore).initiateType(compactionType, memstore.getConfiguration());

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
    ((MyCompactingMemStore)memstore).disableCompaction();
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
    ((MyCompactingMemStore)memstore).enableCompaction();
    // trigger compaction
    ((CompactingMemStore)memstore).flushInMemory();

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf3, 3, val), null);
    memstore.add(new KeyValue(row, fam, qf2, 3, val), null);
    memstore.add(new KeyValue(row, fam, qf1, 3, val), null);
    assertEquals(3, memstore.getActive().getCellsCount());

    assertTrue(chunkCreator.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkCreator.getPoolSize() > 0);

    // clear chunks
    chunkCreator.clearChunksInPool();

    // Creating another snapshot

    MemStoreSnapshot snapshot = memstore.snapshot();
    // close the scanners
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
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
    // close the scanners
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
    memstore.clearSnapshot(snapshot.getId());
    assertTrue(chunkCreator.getPoolSize() > 0);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction tests
  //////////////////////////////////////////////////////////////////////////////
  @Test
  public void testCompaction1Bucket() throws IOException {

    // set memstore to do basic structure flattening, the "eager" option is tested in
    // TestCompactingToCellFlatMapMemStore
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration()
        .set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY, String.valueOf(compactionType));
    ((MyCompactingMemStore)memstore).initiateType(compactionType, memstore.getConfiguration());

    String[] keys1 = { "A", "A", "B", "C" }; //A1, A2, B3, C4

    // test 1 bucket
    int totalCellsLen = addRowsByKeys(memstore, keys1);
    int oneCellOnCSLMHeapSize = 120;
    int oneCellOnCAHeapSize = 88;
    long totalHeapSize = MutableSegment.DEEP_OVERHEAD + 4 * oneCellOnCSLMHeapSize;
    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());

    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // There is no compaction, as the compacting memstore type is basic.
    // totalCellsLen remains the same
    totalHeapSize = MutableSegment.DEEP_OVERHEAD + CellArrayImmutableSegment.DEEP_OVERHEAD_CAM
        + 4 * oneCellOnCAHeapSize;
    assertEquals(totalCellsLen, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());

    MemStoreSize mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    // simulate flusher
    region.decrMemStoreSize(mss);
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  @Test
  public void testCompaction2Buckets() throws IOException {

    // set memstore to do basic structure flattening, the "eager" option is tested in
    // TestCompactingToCellFlatMapMemStore
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.BASIC;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    memstore.getConfiguration().set(MemStoreCompactionStrategy.COMPACTING_MEMSTORE_THRESHOLD_KEY,
        String.valueOf(1));
    ((MyCompactingMemStore)memstore).initiateType(compactionType, memstore.getConfiguration());
    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };

    int totalCellsLen1 = addRowsByKeys(memstore, keys1);
    int oneCellOnCSLMHeapSize = 120;
    int oneCellOnCAHeapSize = 88;
    long totalHeapSize = MutableSegment.DEEP_OVERHEAD + 4 * oneCellOnCSLMHeapSize;

    assertEquals(totalCellsLen1, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());

    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    int counter = 0;
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(4, counter);
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // There is no compaction, as the compacting memstore type is basic.
    // totalCellsLen remains the same
    assertEquals(totalCellsLen1, regionServicesForStores.getMemStoreSize());
    totalHeapSize = MutableSegment.DEEP_OVERHEAD + CellArrayImmutableSegment.DEEP_OVERHEAD_CAM
        + 4 * oneCellOnCAHeapSize;
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());

    int totalCellsLen2 = addRowsByKeys(memstore, keys2);
    totalHeapSize += 3 * oneCellOnCSLMHeapSize;
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize, ((CompactingMemStore) memstore).heapSize());

    MemStoreSize mss = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    totalHeapSize = MutableSegment.DEEP_OVERHEAD + CellArrayImmutableSegment.DEEP_OVERHEAD_CAM
        + 7 * oneCellOnCAHeapSize;
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());

    mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    // simulate flusher
    region.decrMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(),
      mss.getCellsCount());
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(7, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  @Test
  public void testCompaction3Buckets() throws IOException {

    // set memstore to do data compaction and not to use the speculative scan
    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.EAGER;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    ((MyCompactingMemStore)memstore).initiateType(compactionType, memstore.getConfiguration());
    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };
    String[] keys3 = { "D", "B", "B" };

    int totalCellsLen1 = addRowsByKeys(memstore, keys1);// Adding 4 cells.
    int oneCellOnCSLMHeapSize = 120;
    int oneCellOnCAHeapSize = 88;
    assertEquals(totalCellsLen1, region.getMemStoreDataSize());
    long totalHeapSize = MutableSegment.DEEP_OVERHEAD + 4 * oneCellOnCSLMHeapSize;
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact

    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // One cell is duplicated and the compaction will remove it. All cells of same time so adjusting
    // totalCellsLen
    totalCellsLen1 = (totalCellsLen1 * 3) / 4;
    assertEquals(totalCellsLen1, regionServicesForStores.getMemStoreSize());
    // In memory flush to make a CellArrayMap instead of CSLM. See the overhead diff.
    totalHeapSize = MutableSegment.DEEP_OVERHEAD + CellArrayImmutableSegment.DEEP_OVERHEAD_CAM
        + 3 * oneCellOnCAHeapSize;
    assertEquals(totalHeapSize, ((CompactingMemStore)memstore).heapSize());

    int totalCellsLen2 = addRowsByKeys(memstore, keys2);// Adding 3 more cells.
    long totalHeapSize2 = totalHeapSize + 3 * oneCellOnCSLMHeapSize;

    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize2, ((CompactingMemStore) memstore).heapSize());

    ((MyCompactingMemStore) memstore).disableCompaction();
    MemStoreSize mss = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline without compaction
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // No change in the cells data size. ie. memstore size. as there is no compaction.
    assertEquals(totalCellsLen1 + totalCellsLen2, regionServicesForStores.getMemStoreSize());
    assertEquals(totalHeapSize2 + CellArrayImmutableSegment.DEEP_OVERHEAD_CAM,
        ((CompactingMemStore) memstore).heapSize());

    int totalCellsLen3 = addRowsByKeys(memstore, keys3);// 3 more cells added
    assertEquals(totalCellsLen1 + totalCellsLen2 + totalCellsLen3,
        regionServicesForStores.getMemStoreSize());
    long totalHeapSize3 = totalHeapSize2 + CellArrayImmutableSegment.DEEP_OVERHEAD_CAM
        + 3 * oneCellOnCSLMHeapSize;
    assertEquals(totalHeapSize3, ((CompactingMemStore) memstore).heapSize());

    ((MyCompactingMemStore)memstore).enableCompaction();
    mss = memstore.getFlushableSize();
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    // active flushed to pipeline and all 3 segments compacted. Will get rid of duplicated cells.
    // Out of total 10, only 4 cells are unique
    totalCellsLen2 = totalCellsLen2 / 3;// 2 out of 3 cells are duplicated
    totalCellsLen3 = 0;// All duplicated cells.
    assertEquals(totalCellsLen1 + totalCellsLen2 + totalCellsLen3,
        regionServicesForStores.getMemStoreSize());
    // Only 4 unique cells left
    assertEquals(4 * oneCellOnCAHeapSize + MutableSegment.DEEP_OVERHEAD
        + CellArrayImmutableSegment.DEEP_OVERHEAD_CAM, ((CompactingMemStore) memstore).heapSize());

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

  @Test
  public void testMagicCompaction3Buckets() throws IOException {

    MemoryCompactionPolicy compactionType = MemoryCompactionPolicy.ADAPTIVE;
    memstore.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(compactionType));
    memstore.getConfiguration().setDouble(
        AdaptiveMemStoreCompactionStrategy.ADAPTIVE_COMPACTION_THRESHOLD_KEY, 0.45);
    memstore.getConfiguration().setInt(
        AdaptiveMemStoreCompactionStrategy.COMPACTING_MEMSTORE_THRESHOLD_KEY, 2);
    memstore.getConfiguration().setInt(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 1);
    ((MyCompactingMemStore) memstore).initiateType(compactionType, memstore.getConfiguration());

    String[] keys1 = { "A", "B", "D" };
    String[] keys2 = { "A" };
    String[] keys3 = { "A", "A", "B", "C" };
    String[] keys4 = { "D", "B", "B" };

    int totalCellsLen1 = addRowsByKeys(memstore, keys1);// Adding 3 cells.
    int oneCellOnCSLMHeapSize = 120;
    assertEquals(totalCellsLen1, region.getMemStoreDataSize());
    long totalHeapSize = MutableSegment.DEEP_OVERHEAD + 3 * oneCellOnCSLMHeapSize;
    assertEquals(totalHeapSize, memstore.heapSize());

    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline - flatten
    assertEquals(3, ((CompactingMemStore) memstore).getImmutableSegments().getNumOfCells());
    assertEquals(1.0,
        ((CompactingMemStore) memstore).getImmutableSegments().getEstimatedUniquesFrac(), 0);
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    addRowsByKeys(memstore, keys2);// Adding 1 more cell - flatten.
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline without compaction
    assertEquals(4, ((CompactingMemStore) memstore).getImmutableSegments().getNumOfCells());
    assertEquals(1.0,
        ((CompactingMemStore) memstore).getImmutableSegments().getEstimatedUniquesFrac(), 0);
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    addRowsByKeys(memstore, keys3);// Adding 4 more cells - merge.
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline without compaction
    assertEquals(8, ((CompactingMemStore) memstore).getImmutableSegments().getNumOfCells());
    assertEquals((4.0 / 8.0),
        ((CompactingMemStore) memstore).getImmutableSegments().getEstimatedUniquesFrac(), 0);
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    addRowsByKeys(memstore, keys4);// 3 more cells added - compact (or not)
    ((CompactingMemStore)memstore).flushInMemory(); // push keys to pipeline and compact
    int numCells = ((CompactingMemStore) memstore).getImmutableSegments().getNumOfCells();
    assertTrue(4 == numCells || 11 == numCells);
    assertEquals(0, memstore.getSnapshot().getCellsCount());

    MemStoreSize mss = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    // simulate flusher
    region.decrMemStoreSize(mss);
    ImmutableSegment s = memstore.getSnapshot();
    numCells = s.getCellsCount();
    assertTrue(4 == numCells || 11 == numCells);
    assertEquals(0, regionServicesForStores.getMemStoreSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  protected int addRowsByKeys(final AbstractMemStore hmc, String[] keys) {
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    long size = hmc.getActive().getDataSize();
    long heapOverhead = hmc.getActive().getHeapSize();
    int cellsCount = hmc.getActive().getCellsCount();
    int totalLen = 0;
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      byte[] val = Bytes.toBytes(keys[i] + i);
      KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
      totalLen += Segment.getCellLength(kv);
      hmc.add(kv, null);
      LOG.debug("added kv: " + kv.getKeyString() + ", timestamp:" + kv.getTimestamp());
    }
    regionServicesForStores.addMemStoreSize(hmc.getActive().getDataSize() - size,
      hmc.getActive().getHeapSize() - heapOverhead, 0,
      hmc.getActive().getCellsCount() - cellsCount);
    return totalLen;
  }

  // for controlling the val size when adding a new cell
  protected int addRowsByKeys(final AbstractMemStore hmc, String[] keys, byte[] val) {
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    long size = hmc.getActive().getDataSize();
    long heapOverhead = hmc.getActive().getHeapSize();
    int cellsCount = hmc.getActive().getCellsCount();
    int totalLen = 0;
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
      totalLen += Segment.getCellLength(kv);
      hmc.add(kv, null);
      LOG.debug("added kv: " + kv.getKeyString() + ", timestamp:" + kv.getTimestamp());
    }
    regionServicesForStores.addMemStoreSize(hmc.getActive().getDataSize() - size,
      hmc.getActive().getHeapSize() - heapOverhead, 0, cellsCount);
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

  static protected class MyCompactingMemStore extends CompactingMemStore {

    public MyCompactingMemStore(Configuration conf, CellComparator c, HStore store,
        RegionServicesForStores regionServices, MemoryCompactionPolicy compactionPolicy)
        throws IOException {
      super(conf, c, store, regionServices, compactionPolicy);
    }

    void disableCompaction() {
      allowCompaction.set(false);
    }

    void enableCompaction() {
      allowCompaction.set(true);
    }
    void initiateType(MemoryCompactionPolicy compactionType, Configuration conf)
        throws IllegalArgumentIOException {
      compactor.initiateCompactionStrategy(compactionType, conf, "CF_TEST");
    }

  }
}
