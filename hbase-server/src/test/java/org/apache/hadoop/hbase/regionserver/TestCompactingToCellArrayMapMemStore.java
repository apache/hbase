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

import org.apache.hadoop.hbase.util.Threads;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;



/**
 * compacted memstore test case
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestCompactingToCellArrayMapMemStore extends TestCompactingMemStore {

  private static final Log LOG = LogFactory.getLog(TestCompactingToCellArrayMapMemStore.class);
  //private static MemStoreChunkPool chunkPool;
  //private HRegion region;
  //private RegionServicesForStores regionServicesForStores;
  //private HStore store;

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////

  @Override public void tearDown() throws Exception {
    chunkPool.clearChunks();
  }

  @Override public void setUp() throws Exception {
    compactingSetUp();
    Configuration conf = HBaseConfiguration.create();

    // set memstore to do data compaction and not to use the speculative scan
    conf.set("hbase.hregion.compacting.memstore.type", "data-compaction");

    this.memstore =
        new CompactingMemStore(conf, CellComparator.COMPARATOR, store,
            regionServicesForStores);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction tests
  //////////////////////////////////////////////////////////////////////////////
  public void testCompaction1Bucket() throws IOException {
    int counter = 0;
    String[] keys1 = { "A", "A", "B", "C" }; //A1, A2, B3, C4

    // test 1 bucket
    addRowsByKeys(memstore, keys1);
    assertEquals(496, regionServicesForStores.getGlobalMemstoreTotalSize());

    assertEquals(4, memstore.getActive().getCellsCount());
    long size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(264, regionServicesForStores.getGlobalMemstoreTotalSize());
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(3, counter);
    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(3, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  public void testCompaction2Buckets() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };

    addRowsByKeys(memstore, keys1);
    assertEquals(496, regionServicesForStores.getGlobalMemstoreTotalSize());
    long size = memstore.getFlushableSize();

//    assertTrue(
//        "\n\n<<< This is the active size with 4 keys - " + memstore.getActive().getSize()
//            + ". This is the memstore flushable size - " + size + "\n",false);

    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(1000);
    }
    int counter = 0;
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(3,counter);
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(264, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys2);
    assertEquals(640, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    int i = 0;
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
      if (i > 10000000) {
        ((CompactingMemStore) memstore).debug();
        assertTrue("\n\n<<< Infinite loop! :( \n", false);
      }
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    counter = 0;
    for ( Segment s : memstore.getSegments()) {
      counter += s.getCellsCount();
    }
    assertEquals(4,counter);
    assertEquals(368, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());
  }

  public void testCompaction3Buckets() throws IOException {

    String[] keys1 = { "A", "A", "B", "C" };
    String[] keys2 = { "A", "B", "D" };
    String[] keys3 = { "D", "B", "B" };

    addRowsByKeys(memstore, keys1);
    assertEquals(496, region.getMemstoreSize());

    long size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact

    String tstStr = "\n\nFlushable size after first flush in memory:" + size + ". Is MemmStore in compaction?:"
        + ((CompactingMemStore) memstore).isMemStoreFlushingInMemory();
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(264, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys2);

    tstStr += " After adding second part of the keys. Memstore size: " +
        region.getMemstoreSize() + ", Memstore Total Size: " +
        regionServicesForStores.getGlobalMemstoreTotalSize() + "\n\n";

    assertEquals(640, regionServicesForStores.getGlobalMemstoreTotalSize());

    ((CompactingMemStore) memstore).disableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline without compaction
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(640, regionServicesForStores.getGlobalMemstoreTotalSize());

    addRowsByKeys(memstore, keys3);
    assertEquals(1016, regionServicesForStores.getGlobalMemstoreTotalSize());

    ((CompactingMemStore) memstore).enableCompaction();
    size = memstore.getFlushableSize();
    ((CompactingMemStore) memstore).flushInMemory(); // push keys to pipeline and compact
    while (((CompactingMemStore) memstore).isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    assertEquals(0, memstore.getSnapshot().getCellsCount());
    assertEquals(384, regionServicesForStores.getGlobalMemstoreTotalSize());

    size = memstore.getFlushableSize();
    MemStoreSnapshot snapshot = memstore.snapshot(); // push keys to snapshot
    region.addAndGetGlobalMemstoreSize(-size);  // simulate flusher
    ImmutableSegment s = memstore.getSnapshot();
    assertEquals(4, s.getCellsCount());
    assertEquals(0, regionServicesForStores.getGlobalMemstoreTotalSize());

    memstore.clearSnapshot(snapshot.getId());

  }

  //////////////////////////////////////////////////////////////////////////////
  // Merging tests
  //////////////////////////////////////////////////////////////////////////////
  @Test
  public void testMerging() throws IOException {

    String[] keys1 = { "A", "A", "B", "C", "F", "H"};
    String[] keys2 = { "A", "B", "D", "G", "I", "J"};
    String[] keys3 = { "D", "B", "B", "E" };

    memstore.getConfiguration().set("hbase.hregion.compacting.memstore.type", "index-compaction");
    ((CompactingMemStore)memstore).initiateType();
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
    scanners.get(0).seek(KeyValue.LOWESTKEY);
    int count = 0;
    while (scanners.get(0).next() != null) {
      count++;
    }
    assertEquals("the count should be ", count, 150);
    scanners.get(0).close();
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
        CellComparator.COMPARATOR, 10, ((CompactingMemStore) memstore).getStore());
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
        hmc.add(kv);
      }
    }
  }

  private void addRowsByKeys(final AbstractMemStore hmc, String[] keys) {
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf = Bytes.toBytes("testqualifier");
    long size = hmc.getActive().size();//
    for (int i = 0; i < keys.length; i++) {
      long timestamp = System.currentTimeMillis();
      Threads.sleep(1); // to make sure each kv gets a different ts
      byte[] row = Bytes.toBytes(keys[i]);
      byte[] val = Bytes.toBytes(keys[i] + i);
      KeyValue kv = new KeyValue(row, fam, qf, timestamp, val);
      hmc.add(kv);
      LOG.debug("added kv: " + kv.getKeyString() + ", timestamp" + kv.getTimestamp());
    }
    regionServicesForStores.addAndGetGlobalMemstoreSize(hmc.getActive().size() - size);//
  }
}
