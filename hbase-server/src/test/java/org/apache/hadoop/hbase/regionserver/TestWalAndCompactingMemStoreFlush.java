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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * This test verifies the correctness of the Per Column Family flushing strategy
 * when part of the memstores are compacted memstores
 */
@Category({ RegionServerTests.class, LargeTests.class })
public class TestWalAndCompactingMemStoreFlush {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWalAndCompactingMemStoreFlush.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Path DIR = TEST_UTIL.getDataTestDir("TestHRegion");
  public static final TableName TABLENAME = TableName.valueOf("TestWalAndCompactingMemStoreFlush",
      "t1");

  public static final byte[][] FAMILIES = { Bytes.toBytes("f1"), Bytes.toBytes("f2"),
      Bytes.toBytes("f3"), Bytes.toBytes("f4"), Bytes.toBytes("f5") };

  public static final byte[] FAMILY1 = FAMILIES[0];
  public static final byte[] FAMILY2 = FAMILIES[1];
  public static final byte[] FAMILY3 = FAMILIES[2];

  private Configuration conf;

  private HRegion initHRegion(String callingMethod, Configuration conf) throws IOException {
    int i = 0;
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    for (byte[] family : FAMILIES) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      // even column families are going to have compacted memstore
      if (i % 2 == 0) {
        hcd.setInMemoryCompaction(MemoryCompactionPolicy
            .valueOf(conf.get(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY)));
      } else {
        hcd.setInMemoryCompaction(MemoryCompactionPolicy.NONE);
      }
      htd.addFamily(hcd);
      i++;
    }

    HRegionInfo info = new HRegionInfo(TABLENAME, null, null, false);
    Path path = new Path(DIR, callingMethod);
    HRegion region = HBaseTestingUtility.createRegionAndWAL(info, path, conf, htd, false);
    region.regionServicesForStores = Mockito.spy(region.regionServicesForStores);
    ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    Mockito.when(region.regionServicesForStores.getInMemoryCompactionPool()).thenReturn(pool);
    region.initialize(null);
    return region;
  }

  // A helper function to create puts.
  private Put createPut(int familyNum, int putNum) {
    byte[] qf  = Bytes.toBytes("q" + familyNum);
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    Put p = new Put(row);
    p.addColumn(FAMILIES[familyNum - 1], qf, val);
    return p;
  }

  // A helper function to create double puts, so something can be compacted later.
  private Put createDoublePut(int familyNum, int putNum) {
    byte[] qf  = Bytes.toBytes("q" + familyNum);
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    Put p = new Put(row);
    // add twice with different timestamps
    p.addColumn(FAMILIES[familyNum - 1], qf, 10, val);
    p.addColumn(FAMILIES[familyNum - 1], qf, 20, val);
    return p;
  }

  private void verifyInMemoryFlushSize(Region region) {
    assertEquals(
      ((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore).getInmemoryFlushSize(),
      ((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore).getInmemoryFlushSize());
  }

  @Before
  public void setup() {
    conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
        FlushNonSloppyStoresFirstPolicy.class.getName());
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.5);
  }

  @Test
  public void testSelectiveFlushWithEager() throws IOException {
    // Set up the configuration
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 300 * 1024);
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 75 * 1024);
    // set memstore to do data compaction
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(MemoryCompactionPolicy.EAGER));

    // Intialize the region
    HRegion region = initHRegion("testSelectiveFlushWithEager", conf);
    verifyInMemoryFlushSize(region);
    // Add 1200 entries for CF1, 100 for CF2 and 50 for CF3
    for (int i = 1; i <= 1200; i++) {
      region.put(createPut(1, i));        // compacted memstore, all the keys are unique

      if (i <= 100) {
        region.put(createPut(2, i));
        if (i <= 50) {
          // compacted memstore, subject for compaction due to duplications
          region.put(createDoublePut(3, i));
        }
      }
    }

    // Now add more puts for CF2, so that we only flush CF2 (DefaultMemStore) to disk
    for (int i = 100; i < 2000; i++) {
      region.put(createPut(2, i));
    }

    long totalMemstoreSize = region.getMemStoreDataSize();

    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF1PhaseI = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseI = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseI = region.getOldestSeqIdOfStore(FAMILY3);

    // Find the sizes of the memstores of each CF.
    MemStoreSize cf1MemstoreSizePhaseI = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseI = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseI = region.getStore(FAMILY3).getMemStoreSize();

    // Get the overall smallest LSN in the region's memstores.
    long smallestSeqInRegionCurrentMemstorePhaseI = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    String s = "\n\n----------------------------------\n"
        + "Upon initial insert and before any flush, size of CF1 is:"
        + cf1MemstoreSizePhaseI + ", is CF1 compacted memstore?:"
        + region.getStore(FAMILY1).isSloppyMemStore() + ". Size of CF2 is:"
        + cf2MemstoreSizePhaseI + ", is CF2 compacted memstore?:"
        + region.getStore(FAMILY2).isSloppyMemStore() + ". Size of CF3 is:"
        + cf3MemstoreSizePhaseI + ", is CF3 compacted memstore?:"
        + region.getStore(FAMILY3).isSloppyMemStore() + "\n";

    // The overall smallest LSN in the region's memstores should be the same as
    // the LSN of the smallest edit in CF1
    assertEquals(smallestSeqCF1PhaseI, smallestSeqInRegionCurrentMemstorePhaseI);

    // Some other sanity checks.
    assertTrue(smallestSeqCF1PhaseI < smallestSeqCF2PhaseI);
    assertTrue(smallestSeqCF2PhaseI < smallestSeqCF3PhaseI);
    assertTrue(cf1MemstoreSizePhaseI.getDataSize() > 0);
    assertTrue(cf2MemstoreSizePhaseI.getDataSize() > 0);
    assertTrue(cf3MemstoreSizePhaseI.getDataSize() > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    String msg = "totalMemstoreSize="+totalMemstoreSize +
        " cf1MemstoreSizePhaseI="+cf1MemstoreSizePhaseI +
        " cf2MemstoreSizePhaseI="+cf2MemstoreSizePhaseI +
        " cf3MemstoreSizePhaseI="+cf3MemstoreSizePhaseI ;
    assertEquals(msg, totalMemstoreSize, cf1MemstoreSizePhaseI.getDataSize()
        + cf2MemstoreSizePhaseI.getDataSize() + cf3MemstoreSizePhaseI.getDataSize());

    // Flush!!!!!!!!!!!!!!!!!!!!!!
    // We have big compacting memstore CF1 and two small memstores:
    // CF2 (not compacted) and CF3 (compacting)
    // All together they are above the flush size lower bound.
    // Since CF1 and CF3 should be flushed to memory (not to disk),
    // CF2 is going to be flushed to disk.
    // CF1 - nothing to compact (but flattening), CF3 - should be twice compacted
    CompactingMemStore cms1 = (CompactingMemStore) ((HStore) region.getStore(FAMILY1)).memstore;
    CompactingMemStore cms3 = (CompactingMemStore) ((HStore) region.getStore(FAMILY3)).memstore;
    cms1.flushInMemory();
    cms3.flushInMemory();
    region.flush(false);

    // Recalculate everything
    MemStoreSize cf1MemstoreSizePhaseII = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseII = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseII = region.getStore(FAMILY3).getMemStoreSize();

    long smallestSeqInRegionCurrentMemstorePhaseII = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF1PhaseII = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseII = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseII = region.getOldestSeqIdOfStore(FAMILY3);

    s = s + "\n----After first flush! CF1 should be flushed to memory, but not compacted.---\n"
        + "Size of CF1 is:" + cf1MemstoreSizePhaseII + ", size of CF2 is:" + cf2MemstoreSizePhaseII
        + ", size of CF3 is:" + cf3MemstoreSizePhaseII + "\n";

    // CF1 was flushed to memory, but there is nothing to compact, and CF1 was flattened
    assertTrue(cf1MemstoreSizePhaseII.getDataSize() == cf1MemstoreSizePhaseI.getDataSize());
    assertTrue(cf1MemstoreSizePhaseII.getHeapSize() < cf1MemstoreSizePhaseI.getHeapSize());

    // CF2 should become empty
    assertEquals(0, cf2MemstoreSizePhaseII.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSizePhaseII.getHeapSize());

    // verify that CF3 was flushed to memory and was compacted (this is approximation check)
    assertTrue(cf3MemstoreSizePhaseI.getDataSize() > cf3MemstoreSizePhaseII.getDataSize());
    assertTrue(
        cf3MemstoreSizePhaseI.getHeapSize() / 2 > cf3MemstoreSizePhaseII.getHeapSize());

    // Now the smallest LSN in the region should be the same as the smallest
    // LSN in the memstore of CF1.
    assertEquals(smallestSeqInRegionCurrentMemstorePhaseII, smallestSeqCF1PhaseI);

    // Now add more puts for CF1, so that we also flush CF1 to disk instead of
    // memory in next flush
    for (int i = 1200; i < 3000; i++) {
      region.put(createPut(1, i));
    }

    s = s + "The smallest sequence in region WAL is: " + smallestSeqInRegionCurrentMemstorePhaseII
        + ", the smallest sequence in CF1:" + smallestSeqCF1PhaseII + ", " +
        "the smallest sequence in CF2:"
        + smallestSeqCF2PhaseII +", the smallest sequence in CF3:" + smallestSeqCF3PhaseII + "\n";

    // How much does the CF1 memstore occupy? Will be used later.
    MemStoreSize cf1MemstoreSizePhaseIII = region.getStore(FAMILY1).getMemStoreSize();
    long smallestSeqCF1PhaseIII = region.getOldestSeqIdOfStore(FAMILY1);

    s = s + "----After more puts into CF1 its size is:" + cf1MemstoreSizePhaseIII
        + ", and its sequence is:" + smallestSeqCF1PhaseIII + " ----\n" ;


    // Flush!!!!!!!!!!!!!!!!!!!!!!
    // Flush again, CF1 is flushed to disk
    // CF2 is flushed to disk, because it is not in-memory compacted memstore
    // CF3 is flushed empty to memory (actually nothing happens to CF3)
    region.flush(false);

    // Recalculate everything
    MemStoreSize cf1MemstoreSizePhaseIV = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseIV = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseIV = region.getStore(FAMILY3).getMemStoreSize();

    long smallestSeqInRegionCurrentMemstorePhaseIV = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF1PhaseIV = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseIV = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseIV = region.getOldestSeqIdOfStore(FAMILY3);

    s = s + "----After SECOND FLUSH, CF1 size is:" + cf1MemstoreSizePhaseIV + ", CF2 size is:"
        + cf2MemstoreSizePhaseIV + " and CF3 size is:" + cf3MemstoreSizePhaseIV
        + "\n";

    s = s + "The smallest sequence in region WAL is: " + smallestSeqInRegionCurrentMemstorePhaseIV
        + ", the smallest sequence in CF1:" + smallestSeqCF1PhaseIV + ", " +
        "the smallest sequence in CF2:"
        + smallestSeqCF2PhaseIV +", the smallest sequence in CF3:" + smallestSeqCF3PhaseIV
        + "\n";

    // CF1's pipeline component (inserted before first flush) should be flushed to disk
    // CF2 should be flushed to disk
    assertTrue(cf1MemstoreSizePhaseIII.getDataSize() > cf1MemstoreSizePhaseIV.getDataSize());
    assertEquals(0, cf2MemstoreSizePhaseIV.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSizePhaseIV.getHeapSize());

    // CF3 shouldn't have been touched.
    assertEquals(cf3MemstoreSizePhaseIV, cf3MemstoreSizePhaseII);

    // the smallest LSN of CF3 shouldn't change
    assertEquals(smallestSeqCF3PhaseII, smallestSeqCF3PhaseIV);

    // CF3 should be bottleneck for WAL
    assertEquals(s, smallestSeqInRegionCurrentMemstorePhaseIV, smallestSeqCF3PhaseIV);

    // Flush!!!!!!!!!!!!!!!!!!!!!!
    // Trying to clean the existing memstores, CF2 all flushed to disk. The single
    // memstore segment in the compaction pipeline of CF1 and CF3 should be flushed to disk.
    region.flush(true);

    // Recalculate everything
    MemStoreSize cf1MemstoreSizePhaseV = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseV = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseV = region.getStore(FAMILY3).getMemStoreSize();
    long smallestSeqInRegionCurrentMemstorePhaseV = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    assertEquals(0, cf1MemstoreSizePhaseV.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf1MemstoreSizePhaseV.getHeapSize());
    assertEquals(0, cf2MemstoreSizePhaseV.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSizePhaseV.getHeapSize());
    assertEquals(0, cf3MemstoreSizePhaseV.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf3MemstoreSizePhaseV.getHeapSize());

    // What happens when we hit the memstore limit, but we are not able to find
    // any Column Family above the threshold?
    // In that case, we should flush all the CFs.

    // The memstore limit is 100*1024 and the column family flush threshold is
    // around 25*1024. We try to just hit the memstore limit with each CF's
    // memstore being below the CF flush threshold.
    for (int i = 1; i <= 300; i++) {
      region.put(createPut(1, i));
      region.put(createPut(2, i));
      region.put(createPut(3, i));
      region.put(createPut(4, i));
      region.put(createPut(5, i));
    }

    region.flush(false);

    s = s + "----AFTER THIRD AND FORTH FLUSH, The smallest sequence in region WAL is: "
        + smallestSeqInRegionCurrentMemstorePhaseV
        + ". After additional inserts and last flush, the entire region size is:" + region
        .getMemStoreDataSize()
        + "\n----------------------------------\n";

    // Since we won't find any CF above the threshold, and hence no specific
    // store to flush, we should flush all the memstores
    // Also compacted memstores are flushed to disk.
    assertEquals(0, region.getMemStoreDataSize());
    System.out.println(s);
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  /*------------------------------------------------------------------------------*/
  /* Check the same as above but for index-compaction type of compacting memstore */
  @Test
  public void testSelectiveFlushWithIndexCompaction() throws IOException {
    /*------------------------------------------------------------------------------*/
    /* SETUP */
    // Set up the configuration
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 300 * 1024);
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 75 * 1024);
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.5);
    // set memstore to index-compaction
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(MemoryCompactionPolicy.BASIC));

    // Initialize the region
    HRegion region = initHRegion("testSelectiveFlushWithIndexCompaction", conf);
    verifyInMemoryFlushSize(region);
    /*------------------------------------------------------------------------------*/
    /* PHASE I - insertions */
    // Add 1200 entries for CF1, 100 for CF2 and 50 for CF3
    for (int i = 1; i <= 1200; i++) {
      region.put(createPut(1, i));        // compacted memstore
      if (i <= 100) {
        region.put(createPut(2, i));
        if (i <= 50) {
          region.put(createDoublePut(3, i)); // subject for in-memory compaction
        }
      }
    }
    // Now add more puts for CF2, so that we only flush CF2 to disk
    for (int i = 100; i < 2000; i++) {
      region.put(createPut(2, i));
    }

    /*------------------------------------------------------------------------------*/
    /*------------------------------------------------------------------------------*/
    /* PHASE I - collect sizes */
    long totalMemstoreSizePhaseI = region.getMemStoreDataSize();
    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF1PhaseI = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseI = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseI = region.getOldestSeqIdOfStore(FAMILY3);
    // Find the sizes of the memstores of each CF.
    MemStoreSize cf1MemstoreSizePhaseI = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseI = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseI = region.getStore(FAMILY3).getMemStoreSize();
    // Get the overall smallest LSN in the region's memstores.
    long smallestSeqInRegionCurrentMemstorePhaseI = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    /*------------------------------------------------------------------------------*/
    /* PHASE I - validation */
    // The overall smallest LSN in the region's memstores should be the same as
    // the LSN of the smallest edit in CF1
    assertEquals(smallestSeqCF1PhaseI, smallestSeqInRegionCurrentMemstorePhaseI);
    // Some other sanity checks.
    assertTrue(smallestSeqCF1PhaseI < smallestSeqCF2PhaseI);
    assertTrue(smallestSeqCF2PhaseI < smallestSeqCF3PhaseI);
    assertTrue(cf1MemstoreSizePhaseI.getDataSize() > 0);
    assertTrue(cf2MemstoreSizePhaseI.getDataSize() > 0);
    assertTrue(cf3MemstoreSizePhaseI.getDataSize() > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    assertEquals(totalMemstoreSizePhaseI, cf1MemstoreSizePhaseI.getDataSize()
        + cf2MemstoreSizePhaseI.getDataSize() + cf3MemstoreSizePhaseI.getDataSize());

    /*------------------------------------------------------------------------------*/
    /* PHASE I - Flush */
    // First Flush in Test!!!!!!!!!!!!!!!!!!!!!!
    // CF1, CF2, CF3, all together they are above the flush size lower bound.
    // Since CF1 and CF3 are compacting, CF2 is going to be flushed to disk.
    // CF1 and CF3 - flushed to memory and flatten explicitly
    region.flush(false);
    CompactingMemStore cms1 = (CompactingMemStore) ((HStore) region.getStore(FAMILY1)).memstore;
    CompactingMemStore cms3 = (CompactingMemStore) ((HStore) region.getStore(FAMILY3)).memstore;
    cms1.flushInMemory();
    cms3.flushInMemory();

    // CF3/CF1 should be merged so wait here to be sure the compaction is done
    while (((CompactingMemStore) ((HStore) region.getStore(FAMILY1)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    while (((CompactingMemStore) ((HStore) region.getStore(FAMILY3)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }

    /*------------------------------------------------------------------------------*/
    /*------------------------------------------------------------------------------*/
    /* PHASE II - collect sizes */
    // Recalculate everything
    MemStoreSize cf1MemstoreSizePhaseII = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseII = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseII = region.getStore(FAMILY3).getMemStoreSize();
    long smallestSeqInRegionCurrentMemstorePhaseII = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF3PhaseII = region.getOldestSeqIdOfStore(FAMILY3);
    long totalMemstoreSizePhaseII = region.getMemStoreDataSize();

    /*------------------------------------------------------------------------------*/
    /* PHASE II - validation */
    // CF1 was flushed to memory, should be flattened and take less space
    assertEquals(cf1MemstoreSizePhaseII.getDataSize() , cf1MemstoreSizePhaseI.getDataSize());
    assertTrue(cf1MemstoreSizePhaseII.getHeapSize() < cf1MemstoreSizePhaseI.getHeapSize());
    // CF2 should become empty
    assertEquals(0, cf2MemstoreSizePhaseII.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSizePhaseII.getHeapSize());
    // verify that CF3 was flushed to memory and was not compacted (this is an approximation check)
    // if compacted CF# should be at least twice less because its every key was duplicated
    assertEquals(cf3MemstoreSizePhaseII.getDataSize() , cf3MemstoreSizePhaseI.getDataSize());
    assertTrue(cf3MemstoreSizePhaseI.getHeapSize() / 2 < cf3MemstoreSizePhaseII.getHeapSize());

    // Now the smallest LSN in the region should be the same as the smallest
    // LSN in the memstore of CF1.
    assertEquals(smallestSeqInRegionCurrentMemstorePhaseII, smallestSeqCF1PhaseI);
    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3. Counting the empty active segments in CF1/2/3 and pipeline
    // items in CF1/2
    assertEquals(totalMemstoreSizePhaseII, cf1MemstoreSizePhaseII.getDataSize()
        + cf2MemstoreSizePhaseII.getDataSize() + cf3MemstoreSizePhaseII.getDataSize());

    /*------------------------------------------------------------------------------*/
    /*------------------------------------------------------------------------------*/
    /* PHASE III - insertions */
    // Now add more puts for CF1, so that we also flush CF1 to disk instead of
    // memory in next flush. This is causing the CF! to be flushed to memory twice.
    for (int i = 1200; i < 8000; i++) {
      region.put(createPut(1, i));
    }

    // CF1 should be flatten and merged so wait here to be sure the compaction is done
    while (((CompactingMemStore) ((HStore) region.getStore(FAMILY1)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }

    /*------------------------------------------------------------------------------*/
    /* PHASE III - collect sizes */
    // How much does the CF1 memstore occupy now? Will be used later.
    MemStoreSize cf1MemstoreSizePhaseIII = region.getStore(FAMILY1).getMemStoreSize();
    long totalMemstoreSizePhaseIII = region.getMemStoreDataSize();

    /*------------------------------------------------------------------------------*/
    /* PHASE III - validation */
    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3. Counting the empty active segments in CF1/2/3 and pipeline
    // items in CF1/2
    assertEquals(totalMemstoreSizePhaseIII, cf1MemstoreSizePhaseIII.getDataSize()
        + cf2MemstoreSizePhaseII.getDataSize() + cf3MemstoreSizePhaseII.getDataSize());

    /*------------------------------------------------------------------------------*/
    /* PHASE III - Flush */
    // Second Flush in Test!!!!!!!!!!!!!!!!!!!!!!
    // CF1 is flushed to disk, but not entirely emptied.
    // CF2 was and remained empty, same way nothing happens to CF3
    region.flush(false);

    /*------------------------------------------------------------------------------*/
    /*------------------------------------------------------------------------------*/
    /* PHASE IV - collect sizes */
    // Recalculate everything
    MemStoreSize cf1MemstoreSizePhaseIV = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseIV = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseIV = region.getStore(FAMILY3).getMemStoreSize();
    long smallestSeqInRegionCurrentMemstorePhaseIV = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF3PhaseIV = region.getOldestSeqIdOfStore(FAMILY3);

    /*------------------------------------------------------------------------------*/
    /* PHASE IV - validation */
    // CF1's biggest pipeline component (inserted before first flush) should be flushed to disk
    // CF2 should remain empty
    assertTrue(cf1MemstoreSizePhaseIII.getDataSize() > cf1MemstoreSizePhaseIV.getDataSize());
    assertEquals(0, cf2MemstoreSizePhaseIV.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSizePhaseIV.getHeapSize());
    // CF3 shouldn't have been touched.
    assertEquals(cf3MemstoreSizePhaseIV, cf3MemstoreSizePhaseII);
    // the smallest LSN of CF3 shouldn't change
    assertEquals(smallestSeqCF3PhaseII, smallestSeqCF3PhaseIV);
    // CF3 should be bottleneck for WAL
    assertEquals(smallestSeqInRegionCurrentMemstorePhaseIV, smallestSeqCF3PhaseIV);

    /*------------------------------------------------------------------------------*/
    /* PHASE IV - Flush */
    // Third Flush in Test!!!!!!!!!!!!!!!!!!!!!!
    // Force flush to disk on all memstores (flush parameter true).
    // CF1/CF3 all flushed to disk. Note that active sets of CF1 and CF3 are empty
    region.flush(true);

    /*------------------------------------------------------------------------------*/
    /*------------------------------------------------------------------------------*/
    /* PHASE V - collect sizes */
    // Recalculate everything
    MemStoreSize cf1MemstoreSizePhaseV = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseV = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseV = region.getStore(FAMILY3).getMemStoreSize();
    long smallestSeqInRegionCurrentMemstorePhaseV = getWAL(region)
        .getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long totalMemstoreSizePhaseV = region.getMemStoreDataSize();

    /*------------------------------------------------------------------------------*/
    /* PHASE V - validation */
    assertEquals(0, cf1MemstoreSizePhaseV.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf1MemstoreSizePhaseV.getHeapSize());
    assertEquals(0, cf2MemstoreSizePhaseV.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSizePhaseV.getHeapSize());
    assertEquals(0, cf3MemstoreSizePhaseV.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf3MemstoreSizePhaseV.getHeapSize());
    // The total memstores size should be empty
    assertEquals(0, totalMemstoreSizePhaseV);
    // Because there is nothing in any memstore the WAL's LSN should be -1
    assertEquals(HConstants.NO_SEQNUM, smallestSeqInRegionCurrentMemstorePhaseV);

    // What happens when we hit the memstore limit, but we are not able to find
    // any Column Family above the threshold?
    // In that case, we should flush all the CFs.

    /*------------------------------------------------------------------------------*/
    /*------------------------------------------------------------------------------*/
    /* PHASE VI - insertions */
    // The memstore limit is 200*1024 and the column family flush threshold is
    // around 50*1024. We try to just hit the memstore limit with each CF's
    // memstore being below the CF flush threshold.
    for (int i = 1; i <= 300; i++) {
      region.put(createPut(1, i));
      region.put(createPut(2, i));
      region.put(createPut(3, i));
      region.put(createPut(4, i));
      region.put(createPut(5, i));
    }

    MemStoreSize cf1ActiveSizePhaseVI = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf3ActiveSizePhaseVI = region.getStore(FAMILY3).getMemStoreSize();
    MemStoreSize cf5ActiveSizePhaseVI = region.getStore(FAMILIES[4]).getMemStoreSize();

    /*------------------------------------------------------------------------------*/
    /* PHASE VI - Flush */
    // Fourth Flush in Test!!!!!!!!!!!!!!!!!!!!!!
    // None among compacting memstores was flushed to memory due to previous puts.
    // But is going to be moved to pipeline and flatten due to the flush.
    region.flush(false);
    // Since we won't find any CF above the threshold, and hence no specific
    // store to flush, we should flush all the memstores
    // Also compacted memstores are flushed to disk, but not entirely emptied
    MemStoreSize cf1ActiveSizePhaseVII = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf3ActiveSizePhaseVII = region.getStore(FAMILY3).getMemStoreSize();
    MemStoreSize cf5ActiveSizePhaseVII = region.getStore(FAMILIES[4]).getMemStoreSize();

    assertTrue(cf1ActiveSizePhaseVII.getDataSize() < cf1ActiveSizePhaseVI.getDataSize());
    assertTrue(cf3ActiveSizePhaseVII.getDataSize() < cf3ActiveSizePhaseVI.getDataSize());
    assertTrue(cf5ActiveSizePhaseVII.getDataSize() < cf5ActiveSizePhaseVI.getDataSize());

    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  @Test
  public void testSelectiveFlushAndWALinDataCompaction() throws IOException {
    // Set up the configuration
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 300 * 1024);
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 75 * 1024);
    // set memstore to do data compaction and not to use the speculative scan
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(MemoryCompactionPolicy.EAGER));

    // Intialize the HRegion
    HRegion region = initHRegion("testSelectiveFlushAndWALinDataCompaction", conf);
    verifyInMemoryFlushSize(region);
    // Add 1200 entries for CF1, 100 for CF2 and 50 for CF3
    for (int i = 1; i <= 1200; i++) {
      region.put(createPut(1, i));
      if (i <= 100) {
        region.put(createPut(2, i));
        if (i <= 50) {
          region.put(createPut(3, i));
        }
      }
    }
    // Now add more puts for CF2, so that we only flush CF2 to disk
    for (int i = 100; i < 2000; i++) {
      region.put(createPut(2, i));
    }

    // in this test check the non-composite snapshot - flashing only tail of the pipeline
    ((CompactingMemStore) ((HStore) region.getStore(FAMILY1)).memstore).setCompositeSnapshot(false);
    ((CompactingMemStore) ((HStore) region.getStore(FAMILY3)).memstore).setCompositeSnapshot(false);

    long totalMemstoreSize = region.getMemStoreDataSize();

    // Find the sizes of the memstores of each CF.
    MemStoreSize cf1MemstoreSizePhaseI = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseI = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseI = region.getStore(FAMILY3).getMemStoreSize();

    // Some other sanity checks.
    assertTrue(cf1MemstoreSizePhaseI.getDataSize() > 0);
    assertTrue(cf2MemstoreSizePhaseI.getDataSize() > 0);
    assertTrue(cf3MemstoreSizePhaseI.getDataSize() > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    String msg = "totalMemstoreSize="+totalMemstoreSize +
        " DefaultMemStore.DEEP_OVERHEAD="+DefaultMemStore.DEEP_OVERHEAD +
        " cf1MemstoreSizePhaseI="+cf1MemstoreSizePhaseI +
        " cf2MemstoreSizePhaseI="+cf2MemstoreSizePhaseI +
        " cf3MemstoreSizePhaseI="+cf3MemstoreSizePhaseI ;
    assertEquals(msg, totalMemstoreSize, cf1MemstoreSizePhaseI.getDataSize()
        + cf2MemstoreSizePhaseI.getDataSize() + cf3MemstoreSizePhaseI.getDataSize());

    // Flush!
    CompactingMemStore cms1 = (CompactingMemStore) ((HStore) region.getStore(FAMILY1)).memstore;
    CompactingMemStore cms3 = (CompactingMemStore) ((HStore) region.getStore(FAMILY3)).memstore;
    cms1.flushInMemory();
    cms3.flushInMemory();
    region.flush(false);

    MemStoreSize cf2MemstoreSizePhaseII = region.getStore(FAMILY2).getMemStoreSize();

    long smallestSeqInRegionCurrentMemstorePhaseII =
        region.getWAL().getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF1PhaseII = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseII = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseII = region.getOldestSeqIdOfStore(FAMILY3);

    // CF2 should have been cleared
    assertEquals(0, cf2MemstoreSizePhaseII.getDataSize());
    assertEquals(MutableSegment.DEEP_OVERHEAD, cf2MemstoreSizePhaseII.getHeapSize());

    String s = "\n\n----------------------------------\n"
        + "Upon initial insert and flush, LSN of CF1 is:"
        + smallestSeqCF1PhaseII + ". LSN of CF2 is:"
        + smallestSeqCF2PhaseII + ". LSN of CF3 is:"
        + smallestSeqCF3PhaseII + ", smallestSeqInRegionCurrentMemstore:"
        + smallestSeqInRegionCurrentMemstorePhaseII + "\n";

    // Add same entries to compact them later
    for (int i = 1; i <= 1200; i++) {
      region.put(createPut(1, i));
      if (i <= 100) {
        region.put(createPut(2, i));
        if (i <= 50) {
          region.put(createPut(3, i));
        }
      }
    }
    // Now add more puts for CF2, so that we only flush CF2 to disk
    for (int i = 100; i < 2000; i++) {
      region.put(createPut(2, i));
    }

    long smallestSeqInRegionCurrentMemstorePhaseIII =
        region.getWAL().getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF1PhaseIII = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseIII = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseIII = region.getOldestSeqIdOfStore(FAMILY3);

    s = s + "The smallest sequence in region WAL is: " + smallestSeqInRegionCurrentMemstorePhaseIII
        + ", the smallest sequence in CF1:" + smallestSeqCF1PhaseIII + ", " +
        "the smallest sequence in CF2:"
        + smallestSeqCF2PhaseIII +", the smallest sequence in CF3:" + smallestSeqCF3PhaseIII + "\n";

    // Flush!
    cms1 = (CompactingMemStore) ((HStore) region.getStore(FAMILY1)).memstore;
    cms3 = (CompactingMemStore) ((HStore) region.getStore(FAMILY3)).memstore;
    cms1.flushInMemory();
    cms3.flushInMemory();
    region.flush(false);

    long smallestSeqInRegionCurrentMemstorePhaseIV =
        region.getWAL().getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF1PhaseIV = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseIV = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseIV = region.getOldestSeqIdOfStore(FAMILY3);

    s = s + "The smallest sequence in region WAL is: " + smallestSeqInRegionCurrentMemstorePhaseIV
        + ", the smallest sequence in CF1:" + smallestSeqCF1PhaseIV + ", " +
        "the smallest sequence in CF2:"
        + smallestSeqCF2PhaseIV +", the smallest sequence in CF3:" + smallestSeqCF3PhaseIV + "\n";

    // now check that the LSN of the entire WAL, of CF1 and of CF3 has progressed due to compaction
    assertTrue(s, smallestSeqInRegionCurrentMemstorePhaseIV >
        smallestSeqInRegionCurrentMemstorePhaseIII);
    assertTrue(smallestSeqCF1PhaseIV > smallestSeqCF1PhaseIII);
    assertTrue(smallestSeqCF3PhaseIV > smallestSeqCF3PhaseIII);

    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  @Test
  public void testSelectiveFlushWithBasicAndMerge() throws IOException {
    // Set up the configuration
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 300 * 1024);
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 75 * 1024);
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.8);
    // set memstore to do index compaction with merge
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(MemoryCompactionPolicy.BASIC));
    // length of pipeline that requires merge
    conf.setInt(MemStoreCompactionStrategy.COMPACTING_MEMSTORE_THRESHOLD_KEY, 1);

    // Intialize the HRegion
    HRegion region = initHRegion("testSelectiveFlushWithBasicAndMerge", conf);
    verifyInMemoryFlushSize(region);
    // Add 1200 entries for CF1 (CompactingMemStore), 100 for CF2 (DefaultMemStore) and 50 for CF3
    for (int i = 1; i <= 1200; i++) {
      region.put(createPut(1, i));
      if (i <= 100) {
        region.put(createPut(2, i));
        if (i <= 50) {
          region.put(createPut(3, i));
        }
      }
    }
    // Now put more entries to CF2
    for (int i = 100; i < 2000; i++) {
      region.put(createPut(2, i));
    }

    long totalMemstoreSize = region.getMemStoreDataSize();

    // test in-memory flashing into CAM here
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore).setIndexType(
        CompactingMemStore.IndexType.ARRAY_MAP);
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore).setIndexType(
        CompactingMemStore.IndexType.ARRAY_MAP);

    // Find the sizes of the memstores of each CF.
    MemStoreSize cf1MemstoreSizePhaseI = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseI = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseI = region.getStore(FAMILY3).getMemStoreSize();

    // Some other sanity checks.
    assertTrue(cf1MemstoreSizePhaseI.getDataSize() > 0);
    assertTrue(cf2MemstoreSizePhaseI.getDataSize() > 0);
    assertTrue(cf3MemstoreSizePhaseI.getDataSize() > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    assertEquals(totalMemstoreSize,
        cf1MemstoreSizePhaseI.getDataSize() + cf2MemstoreSizePhaseI.getDataSize()
            + cf3MemstoreSizePhaseI.getDataSize());

    // Initiate in-memory Flush!
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore).flushInMemory();
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore).flushInMemory();
    // CF1 and CF3 should be flatten and merged so wait here to be sure the merge is done
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }

    // Flush-to-disk! CF2 only should be flushed
    region.flush(false);

    MemStoreSize cf1MemstoreSizePhaseII = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseII = region.getStore(FAMILY2).getMemStoreSize();
    MemStoreSize cf3MemstoreSizePhaseII = region.getStore(FAMILY3).getMemStoreSize();

    // CF1 should be flushed in memory and just flattened, so CF1 heap overhead should be smaller
    assertTrue(cf1MemstoreSizePhaseI.getHeapSize() > cf1MemstoreSizePhaseII.getHeapSize());
    // CF1 should be flushed in memory and just flattened, so CF1 data size should remain the same
    assertEquals(cf1MemstoreSizePhaseI.getDataSize(), cf1MemstoreSizePhaseII.getDataSize());
    // CF2 should have been cleared
    assertEquals(0, cf2MemstoreSizePhaseII.getDataSize());

    // Add the same amount of entries to see the merging
    for (int i = 1; i <= 1200; i++) {
      region.put(createPut(1, i));
      if (i <= 100) {
        region.put(createPut(2, i));
        if (i <= 50) {
          region.put(createPut(3, i));
        }
      }
    }
    // Now add more puts for CF2, so that we only flush CF2 to disk
    for (int i = 100; i < 2000; i++) {
      region.put(createPut(2, i));
    }

    MemStoreSize cf1MemstoreSizePhaseIII = region.getStore(FAMILY1).getMemStoreSize();

    // Flush in memory!
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore).flushInMemory();
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore).flushInMemory();
    // CF1 and CF3 should be merged so wait here to be sure the merge is done
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    region.flush(false);

    MemStoreSize cf1MemstoreSizePhaseIV = region.getStore(FAMILY1).getMemStoreSize();
    MemStoreSize cf2MemstoreSizePhaseIV = region.getStore(FAMILY2).getMemStoreSize();

    assertEquals(2*cf1MemstoreSizePhaseI.getDataSize(), cf1MemstoreSizePhaseIV.getDataSize());
    // the decrease in the heap size due to usage of CellArrayMap instead of CSLM
    // should be the same in flattening and in merge (first and second in-memory-flush)
    // but in phase 1 we do not yet have immutable segment
    assertEquals(
        cf1MemstoreSizePhaseI.getHeapSize() - cf1MemstoreSizePhaseII.getHeapSize(),
        cf1MemstoreSizePhaseIII.getHeapSize() - cf1MemstoreSizePhaseIV.getHeapSize()
            - CellArrayImmutableSegment.DEEP_OVERHEAD_CAM);
    assertEquals(3, // active, one in pipeline, snapshot
        ((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore).getSegments().size());
    // CF2 should have been cleared
    assertEquals("\n<<< DEBUG: The data--heap sizes of stores before/after first flushes,"
            + " CF1: " + cf1MemstoreSizePhaseI.getDataSize() + "/" + cf1MemstoreSizePhaseII
            .getDataSize() + "--" + cf1MemstoreSizePhaseI.getHeapSize() + "/" + cf1MemstoreSizePhaseII
            .getHeapSize() + ", CF2: " + cf2MemstoreSizePhaseI.getDataSize() + "/"
            + cf2MemstoreSizePhaseII.getDataSize() + "--" + cf2MemstoreSizePhaseI.getHeapSize() + "/"
            + cf2MemstoreSizePhaseII.getHeapSize() + ", CF3: " + cf3MemstoreSizePhaseI.getDataSize()
            + "/" + cf3MemstoreSizePhaseII.getDataSize() + "--" + cf3MemstoreSizePhaseI.getHeapSize()
            + "/" + cf3MemstoreSizePhaseII.getHeapSize() + "\n<<< AND before/after second flushes "
            + " CF1: " + cf1MemstoreSizePhaseIII.getDataSize() + "/" + cf1MemstoreSizePhaseIV
            .getDataSize() + "--" + cf1MemstoreSizePhaseIII.getHeapSize() + "/" + cf1MemstoreSizePhaseIV
            .getHeapSize() + "\n",
        0, cf2MemstoreSizePhaseIV.getDataSize());

    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  // should end in 300 seconds (5 minutes)
  @Test
  public void testStressFlushAndWALinIndexCompaction() throws IOException {
    // Set up the configuration
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 600 * 1024);
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
        200 * 1024);
    // set memstore to do data compaction and not to use the speculative scan
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
        String.valueOf(MemoryCompactionPolicy.BASIC));

    // Successfully initialize the HRegion
    HRegion region = initHRegion("testSelectiveFlushAndWALinDataCompaction", conf);
    verifyInMemoryFlushSize(region);
    Thread[] threads = new Thread[25];
    for (int i = 0; i < threads.length; i++) {
      int id = i * 10000;
      ConcurrentPutRunnable runnable = new ConcurrentPutRunnable(region, id);
      threads[i] = new Thread(runnable);
      threads[i].start();
    }
    Threads.sleep(10000); // let other threads start
    region.flush(true); // enforce flush of everything TO DISK while there are still ongoing puts
    Threads.sleep(10000); // let other threads continue
    region.flush(true); // enforce flush of everything TO DISK while there are still ongoing puts

    ((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore).flushInMemory();
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore).flushInMemory();
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }

    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * The in-memory-flusher thread performs the flush asynchronously. There is at most one thread per
   * memstore instance. It takes the updatesLock exclusively, pushes active into the pipeline,
   * releases updatesLock and compacts the pipeline.
   */
  private class ConcurrentPutRunnable implements Runnable {
    private final HRegion stressedRegion;
    private final int startNumber;

    ConcurrentPutRunnable(HRegion r, int i) {
      this.stressedRegion = r;
      this.startNumber = i;
    }

    @Override
    public void run() {

      try {
        int dummy = startNumber / 10000;
        System.out.print("Thread " + dummy + " with start number " + startNumber + " starts\n");
        // Add 1200 entries for CF1, 100 for CF2 and 50 for CF3
        for (int i = startNumber; i <= startNumber + 3000; i++) {
          stressedRegion.put(createPut(1, i));
          if (i <= startNumber + 2000) {
            stressedRegion.put(createPut(2, i));
            if (i <= startNumber + 1000) {
              stressedRegion.put(createPut(3, i));
            }
          }
        }
        System.out.print("Thread with start number " + startNumber + " continues to more puts\n");
        // Now add more puts for CF2, so that we only flush CF2 to disk
        for (int i = startNumber + 3000; i < startNumber + 5000; i++) {
          stressedRegion.put(createPut(2, i));
        }
        // And add more puts for CF1
        for (int i = startNumber + 5000; i < startNumber + 7000; i++) {
          stressedRegion.put(createPut(1, i));
        }
        System.out.print("Thread with start number " + startNumber + " flushes\n");
        // flush (IN MEMORY) one of the stores (each thread flushes different store)
        // and wait till the flush and the following action are done
        if (startNumber == 0) {
          ((CompactingMemStore) ((HStore) stressedRegion.getStore(FAMILY1)).memstore)
              .flushInMemory();
          while (((CompactingMemStore) ((HStore) stressedRegion.getStore(FAMILY1)).memstore)
              .isMemStoreFlushingInMemory()) {
            Threads.sleep(10);
          }
        }
        if (startNumber == 10000) {
          ((CompactingMemStore) ((HStore) stressedRegion.getStore(FAMILY2)).memstore).flushInMemory();
          while (((CompactingMemStore) ((HStore) stressedRegion.getStore(FAMILY2)).memstore)
              .isMemStoreFlushingInMemory()) {
            Threads.sleep(10);
          }
        }
        if (startNumber == 20000) {
          ((CompactingMemStore) ((HStore) stressedRegion.getStore(FAMILY3)).memstore).flushInMemory();
          while (((CompactingMemStore) ((HStore) stressedRegion.getStore(FAMILY3)).memstore)
              .isMemStoreFlushingInMemory()) {
            Threads.sleep(10);
          }
        }
        System.out.print("Thread with start number " + startNumber + " finishes\n");
      } catch (IOException e) {
        assert false;
      }
    }
  }

  private WAL getWAL(Region region) {
    return ((HRegion)region).getWAL();
  }
}
