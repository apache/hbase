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

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies the correctness of the Per Column Family flushing strategy
 * when part of the memstores are compacted memstores
 */
@Category({ RegionServerTests.class, LargeTests.class })
public class TestWalAndCompactingMemStoreFlush {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Path DIR = TEST_UTIL.getDataTestDir("TestHRegion");
  public static final TableName TABLENAME = TableName.valueOf("TestWalAndCompactingMemStoreFlush",
      "t1");

  public static final byte[][] FAMILIES = { Bytes.toBytes("f1"), Bytes.toBytes("f2"),
      Bytes.toBytes("f3"), Bytes.toBytes("f4"), Bytes.toBytes("f5") };

  public static final byte[] FAMILY1 = FAMILIES[0];
  public static final byte[] FAMILY2 = FAMILIES[1];
  public static final byte[] FAMILY3 = FAMILIES[2];

  private HRegion initHRegion(String callingMethod, Configuration conf) throws IOException {
    int i=0;
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    for (byte[] family : FAMILIES) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      // even column families are going to have compacted memstore
      if(i%2 == 0) hcd.setInMemoryCompaction(true);
      htd.addFamily(hcd);
      i++;
    }

    HRegionInfo info = new HRegionInfo(TABLENAME, null, null, false);
    Path path = new Path(DIR, callingMethod);
    return HBaseTestingUtility.createRegionAndWAL(info, path, conf, htd);
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

  // A helper function to create gets.
  private Get createGet(int familyNum, int putNum) {
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    return new Get(row);
  }

  // A helper function to verify edits.
  void verifyEdit(int familyNum, int putNum, Table table) throws IOException {
    Result r = table.get(createGet(familyNum, putNum));
    byte[] family = FAMILIES[familyNum - 1];
    byte[] qf = Bytes.toBytes("q" + familyNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    assertNotNull(("Missing Put#" + putNum + " for CF# " + familyNum), r.getFamilyMap(family));
    assertNotNull(("Missing Put#" + putNum + " for CF# " + familyNum),
      r.getFamilyMap(family).get(qf));
    assertTrue(("Incorrect value for Put#" + putNum + " for CF# " + familyNum),
      Arrays.equals(r.getFamilyMap(family).get(qf), val));
  }

  @Test(timeout = 180000)
  public void testSelectiveFlushWithDataCompaction() throws IOException {

    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 600 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
        FlushNonSloppyStoresFirstPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 200 * 1024);
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.25);
    // set memstore to do data compaction
    conf.set("hbase.hregion.compacting.memstore.type", "data-compaction");

    // Intialize the region
    Region region = initHRegion("testSelectiveFlushWithDataCompaction", conf);

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

    long totalMemstoreSize = region.getMemstoreSize();

    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF1PhaseI = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseI = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseI = region.getOldestSeqIdOfStore(FAMILY3);

    // Find the sizes of the memstores of each CF.
    long cf1MemstoreSizePhaseI = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseI = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseI = region.getStore(FAMILY3).getMemStoreSize();

    // Get the overall smallest LSN in the region's memstores.
    long smallestSeqInRegionCurrentMemstorePhaseI = getWAL(region)
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    String s = "\n\n----------------------------------\n"
        + "Upon initial insert and before any flush, size of CF1 is:"
        + cf1MemstoreSizePhaseI + ", is CF1 compacted memstore?:"
        + region.getStore(FAMILY1).isSloppyMemstore() + ". Size of CF2 is:"
        + cf2MemstoreSizePhaseI + ", is CF2 compacted memstore?:"
        + region.getStore(FAMILY2).isSloppyMemstore() + ". Size of CF3 is:"
        + cf3MemstoreSizePhaseI + ", is CF3 compacted memstore?:"
        + region.getStore(FAMILY3).isSloppyMemstore() + "\n";

    // The overall smallest LSN in the region's memstores should be the same as
    // the LSN of the smallest edit in CF1
    assertEquals(smallestSeqCF1PhaseI, smallestSeqInRegionCurrentMemstorePhaseI);

    // Some other sanity checks.
    assertTrue(smallestSeqCF1PhaseI < smallestSeqCF2PhaseI);
    assertTrue(smallestSeqCF2PhaseI < smallestSeqCF3PhaseI);
    assertTrue(cf1MemstoreSizePhaseI > 0);
    assertTrue(cf2MemstoreSizePhaseI > 0);
    assertTrue(cf3MemstoreSizePhaseI > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    String msg = "totalMemstoreSize="+totalMemstoreSize +
        " DefaultMemStore.DEEP_OVERHEAD="+DefaultMemStore.DEEP_OVERHEAD +
        " CompactingMemStore.DEEP_OVERHEAD="+CompactingMemStore.DEEP_OVERHEAD +
        " cf1MemstoreSizePhaseI="+cf1MemstoreSizePhaseI +
        " cf2MemstoreSizePhaseI="+cf2MemstoreSizePhaseI +
        " cf3MemstoreSizePhaseI="+cf3MemstoreSizePhaseI ;
    assertEquals(msg,
        totalMemstoreSize + 2 * (CompactingMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD)
            + (DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD),
        cf1MemstoreSizePhaseI + cf2MemstoreSizePhaseI + cf3MemstoreSizePhaseI);

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
    long cf1MemstoreSizePhaseII = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseII = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseII = region.getStore(FAMILY3).getMemStoreSize();

    long smallestSeqInRegionCurrentMemstorePhaseII = getWAL(region)
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF1PhaseII = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseII = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseII = region.getOldestSeqIdOfStore(FAMILY3);

    s = s + "DefaultMemStore DEEP_OVERHEAD is:" + DefaultMemStore.DEEP_OVERHEAD
        + ", CompactingMemStore DEEP_OVERHEAD is:" + CompactingMemStore.DEEP_OVERHEAD
        + "\n----After first flush! CF1 should be flushed to memory, but not compacted.---\n"
        + "Size of CF1 is:" + cf1MemstoreSizePhaseII + ", size of CF2 is:" + cf2MemstoreSizePhaseII
        + ", size of CF3 is:" + cf3MemstoreSizePhaseII + "\n";

    // CF1 was flushed to memory, but there is nothing to compact, and CF! was flattened
    assertTrue(cf1MemstoreSizePhaseII < cf1MemstoreSizePhaseI);

    // CF2 should become empty
    assertEquals(DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf2MemstoreSizePhaseII);

    // verify that CF3 was flushed to memory and was compacted (this is approximation check)
    assertTrue(cf3MemstoreSizePhaseI / 2 + CompactingMemStore.DEEP_OVERHEAD
        + ImmutableSegment.DEEP_OVERHEAD_CAM
        + CompactionPipeline.ENTRY_OVERHEAD > cf3MemstoreSizePhaseII);

    // CF3 was compacted and flattened!
    assertTrue("\n<<< Size of CF3 in phase I - " + cf3MemstoreSizePhaseI
            + ", size of CF3 in phase II - " + cf3MemstoreSizePhaseII + "\n",
        cf3MemstoreSizePhaseI / 2 > cf3MemstoreSizePhaseII);


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
    long cf1MemstoreSizePhaseIII = region.getStore(FAMILY1).getMemStoreSize();
    long smallestSeqCF1PhaseIII = region.getOldestSeqIdOfStore(FAMILY1);

    s = s + "----After more puts into CF1 its size is:" + cf1MemstoreSizePhaseIII
        + ", and its sequence is:" + smallestSeqCF1PhaseIII + " ----\n" ;


    // Flush!!!!!!!!!!!!!!!!!!!!!!
    // Flush again, CF1 is flushed to disk
    // CF2 is flushed to disk, because it is not in-memory compacted memstore
    // CF3 is flushed empty to memory (actually nothing happens to CF3)
    region.flush(false);

    // Recalculate everything
    long cf1MemstoreSizePhaseIV = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseIV = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseIV = region.getStore(FAMILY3).getMemStoreSize();

    long smallestSeqInRegionCurrentMemstorePhaseIV = getWAL(region)
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
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
    assertTrue(cf1MemstoreSizePhaseIII > cf1MemstoreSizePhaseIV);
    assertEquals(DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf2MemstoreSizePhaseIV);

    // CF3 shouldn't have been touched.
    assertEquals(cf3MemstoreSizePhaseIV, cf3MemstoreSizePhaseII);

    // the smallest LSN of CF3 shouldn't change
    assertEquals(smallestSeqCF3PhaseII, smallestSeqCF3PhaseIV);

    // CF3 should be bottleneck for WAL
    assertEquals(s, smallestSeqInRegionCurrentMemstorePhaseIV, smallestSeqCF3PhaseIV);

    // Flush!!!!!!!!!!!!!!!!!!!!!!
    // Trying to clean the existing memstores, CF2 all flushed to disk. The single
    // memstore segment in the compaction pipeline of CF1 and CF3 should be flushed to disk.
    // Note that active set of CF3 is empty
    // But active set of CF1 is not yet empty
    region.flush(true);

    // Recalculate everything
    long cf1MemstoreSizePhaseV = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseV = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseV = region.getStore(FAMILY3).getMemStoreSize();
    long smallestSeqInRegionCurrentMemstorePhaseV = getWAL(region)
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    assertTrue(
        CompactingMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD <= cf1MemstoreSizePhaseV);
    assertEquals(DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf2MemstoreSizePhaseV);
    assertEquals(CompactingMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf3MemstoreSizePhaseV);

    region.flush(true); // flush once again in order to be sure that everything is empty
    assertEquals(CompactingMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        region.getStore(FAMILY1).getMemStoreSize());

    // What happens when we hit the memstore limit, but we are not able to find
    // any Column Family above the threshold?
    // In that case, we should flush all the CFs.

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

    region.flush(false);

    s = s + "----AFTER THIRD AND FORTH FLUSH, The smallest sequence in region WAL is: "
        + smallestSeqInRegionCurrentMemstorePhaseV
        + ". After additional inserts and last flush, the entire region size is:" + region
        .getMemstoreSize()
        + "\n----------------------------------\n";

    // Since we won't find any CF above the threshold, and hence no specific
    // store to flush, we should flush all the memstores
    // Also compacted memstores are flushed to disk.
    assertEquals(0, region.getMemstoreSize());
    System.out.println(s);
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  /*------------------------------------------------------------------------------*/
  /* Check the same as above but for index-compaction type of compacting memstore */
  @Test(timeout = 180000)
  public void testSelectiveFlushWithIndexCompaction() throws IOException {

    /*------------------------------------------------------------------------------*/
    /* SETUP */
    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 600 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
        FlushNonSloppyStoresFirstPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 200 * 1024);
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.5);
    // set memstore to index-compaction
    conf.set("hbase.hregion.compacting.memstore.type", "index-compaction");

    // Initialize the region
    Region region = initHRegion("testSelectiveFlushWithIndexCompaction", conf);

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
    long totalMemstoreSizePhaseI = region.getMemstoreSize();
    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF1PhaseI = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseI = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseI = region.getOldestSeqIdOfStore(FAMILY3);
    // Find the sizes of the memstores of each CF.
    long cf1MemstoreSizePhaseI = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseI = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseI = region.getStore(FAMILY3).getMemStoreSize();
    // Get the overall smallest LSN in the region's memstores.
    long smallestSeqInRegionCurrentMemstorePhaseI = getWAL(region)
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());

    /*------------------------------------------------------------------------------*/
    /* PHASE I - validation */
    // The overall smallest LSN in the region's memstores should be the same as
    // the LSN of the smallest edit in CF1
    assertEquals(smallestSeqCF1PhaseI, smallestSeqInRegionCurrentMemstorePhaseI);
    // Some other sanity checks.
    assertTrue(smallestSeqCF1PhaseI < smallestSeqCF2PhaseI);
    assertTrue(smallestSeqCF2PhaseI < smallestSeqCF3PhaseI);
    assertTrue(cf1MemstoreSizePhaseI > 0);
    assertTrue(cf2MemstoreSizePhaseI > 0);
    assertTrue(cf3MemstoreSizePhaseI > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    assertEquals(
        totalMemstoreSizePhaseI
            + 1 * DefaultMemStore.DEEP_OVERHEAD
            + 2 * CompactingMemStore.DEEP_OVERHEAD
            + 3 * MutableSegment.DEEP_OVERHEAD,
        cf1MemstoreSizePhaseI + cf2MemstoreSizePhaseI + cf3MemstoreSizePhaseI);

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
    long cf1MemstoreSizePhaseII = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseII = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseII = region.getStore(FAMILY3).getMemStoreSize();
    long smallestSeqInRegionCurrentMemstorePhaseII = getWAL(region)
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    // Find the smallest LSNs for edits wrt to each CF.
    long smallestSeqCF3PhaseII = region.getOldestSeqIdOfStore(FAMILY3);
    long totalMemstoreSizePhaseII = region.getMemstoreSize();

    /*------------------------------------------------------------------------------*/
    /* PHASE II - validation */
    // CF1 was flushed to memory, should be flattened and take less space
    assertTrue(cf1MemstoreSizePhaseII < cf1MemstoreSizePhaseI);
    // CF2 should become empty
    assertEquals(DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf2MemstoreSizePhaseII);
    // verify that CF3 was flushed to memory and was not compacted (this is an approximation check)
    // if compacted CF# should be at least twice less because its every key was duplicated
    assertTrue(cf3MemstoreSizePhaseI / 2 < cf3MemstoreSizePhaseII);

    // Now the smallest LSN in the region should be the same as the smallest
    // LSN in the memstore of CF1.
    assertEquals(smallestSeqInRegionCurrentMemstorePhaseII, smallestSeqCF1PhaseI);
    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3. Counting the empty active segments in CF1/2/3 and pipeline
    // items in CF1/2
    assertEquals(
        totalMemstoreSizePhaseII
            + 1 * DefaultMemStore.DEEP_OVERHEAD
            + 2 * CompactingMemStore.DEEP_OVERHEAD
            + 3 * MutableSegment.DEEP_OVERHEAD
            + 2 * CompactionPipeline.ENTRY_OVERHEAD
            + 2 * ImmutableSegment.DEEP_OVERHEAD_CAM,
        cf1MemstoreSizePhaseII + cf2MemstoreSizePhaseII + cf3MemstoreSizePhaseII);

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
    long cf1MemstoreSizePhaseIII = region.getStore(FAMILY1).getMemStoreSize();
    long totalMemstoreSizePhaseIII = region.getMemstoreSize();

    /*------------------------------------------------------------------------------*/
    /* PHASE III - validation */
    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3. Counting the empty active segments in CF1/2/3 and pipeline
    // items in CF1/2
    assertEquals(
        totalMemstoreSizePhaseIII
            + 1 * DefaultMemStore.DEEP_OVERHEAD
            + 2 * CompactingMemStore.DEEP_OVERHEAD
            + 3 * MutableSegment.DEEP_OVERHEAD
            + 2 * CompactionPipeline.ENTRY_OVERHEAD
            + 2 * ImmutableSegment.DEEP_OVERHEAD_CAM,
        cf1MemstoreSizePhaseIII + cf2MemstoreSizePhaseII + cf3MemstoreSizePhaseII);

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
    long cf1MemstoreSizePhaseIV = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseIV = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseIV = region.getStore(FAMILY3).getMemStoreSize();
    long smallestSeqInRegionCurrentMemstorePhaseIV = getWAL(region)
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF3PhaseIV = region.getOldestSeqIdOfStore(FAMILY3);

    /*------------------------------------------------------------------------------*/
    /* PHASE IV - validation */
    // CF1's biggest pipeline component (inserted before first flush) should be flushed to disk
    // CF2 should remain empty
    assertTrue(cf1MemstoreSizePhaseIII > cf1MemstoreSizePhaseIV);
    assertEquals(DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf2MemstoreSizePhaseIV);
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
    long cf1MemstoreSizePhaseV = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseV = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseV = region.getStore(FAMILY3).getMemStoreSize();
    long smallestSeqInRegionCurrentMemstorePhaseV = getWAL(region)
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long totalMemstoreSizePhaseV = region.getMemstoreSize();

    /*------------------------------------------------------------------------------*/
    /* PHASE V - validation */
    assertEquals(CompactingMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf1MemstoreSizePhaseV);
    assertEquals(DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf2MemstoreSizePhaseV);
    assertEquals(CompactingMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf3MemstoreSizePhaseV);
    // The total memstores size should be empty
    assertEquals(totalMemstoreSizePhaseV, 0);
    // Because there is nothing in any memstore the WAL's LSN should be -1
    assertEquals(smallestSeqInRegionCurrentMemstorePhaseV, HConstants.NO_SEQNUM);

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

    long cf1ActiveSizePhaseVI = region.getStore(FAMILY1).getMemStoreSize();
    long cf3ActiveSizePhaseVI = region.getStore(FAMILY3).getMemStoreSize();
    long cf5ActiveSizePhaseVI = region.getStore(FAMILIES[4]).getMemStoreSize();

    /*------------------------------------------------------------------------------*/
    /* PHASE VI - Flush */
    // Fourth Flush in Test!!!!!!!!!!!!!!!!!!!!!!
    // None among compacting memstores was flushed to memory due to previous puts.
    // But is going to be moved to pipeline and flatten due to the flush.
    region.flush(false);
    // Since we won't find any CF above the threshold, and hence no specific
    // store to flush, we should flush all the memstores
    // Also compacted memstores are flushed to disk, but not entirely emptied
    long cf1ActiveSizePhaseVII = region.getStore(FAMILY1).getMemStoreSize();
    long cf3ActiveSizePhaseVII = region.getStore(FAMILY3).getMemStoreSize();
    long cf5ActiveSizePhaseVII = region.getStore(FAMILIES[4]).getMemStoreSize();

    assertTrue(cf1ActiveSizePhaseVII < cf1ActiveSizePhaseVI);
    assertTrue(cf3ActiveSizePhaseVII < cf3ActiveSizePhaseVI);
    assertTrue(cf5ActiveSizePhaseVII < cf5ActiveSizePhaseVI);

    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  @Test(timeout = 180000)
  public void testSelectiveFlushAndWALinDataCompaction() throws IOException {
    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 600 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushNonSloppyStoresFirstPolicy.class
        .getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN, 200 *
        1024);
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.5);
    // set memstore to do data compaction and not to use the speculative scan
    conf.set("hbase.hregion.compacting.memstore.type", "data-compaction");

    // Intialize the HRegion
    HRegion region = initHRegion("testSelectiveFlushAndWALinDataCompaction", conf);
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

    long totalMemstoreSize = region.getMemstoreSize();

    // Find the sizes of the memstores of each CF.
    long cf1MemstoreSizePhaseI = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseI = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseI = region.getStore(FAMILY3).getMemStoreSize();

    // Some other sanity checks.
    assertTrue(cf1MemstoreSizePhaseI > 0);
    assertTrue(cf2MemstoreSizePhaseI > 0);
    assertTrue(cf3MemstoreSizePhaseI > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    String msg = "totalMemstoreSize="+totalMemstoreSize +
        " DefaultMemStore.DEEP_OVERHEAD="+DefaultMemStore.DEEP_OVERHEAD +
        " cf1MemstoreSizePhaseI="+cf1MemstoreSizePhaseI +
        " cf2MemstoreSizePhaseI="+cf2MemstoreSizePhaseI +
        " cf3MemstoreSizePhaseI="+cf3MemstoreSizePhaseI ;
    assertEquals(msg,
        totalMemstoreSize + 2 * (CompactingMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD)
            + (DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD),
        cf1MemstoreSizePhaseI + cf2MemstoreSizePhaseI + cf3MemstoreSizePhaseI);

    // Flush!
    CompactingMemStore cms1 = (CompactingMemStore) ((HStore) region.getStore(FAMILY1)).memstore;
    CompactingMemStore cms3 = (CompactingMemStore) ((HStore) region.getStore(FAMILY3)).memstore;
    cms1.flushInMemory();
    cms3.flushInMemory();
    region.flush(false);

    long cf2MemstoreSizePhaseII = region.getStore(FAMILY2).getMemStoreSize();

    long smallestSeqInRegionCurrentMemstorePhaseII =
        region.getWAL().getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF1PhaseII = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseII = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseII = region.getOldestSeqIdOfStore(FAMILY3);

    // CF2 should have been cleared
    assertEquals(DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf2MemstoreSizePhaseII);

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
        region.getWAL().getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
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
        region.getWAL().getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
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

  @Test(timeout = 180000)
  public void testSelectiveFlushAndWALinIndexCompaction() throws IOException {
    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 600 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
        FlushNonSloppyStoresFirstPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
        200 * 1024);
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.5);
    // set memstore to do data compaction and not to use the speculative scan
    conf.set("hbase.hregion.compacting.memstore.type", "index-compaction");

    // Intialize the HRegion
    HRegion region = initHRegion("testSelectiveFlushAndWALinDataCompaction", conf);
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

    long totalMemstoreSize = region.getMemstoreSize();

    // Find the sizes of the memstores of each CF.
    long cf1MemstoreSizePhaseI = region.getStore(FAMILY1).getMemStoreSize();
    long cf2MemstoreSizePhaseI = region.getStore(FAMILY2).getMemStoreSize();
    long cf3MemstoreSizePhaseI = region.getStore(FAMILY3).getMemStoreSize();

    // Some other sanity checks.
    assertTrue(cf1MemstoreSizePhaseI > 0);
    assertTrue(cf2MemstoreSizePhaseI > 0);
    assertTrue(cf3MemstoreSizePhaseI > 0);

    // The total memstore size should be the same as the sum of the sizes of
    // memstores of CF1, CF2 and CF3.
    assertEquals(
        totalMemstoreSize
            + 1 * DefaultMemStore.DEEP_OVERHEAD
            + 2 * CompactingMemStore.DEEP_OVERHEAD
            + 3 * MutableSegment.DEEP_OVERHEAD,
        cf1MemstoreSizePhaseI + cf2MemstoreSizePhaseI + cf3MemstoreSizePhaseI);

    // Flush!
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore).flushInMemory();
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore).flushInMemory();
    // CF1 and CF3 should be compacted so wait here to be sure the compaction is done
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    region.flush(false);

    long cf2MemstoreSizePhaseII = region.getStore(FAMILY2).getMemStoreSize();

    long smallestSeqInRegionCurrentMemstorePhaseII = region.getWAL()
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF1PhaseII = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseII = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseII = region.getOldestSeqIdOfStore(FAMILY3);

    // CF2 should have been cleared
    assertEquals(DefaultMemStore.DEEP_OVERHEAD + MutableSegment.DEEP_OVERHEAD,
        cf2MemstoreSizePhaseII);

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

    long smallestSeqInRegionCurrentMemstorePhaseIII = region.getWAL()
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF1PhaseIII = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseIII = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseIII = region.getOldestSeqIdOfStore(FAMILY3);

    // Flush!
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore).flushInMemory();
    ((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore).flushInMemory();
    // CF1 and CF3 should be compacted so wait here to be sure the compaction is done
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY1)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    while (((CompactingMemStore) ((HStore)region.getStore(FAMILY3)).memstore)
        .isMemStoreFlushingInMemory()) {
      Threads.sleep(10);
    }
    region.flush(false);

    long smallestSeqInRegionCurrentMemstorePhaseIV = region.getWAL()
        .getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
    long smallestSeqCF1PhaseIV = region.getOldestSeqIdOfStore(FAMILY1);
    long smallestSeqCF2PhaseIV = region.getOldestSeqIdOfStore(FAMILY2);
    long smallestSeqCF3PhaseIV = region.getOldestSeqIdOfStore(FAMILY3);

    // now check that the LSN of the entire WAL, of CF1 and of CF3 has NOT progressed due to merge
    assertFalse(
        smallestSeqInRegionCurrentMemstorePhaseIV > smallestSeqInRegionCurrentMemstorePhaseIII);
    assertFalse(smallestSeqCF1PhaseIV > smallestSeqCF1PhaseIII);
    assertFalse(smallestSeqCF3PhaseIV > smallestSeqCF3PhaseIII);

    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  // should end in 300 seconds (5 minutes)
  @Test(timeout = 300000)
  public void testStressFlushAndWALinIndexCompaction() throws IOException {
    // Set up the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 600 * 1024);
    conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY,
        FlushNonSloppyStoresFirstPolicy.class.getName());
    conf.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
        200 * 1024);
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.5);
    // set memstore to do data compaction and not to use the speculative scan
    conf.set("hbase.hregion.compacting.memstore.type", "index-compaction");

    // Successfully initialize the HRegion
    HRegion region = initHRegion("testSelectiveFlushAndWALinDataCompaction", conf);

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
