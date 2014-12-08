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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY_BYTES;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestCase.HRegionIncommon;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;


/**
 * Test major compactions
 */
@Category(MediumTests.class)
public class TestMajorCompaction {
  @Rule public TestName name = new TestName();
  static final Log LOG = LogFactory.getLog(TestMajorCompaction.class.getName());
  private static final HBaseTestingUtility UTIL = HBaseTestingUtility.createLocalHTU();
  protected Configuration conf = UTIL.getConfiguration();
  
  private HRegion r = null;
  private HTableDescriptor htd = null;
  private static final byte [] COLUMN_FAMILY = fam1;
  private final byte [] STARTROW = Bytes.toBytes(START_KEY);
  private static final byte [] COLUMN_FAMILY_TEXT = COLUMN_FAMILY;
  private int compactionThreshold;
  private byte[] secondRowBytes, thirdRowBytes;
  private static final long MAX_FILES_TO_COMPACT = 10;

  /** constructor */
  public TestMajorCompaction() {
    super();

    // Set cache flush size to 1MB
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024*1024);
    conf.setInt("hbase.hregion.memstore.block.multiplier", 100);
    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);

    secondRowBytes = START_KEY_BYTES.clone();
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    thirdRowBytes = START_KEY_BYTES.clone();
    thirdRowBytes[START_KEY_BYTES.length - 1] += 2;
  }

  @Before
  public void setUp() throws Exception {
    this.htd = UTIL.createTableDescriptor(name.getMethodName());
    this.r = UTIL.createLocalHRegion(htd, null, null);
  }

  @After
  public void tearDown() throws Exception {
    HLog hlog = r.getLog();
    this.r.close();
    hlog.closeAndDelete();
  }

  /**
   * Test that on a major compaction, if all cells are expired or deleted, then
   * we'll end up with no product.  Make sure scanner over region returns
   * right answer in this case - and that it just basically works.
   * @throws IOException
   */
  @Test
  public void testMajorCompactingToNoOutput() throws IOException {
    createStoreFile(r);
    for (int i = 0; i < compactionThreshold; i++) {
      createStoreFile(r);
    }
    // Now delete everything.
    InternalScanner s = r.getScanner(new Scan());
    do {
      List<Cell> results = new ArrayList<Cell>();
      boolean result = s.next(results);
      r.delete(new Delete(CellUtil.cloneRow(results.get(0))));
      if (!result) break;
    } while(true);
    s.close();
    // Flush
    r.flushcache();
    // Major compact.
    r.compactStores(true);
    s = r.getScanner(new Scan());
    int counter = 0;
    do {
      List<Cell> results = new ArrayList<Cell>();
      boolean result = s.next(results);
      if (!result) break;
      counter++;
    } while(true);
    assertEquals(0, counter);
  }

  /**
   * Run compaction and flushing memstore
   * Assert deletes get cleaned up.
   * @throws Exception
   */
  @Test
  public void testMajorCompaction() throws Exception {
    majorCompaction();
  }

  @Test
  public void testDataBlockEncodingInCacheOnly() throws Exception {
    majorCompactionWithDataBlockEncoding(true);
  }

  @Test
  public void testDataBlockEncodingEverywhere() throws Exception {
    majorCompactionWithDataBlockEncoding(false);
  }

  public void majorCompactionWithDataBlockEncoding(boolean inCacheOnly)
      throws Exception {
    Map<HStore, HFileDataBlockEncoder> replaceBlockCache =
        new HashMap<HStore, HFileDataBlockEncoder>();
    for (Entry<byte[], Store> pair : r.getStores().entrySet()) {
      HStore store = (HStore) pair.getValue();
      HFileDataBlockEncoder blockEncoder = store.getDataBlockEncoder();
      replaceBlockCache.put(store, blockEncoder);
      final DataBlockEncoding inCache = DataBlockEncoding.PREFIX;
      final DataBlockEncoding onDisk = inCacheOnly ? DataBlockEncoding.NONE :
          inCache;
      store.setDataBlockEncoderInTest(new HFileDataBlockEncoderImpl(onDisk));
    }

    majorCompaction();

    // restore settings
    for (Entry<HStore, HFileDataBlockEncoder> entry :
        replaceBlockCache.entrySet()) {
      entry.getKey().setDataBlockEncoderInTest(entry.getValue());
    }
  }

  private void majorCompaction() throws Exception {
    createStoreFile(r);
    for (int i = 0; i < compactionThreshold; i++) {
      createStoreFile(r);
    }
    // Add more content.
    HBaseTestCase.addContent(new HRegionIncommon(r), Bytes.toString(COLUMN_FAMILY));

    // Now there are about 5 versions of each column.
    // Default is that there only 3 (MAXVERSIONS) versions allowed per column.
    //
    // Assert == 3 when we ask for versions.
    Result result = r.get(new Get(STARTROW).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100));
    assertEquals(compactionThreshold, result.size());

    // see if CompactionProgress is in place but null
    for (Store store : this.r.stores.values()) {
      assertNull(store.getCompactionProgress());
    }

    r.flushcache();
    r.compactStores(true);

    // see if CompactionProgress has done its thing on at least one store
    int storeCount = 0;
    for (Store store : this.r.stores.values()) {
      CompactionProgress progress = store.getCompactionProgress();
      if( progress != null ) {
        ++storeCount;
        assertTrue(progress.currentCompactedKVs > 0);
        assertTrue(progress.totalCompactingKVs > 0);
      }
      assertTrue(storeCount > 0);
    }

    // look at the second row
    // Increment the least significant character so we get to next row.
    byte [] secondRowBytes = START_KEY_BYTES.clone();
    secondRowBytes[START_KEY_BYTES.length - 1]++;

    // Always 3 versions if that is what max versions is.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).
        setMaxVersions(100));
    LOG.debug("Row " + Bytes.toStringBinary(secondRowBytes) + " after " +
        "initial compaction: " + result);
    assertEquals("Invalid number of versions of row "
        + Bytes.toStringBinary(secondRowBytes) + ".", compactionThreshold,
        result.size());

    // Now add deletes to memstore and then flush it.
    // That will put us over
    // the compaction threshold of 3 store files.  Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    LOG.debug("Adding deletes to memstore and flushing");
    Delete delete = new Delete(secondRowBytes, System.currentTimeMillis());
    byte [][] famAndQf = {COLUMN_FAMILY, null};
    delete.deleteFamily(famAndQf[0]);
    r.delete(delete);

    // Assert deleted.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100));
    assertTrue("Second row should have been deleted", result.isEmpty());

    r.flushcache();

    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100));
    assertTrue("Second row should have been deleted", result.isEmpty());

    // Add a bit of data and flush.  Start adding at 'bbb'.
    createSmallerStoreFile(this.r);
    r.flushcache();
    // Assert that the second row is still deleted.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100));
    assertTrue("Second row should still be deleted", result.isEmpty());

    // Force major compaction.
    r.compactStores(true);
    assertEquals(r.getStore(COLUMN_FAMILY_TEXT).getStorefiles().size(), 1);

    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100));
    assertTrue("Second row should still be deleted", result.isEmpty());

    // Make sure the store files do have some 'aaa' keys in them -- exactly 3.
    // Also, that compacted store files do not have any secondRowBytes because
    // they were deleted.
    verifyCounts(3,0);

    // Multiple versions allowed for an entry, so the delete isn't enough
    // Lower TTL and expire to ensure that all our entries have been wiped
    final int ttl = 1000;
    for (Store hstore : this.r.stores.values()) {
      HStore store = ((HStore) hstore);
      ScanInfo old = store.getScanInfo();
      ScanInfo si = new ScanInfo(old.getFamily(),
          old.getMinVersions(), old.getMaxVersions(), ttl,
          old.getKeepDeletedCells(), 0, old.getComparator());
      store.setScanInfo(si);
    }
    Thread.sleep(1000);

    r.compactStores(true);
    int count = count();
    assertEquals("Should not see anything after TTL has expired", 0, count);
  }

  @Test
  public void testTimeBasedMajorCompaction() throws Exception {
    // create 2 storefiles and force a major compaction to reset the time
    int delay = 10 * 1000; // 10 sec
    float jitterPct = 0.20f; // 20%
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, delay);
    conf.setFloat("hbase.hregion.majorcompaction.jitter", jitterPct);

    HStore s = ((HStore) r.getStore(COLUMN_FAMILY));
    s.storeEngine.getCompactionPolicy().setConf(conf);
    try {
      createStoreFile(r);
      createStoreFile(r);
      r.compactStores(true);

      // add one more file & verify that a regular compaction won't work
      createStoreFile(r);
      r.compactStores(false);
      assertEquals(2, s.getStorefilesCount());

      // ensure that major compaction time is deterministic
      RatioBasedCompactionPolicy
          c = (RatioBasedCompactionPolicy)s.storeEngine.getCompactionPolicy();
      Collection<StoreFile> storeFiles = s.getStorefiles();
      long mcTime = c.getNextMajorCompactTime(storeFiles);
      for (int i = 0; i < 10; ++i) {
        assertEquals(mcTime, c.getNextMajorCompactTime(storeFiles));
      }

      // ensure that the major compaction time is within the variance
      long jitter = Math.round(delay * jitterPct);
      assertTrue(delay - jitter <= mcTime && mcTime <= delay + jitter);

      // wait until the time-based compaction interval
      Thread.sleep(mcTime);

      // trigger a compaction request and ensure that it's upgraded to major
      r.compactStores(false);
      assertEquals(1, s.getStorefilesCount());
    } finally {
      // reset the timed compaction settings
      conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 1000*60*60*24);
      conf.setFloat("hbase.hregion.majorcompaction.jitter", 0.20F);
      // run a major to reset the cache
      createStoreFile(r);
      r.compactStores(true);
      assertEquals(1, s.getStorefilesCount());
    }
  }

  private void verifyCounts(int countRow1, int countRow2) throws Exception {
    int count1 = 0;
    int count2 = 0;
    for (StoreFile f: this.r.stores.get(COLUMN_FAMILY_TEXT).getStorefiles()) {
      HFileScanner scanner = f.getReader().getScanner(false, false);
      scanner.seekTo();
      do {
        byte [] row = scanner.getKeyValue().getRow();
        if (Bytes.equals(row, STARTROW)) {
          count1++;
        } else if(Bytes.equals(row, secondRowBytes)) {
          count2++;
        }
      } while(scanner.next());
    }
    assertEquals(countRow1,count1);
    assertEquals(countRow2,count2);
  }


  private int count() throws IOException {
    int count = 0;
    for (StoreFile f: this.r.stores.
        get(COLUMN_FAMILY_TEXT).getStorefiles()) {
      HFileScanner scanner = f.getReader().getScanner(false, false);
      if (!scanner.seekTo()) {
        continue;
      }
      do {
        count++;
      } while(scanner.next());
    }
    return count;
  }

  private void createStoreFile(final HRegion region) throws IOException {
    createStoreFile(region, Bytes.toString(COLUMN_FAMILY));
  }

  private void createStoreFile(final HRegion region, String family) throws IOException {
    HRegionIncommon loader = new HRegionIncommon(region);
    HBaseTestCase.addContent(loader, family);
    loader.flushcache();
  }

  private void createSmallerStoreFile(final HRegion region) throws IOException {
    HRegionIncommon loader = new HRegionIncommon(region);
    HBaseTestCase.addContent(loader, Bytes.toString(COLUMN_FAMILY), ("" +
    		"bbb").getBytes(), null);
    loader.flushcache();
  }

  /**
   * Test for HBASE-5920 - Test user requested major compactions always occurring
   */
  @Test
  public void testNonUserMajorCompactionRequest() throws Exception {
    Store store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < MAX_FILES_TO_COMPACT + 1; i++) {
      createStoreFile(r);
    }
    store.triggerMajorCompaction();

    CompactionRequest request = store.requestCompaction(Store.NO_PRIORITY, null).getRequest();
    assertNotNull("Expected to receive a compaction request", request);
    assertEquals(
      "System-requested major compaction should not occur if there are too many store files",
      false,
      request.isMajor());
  }

  /**
   * Test for HBASE-5920
   */
  @Test
  public void testUserMajorCompactionRequest() throws IOException{
    Store store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < MAX_FILES_TO_COMPACT + 1; i++) {
      createStoreFile(r);
    }
    store.triggerMajorCompaction();
    CompactionRequest request = store.requestCompaction(Store.PRIORITY_USER, null).getRequest();
    assertNotNull("Expected to receive a compaction request", request);
    assertEquals(
      "User-requested major compaction should always occur, even if there are too many store files",
      true, 
      request.isMajor());
  }

  /**
   * Test that on a major compaction, if all cells are expired or deleted, then we'll end up with no
   * product. Make sure scanner over region returns right answer in this case - and that it just
   * basically works.
   * @throws IOException
   */
  public void testMajorCompactingToNoOutputWithReverseScan() throws IOException {
    createStoreFile(r);
    for (int i = 0; i < compactionThreshold; i++) {
      createStoreFile(r);
    }
    // Now delete everything.
    Scan scan = new Scan();
    scan.setReversed(true);
    InternalScanner s = r.getScanner(scan);
    do {
      List<Cell> results = new ArrayList<Cell>();
      boolean result = s.next(results);
      assertTrue(!results.isEmpty());
      r.delete(new Delete(results.get(0).getRow()));
      if (!result) break;
    } while (true);
    s.close();
    // Flush
    r.flushcache();
    // Major compact.
    r.compactStores(true);
    scan = new Scan();
    scan.setReversed(true);
    s = r.getScanner(scan);
    int counter = 0;
    do {
      List<Cell> results = new ArrayList<Cell>();
      boolean result = s.next(results);
      if (!result) break;
      counter++;
    } while (true);
    s.close();
    assertEquals(0, counter);
  }
}
