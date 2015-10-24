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

import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY_BYTES;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test of a long-lived scanner validating as we go.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestScanner {
  @Rule public TestName name = new TestName();
  private static final Log LOG = LogFactory.getLog(TestScanner.class);
  private final static HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();

  private static final byte [] FIRST_ROW = HConstants.EMPTY_START_ROW;
  private static final byte [][] COLS = { HConstants.CATALOG_FAMILY };
  private static final byte [][] EXPLICIT_COLS = {
    HConstants.REGIONINFO_QUALIFIER, HConstants.SERVER_QUALIFIER,
      // TODO ryan
      //HConstants.STARTCODE_QUALIFIER
  };

  static final HTableDescriptor TESTTABLEDESC =
    new HTableDescriptor(TableName.valueOf("testscanner"));
  static {
    TESTTABLEDESC.addFamily(
        new HColumnDescriptor(HConstants.CATALOG_FAMILY)
            // Ten is an arbitrary number.  Keep versions to help debugging.
            .setMaxVersions(10)
            .setBlockCacheEnabled(false)
            .setBlocksize(8 * 1024)
    );
  }
  /** HRegionInfo for root region */
  public static final HRegionInfo REGION_INFO =
    new HRegionInfo(TESTTABLEDESC.getTableName(), HConstants.EMPTY_BYTE_ARRAY,
    HConstants.EMPTY_BYTE_ARRAY);

  private static final byte [] ROW_KEY = REGION_INFO.getRegionName();

  private static final long START_CODE = Long.MAX_VALUE;

  private HRegion region;

  private byte[] firstRowBytes, secondRowBytes, thirdRowBytes;
  final private byte[] col1;

  public TestScanner() {
    super();

    firstRowBytes = START_KEY_BYTES;
    secondRowBytes = START_KEY_BYTES.clone();
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    thirdRowBytes = START_KEY_BYTES.clone();
    thirdRowBytes[START_KEY_BYTES.length - 1] += 2;
    col1 = Bytes.toBytes("column1");
  }

  /**
   * Test basic stop row filter works.
   * @throws Exception
   */
  @Test
  public void testStopRow() throws Exception {
    byte [] startrow = Bytes.toBytes("bbb");
    byte [] stoprow = Bytes.toBytes("ccc");
    try {
      this.region = TEST_UTIL.createLocalHRegion(TESTTABLEDESC, null, null);
      HBaseTestCase.addContent(this.region, HConstants.CATALOG_FAMILY);
      List<Cell> results = new ArrayList<Cell>();
      // Do simple test of getting one row only first.
      Scan scan = new Scan(Bytes.toBytes("abc"), Bytes.toBytes("abd"));
      scan.addFamily(HConstants.CATALOG_FAMILY);

      InternalScanner s = region.getScanner(scan);
      int count = 0;
      while (s.next(results)) {
        count++;
      }
      s.close();
      assertEquals(0, count);
      // Now do something a bit more imvolved.
      scan = new Scan(startrow, stoprow);
      scan.addFamily(HConstants.CATALOG_FAMILY);

      s = region.getScanner(scan);
      count = 0;
      Cell kv = null;
      results = new ArrayList<Cell>();
      for (boolean first = true; s.next(results);) {
        kv = results.get(0);
        if (first) {
          assertTrue(CellUtil.matchingRow(kv,  startrow));
          first = false;
        }
        count++;
      }
      assertTrue(Bytes.BYTES_COMPARATOR.compare(stoprow, CellUtil.cloneRow(kv)) > 0);
      // We got something back.
      assertTrue(count > 10);
      s.close();
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
    }
  }

  void rowPrefixFilter(Scan scan) throws IOException {
    List<Cell> results = new ArrayList<Cell>();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    InternalScanner s = region.getScanner(scan);
    boolean hasMore = true;
    while (hasMore) {
      hasMore = s.next(results);
      for (Cell kv : results) {
        assertEquals((byte)'a', CellUtil.cloneRow(kv)[0]);
        assertEquals((byte)'b', CellUtil.cloneRow(kv)[1]);
      }
      results.clear();
    }
    s.close();
  }

  void rowInclusiveStopFilter(Scan scan, byte[] stopRow) throws IOException {
    List<Cell> results = new ArrayList<Cell>();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    InternalScanner s = region.getScanner(scan);
    boolean hasMore = true;
    while (hasMore) {
      hasMore = s.next(results);
      for (Cell kv : results) {
        assertTrue(Bytes.compareTo(CellUtil.cloneRow(kv), stopRow) <= 0);
      }
      results.clear();
    }
    s.close();
  }

  @Test
  public void testFilters() throws IOException {
    try {
      this.region = TEST_UTIL.createLocalHRegion(TESTTABLEDESC, null, null);
      HBaseTestCase.addContent(this.region, HConstants.CATALOG_FAMILY);
      byte [] prefix = Bytes.toBytes("ab");
      Filter newFilter = new PrefixFilter(prefix);
      Scan scan = new Scan();
      scan.setFilter(newFilter);
      rowPrefixFilter(scan);

      byte[] stopRow = Bytes.toBytes("bbc");
      newFilter = new WhileMatchFilter(new InclusiveStopFilter(stopRow));
      scan = new Scan();
      scan.setFilter(newFilter);
      rowInclusiveStopFilter(scan, stopRow);

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
    }
  }

  /**
   * Test that closing a scanner while a client is using it doesn't throw
   * NPEs but instead a UnknownScannerException. HBASE-2503
   * @throws Exception
   */
  @Test
  public void testRaceBetweenClientAndTimeout() throws Exception {
    try {
      this.region = TEST_UTIL.createLocalHRegion(TESTTABLEDESC, null, null);
      HBaseTestCase.addContent(this.region, HConstants.CATALOG_FAMILY);
      Scan scan = new Scan();
      InternalScanner s = region.getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      try {
        s.next(results);
        s.close();
        s.next(results);
        fail("We don't want anything more, we should be failing");
      } catch (UnknownScannerException ex) {
        // ok!
        return;
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
    }
  }

  /** The test!
   * @throws IOException
   */
  @Test
  public void testScanner() throws IOException {
    try {
      region = TEST_UTIL.createLocalHRegion(TESTTABLEDESC, null, null);
      Table table = new RegionAsTable(region);

      // Write information to the meta table

      Put put = new Put(ROW_KEY, System.currentTimeMillis());

      put.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
          REGION_INFO.toByteArray());
      table.put(put);

      // What we just committed is in the memstore. Verify that we can get
      // it back both with scanning and get

      scan(false, null);
      getRegionInfo(table);

      // Close and re-open

      ((HRegion)region).close();
      region = HRegion.openHRegion(region, null);
      table = new RegionAsTable(region);

      // Verify we can get the data back now that it is on disk.

      scan(false, null);
      getRegionInfo(table);

      // Store some new information

      String address = HConstants.LOCALHOST_IP + ":" + HBaseTestingUtility.randomFreePort();

      put = new Put(ROW_KEY, System.currentTimeMillis());
      put.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
          Bytes.toBytes(address));

//      put.add(HConstants.COL_STARTCODE, Bytes.toBytes(START_CODE));

      table.put(put);

      // Validate that we can still get the HRegionInfo, even though it is in
      // an older row on disk and there is a newer row in the memstore

      scan(true, address.toString());
      getRegionInfo(table);

      // flush cache
      this.region.flush(true);

      // Validate again

      scan(true, address.toString());
      getRegionInfo(table);

      // Close and reopen

      ((HRegion)region).close();
      region = HRegion.openHRegion(region,null);
      table = new RegionAsTable(region);

      // Validate again

      scan(true, address.toString());
      getRegionInfo(table);

      // Now update the information again

      address = "bar.foo.com:4321";

      put = new Put(ROW_KEY, System.currentTimeMillis());

      put.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER, Bytes.toBytes(address));
      table.put(put);

      // Validate again

      scan(true, address.toString());
      getRegionInfo(table);

      // flush cache

      region.flush(true);

      // Validate again

      scan(true, address.toString());
      getRegionInfo(table);

      // Close and reopen

      ((HRegion)this.region).close();
      this.region = HRegion.openHRegion(region, null);
      table = new RegionAsTable(this.region);

      // Validate again

      scan(true, address.toString());
      getRegionInfo(table);

    } finally {
      // clean up
      HBaseTestingUtility.closeRegionAndWAL(this.region);
    }
  }

  /** Compare the HRegionInfo we read from HBase to what we stored */
  private void validateRegionInfo(byte [] regionBytes) throws IOException {
    HRegionInfo info = HRegionInfo.parseFromOrNull(regionBytes);

    assertEquals(REGION_INFO.getRegionId(), info.getRegionId());
    assertEquals(0, info.getStartKey().length);
    assertEquals(0, info.getEndKey().length);
    assertEquals(0, Bytes.compareTo(info.getRegionName(), REGION_INFO.getRegionName()));
    //assertEquals(0, info.getTableDesc().compareTo(REGION_INFO.getTableDesc()));
  }

  /** Use a scanner to get the region info and then validate the results */
  private void scan(boolean validateStartcode, String serverName)
  throws IOException {
    InternalScanner scanner = null;
    Scan scan = null;
    List<Cell> results = new ArrayList<Cell>();
    byte [][][] scanColumns = {
        COLS,
        EXPLICIT_COLS
    };

    for(int i = 0; i < scanColumns.length; i++) {
      try {
        scan = new Scan(FIRST_ROW);
        for (int ii = 0; ii < EXPLICIT_COLS.length; ii++) {
          scan.addColumn(COLS[0],  EXPLICIT_COLS[ii]);
        }
        scanner = region.getScanner(scan);
        while (scanner.next(results)) {
          assertTrue(hasColumn(results, HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER));
          byte [] val = CellUtil.cloneValue(getColumn(results, HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER));
          validateRegionInfo(val);
          if(validateStartcode) {
//            assertTrue(hasColumn(results, HConstants.CATALOG_FAMILY,
//                HConstants.STARTCODE_QUALIFIER));
//            val = getColumn(results, HConstants.CATALOG_FAMILY,
//                HConstants.STARTCODE_QUALIFIER).getValue();
            assertNotNull(val);
            assertFalse(val.length == 0);
            long startCode = Bytes.toLong(val);
            assertEquals(START_CODE, startCode);
          }

          if(serverName != null) {
            assertTrue(hasColumn(results, HConstants.CATALOG_FAMILY,
                HConstants.SERVER_QUALIFIER));
            val = CellUtil.cloneValue(getColumn(results, HConstants.CATALOG_FAMILY,
                HConstants.SERVER_QUALIFIER));
            assertNotNull(val);
            assertFalse(val.length == 0);
            String server = Bytes.toString(val);
            assertEquals(0, server.compareTo(serverName));
          }
        }
      } finally {
        InternalScanner s = scanner;
        scanner = null;
        if(s != null) {
          s.close();
        }
      }
    }
  }

  private boolean hasColumn(final List<Cell> kvs, final byte [] family,
      final byte [] qualifier) {
    for (Cell kv: kvs) {
      if (CellUtil.matchingFamily(kv, family) && CellUtil.matchingQualifier(kv, qualifier)) {
        return true;
      }
    }
    return false;
  }

  private Cell getColumn(final List<Cell> kvs, final byte [] family,
      final byte [] qualifier) {
    for (Cell kv: kvs) {
      if (CellUtil.matchingFamily(kv, family) && CellUtil.matchingQualifier(kv, qualifier)) {
        return kv;
      }
    }
    return null;
  }


  /** Use get to retrieve the HRegionInfo and validate it */
  private void getRegionInfo(Table table) throws IOException {
    Get get = new Get(ROW_KEY);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    Result result = table.get(get);
    byte [] bytes = result.value();
    validateRegionInfo(bytes);
  }

  /**
   * Tests to do a sync flush during the middle of a scan. This is testing the StoreScanner
   * update readers code essentially.  This is not highly concurrent, since its all 1 thread.
   * HBase-910.
   * @throws Exception
   */
  @Test
  public void testScanAndSyncFlush() throws Exception {
    this.region = TEST_UTIL.createLocalHRegion(TESTTABLEDESC, null, null);
    Table hri = new RegionAsTable(region);
    try {
        LOG.info("Added: " +
          HBaseTestCase.addContent(hri, Bytes.toString(HConstants.CATALOG_FAMILY),
            Bytes.toString(HConstants.REGIONINFO_QUALIFIER)));
      int count = count(hri, -1, false);
      assertEquals(count, count(hri, 100, false)); // do a sync flush.
    } catch (Exception e) {
      LOG.error("Failed", e);
      throw e;
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
    }
  }

  /**
   * Tests to do a concurrent flush (using a 2nd thread) while scanning.  This tests both
   * the StoreScanner update readers and the transition from memstore -> snapshot -> store file.
   *
   * @throws Exception
   */
  @Test
  public void testScanAndRealConcurrentFlush() throws Exception {
    this.region = TEST_UTIL.createLocalHRegion(TESTTABLEDESC, null, null);
    Table hri = new RegionAsTable(region);
    try {
        LOG.info("Added: " +
          HBaseTestCase.addContent(hri, Bytes.toString(HConstants.CATALOG_FAMILY),
            Bytes.toString(HConstants.REGIONINFO_QUALIFIER)));
      int count = count(hri, -1, false);
      assertEquals(count, count(hri, 100, true)); // do a true concurrent background thread flush
    } catch (Exception e) {
      LOG.error("Failed", e);
      throw e;
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
    }
  }

  /**
   * Make sure scanner returns correct result when we run a major compaction
   * with deletes.
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testScanAndConcurrentMajorCompact() throws Exception {
    HTableDescriptor htd = TEST_UTIL.createTableDescriptor(name.getMethodName());
    this.region = TEST_UTIL.createLocalHRegion(htd, null, null);
    Table hri = new RegionAsTable(region);

    try {
      HBaseTestCase.addContent(hri, Bytes.toString(fam1), Bytes.toString(col1),
          firstRowBytes, secondRowBytes);
      HBaseTestCase.addContent(hri, Bytes.toString(fam2), Bytes.toString(col1),
          firstRowBytes, secondRowBytes);

      Delete dc = new Delete(firstRowBytes);
      /* delete column1 of firstRow */
      dc.deleteColumns(fam1, col1);
      region.delete(dc);
      region.flush(true);

      HBaseTestCase.addContent(hri, Bytes.toString(fam1), Bytes.toString(col1),
          secondRowBytes, thirdRowBytes);
      HBaseTestCase.addContent(hri, Bytes.toString(fam2), Bytes.toString(col1),
          secondRowBytes, thirdRowBytes);
      region.flush(true);

      InternalScanner s = region.getScanner(new Scan());
      // run a major compact, column1 of firstRow will be cleaned.
      region.compact(true);

      List<Cell> results = new ArrayList<Cell>();
      s.next(results);

      // make sure returns column2 of firstRow
      assertTrue("result is not correct, keyValues : " + results,
          results.size() == 1);
      assertTrue(CellUtil.matchingRow(results.get(0), firstRowBytes)); 
      assertTrue(CellUtil.matchingFamily(results.get(0), fam2));

      results = new ArrayList<Cell>();
      s.next(results);

      // get secondRow
      assertTrue(results.size() == 2);
      assertTrue(CellUtil.matchingRow(results.get(0), secondRowBytes));
      assertTrue(CellUtil.matchingFamily(results.get(0), fam1));
      assertTrue(CellUtil.matchingFamily(results.get(1), fam2));
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
    }
  }


  /*
   * @param hri Region
   * @param flushIndex At what row we start the flush.
   * @param concurrent if the flush should be concurrent or sync.
   * @return Count of rows found.
   * @throws IOException
   */
  private int count(final Table countTable, final int flushIndex, boolean concurrent)
  throws IOException {
    LOG.info("Taking out counting scan");
    Scan scan = new Scan();
    for (byte [] qualifier: EXPLICIT_COLS) {
      scan.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    }
    ResultScanner s = countTable.getScanner(scan);
    int count = 0;
    boolean justFlushed = false;
    while (s.next() != null) {
      if (justFlushed) {
        LOG.info("after next() just after next flush");
        justFlushed = false;
      }
      count++;
      if (flushIndex == count) {
        LOG.info("Starting flush at flush index " + flushIndex);
        Thread t = new Thread() {
          public void run() {
            try {
              region.flush(true);
              LOG.info("Finishing flush");
            } catch (IOException e) {
              LOG.info("Failed flush cache");
            }
          }
        };
        if (concurrent) {
          t.start(); // concurrently flush.
        } else {
          t.run(); // sync flush
        }
        LOG.info("Continuing on after kicking off background flush");
        justFlushed = true;
      }
    }
    s.close();
    LOG.info("Found " + count + " items");
    return count;
  }
}
