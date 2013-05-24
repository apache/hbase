/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.regionserver.RegionScannerHolder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * A client-side test, mostly testing scanners with various parameters.
 */
@Category(MediumTests.class)
@RunWith(Parameterized.class)
public class TestScannersFromClientSide {
  private static final Log LOG = LogFactory.getLog(TestScannersFromClientSide.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  private final boolean prefetching;
  private long maxSize;

  @Parameters
  public static final Collection<Object[]> parameters() {
    List<Object[]> prefetchings = new ArrayList<Object[]>();
    prefetchings.add(new Object[] {Long.valueOf(-1)});
    prefetchings.add(new Object[] {Long.valueOf(0)});
    prefetchings.add(new Object[] {Long.valueOf(1)});
    prefetchings.add(new Object[] {Long.valueOf(1024)});
    return prefetchings;
  }

  public TestScannersFromClientSide(Long maxPrefetchedResultSize) {
    this.maxSize = maxPrefetchedResultSize.longValue();
    if (this.maxSize < 0) {
      this.prefetching = false;
    } else {
      this.prefetching = true;
      if (this.maxSize == 0) {
        this.maxSize = RegionScannerHolder.MAX_PREFETCHED_RESULT_SIZE_DEFAULT;
      } else {
        MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
        for (JVMClusterUtil.RegionServerThread rst: cluster.getLiveRegionServerThreads()) {
          Configuration conf = rst.getRegionServer().getConfiguration();
          conf.setLong(RegionScannerHolder.MAX_PREFETCHED_RESULT_SIZE_KEY, maxSize);
        }
      }
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    long remainingPrefetchedSize = RegionScannerHolder.getPrefetchedResultSize();
    assertEquals("All prefetched results should be gone",
      0, remainingPrefetchedSize);
  }

  /**
   * Test from client side for batch of scan
   *
   * @throws Exception
   */
  @Test
  public void testScanBatchWithDefaultCaching() throws Exception {
    batchedScanWithCachingSpecified(-1);  // Using default caching which is 100
  }

    /**
     * Test from client side for batch of scan
     *
     * @throws Exception
     */
    @Test
  public void testScanBatch() throws Exception {
      batchedScanWithCachingSpecified(1);
  }

  private void batchedScanWithCachingSpecified(int caching) throws Exception {
    byte [] TABLE = Bytes.toBytes(
      "testScanBatch-" + prefetching + "_" + maxSize + "_" + caching);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 8);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);

    Put put;
    Scan scan;
    Delete delete;
    Result result;
    ClientScanner scanner;
    boolean toLog = true;
    List<KeyValue> kvListExp;

    // table: row, family, c0:0, c1:1, ... , c7:7
    put = new Put(ROW);
    for (int i=0; i < QUALIFIERS.length; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIERS[i], i, VALUE);
      put.add(kv);
    }
    ht.put(put);

    // table: row, family, c0:0, c1:1, ..., c6:2, c6:6 , c7:7
    put = new Put(ROW);
    KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIERS[6], 2, VALUE);
    put.add(kv);
    ht.put(put);

    // delete upto ts: 3
    delete = new Delete(ROW);
    delete.deleteFamily(FAMILY, 3);
    ht.delete(delete);

    // without batch
    scan = new Scan(ROW);
    scan.setCaching(caching);
    scan.setMaxVersions();
    scan.setPrefetching(prefetching);
    scanner = (ClientScanner)ht.getScanner(scan);
    verifyPrefetching(scanner);

    // c4:4, c5:5, c6:6, c7:7
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing first batch of scan");
    verifyPrefetching(scanner);

    // with batch
    scan = new Scan(ROW);
    scan.setCaching(caching);
    scan.setMaxVersions();
    scan.setBatch(2);
    scan.setPrefetching(prefetching);
    scanner = (ClientScanner)ht.getScanner(scan);
    verifyPrefetching(scanner);

    // First batch: c4:4, c5:5
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing first batch of scan");
    verifyPrefetching(scanner);

    // Second batch: c6:6, c7:7
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing second batch of scan");
    verifyPrefetching(scanner);
  }

  /**
   * Test from client side for get with maxResultPerCF set
   *
   * @throws Exception
   */
  @Test
  public void testGetMaxResults() throws Exception {
    byte [] TABLE = Bytes.toBytes("testGetMaxResults-" + prefetching + "_" + maxSize);
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 20);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Get get;
    Put put;
    Result result;
    boolean toLog = true;
    List<KeyValue> kvListExp;

    kvListExp = new ArrayList<KeyValue>();
    // Insert one CF for row[0]
    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
      kvListExp.add(kv);
    }
    ht.put(put);

    get = new Get(ROW);
    result = ht.get(get);
    verifyResult(result, kvListExp, toLog, "Testing without setting maxResults");

    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(2);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[0], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing basic setMaxResults");

    // Filters: ColumnRangeFilter
    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(5);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5],
                                        true));
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[2], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[3], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing single CF with CRF");

    // Insert two more CF for row[0]
    // 20 columns for CF2, 10 columns for CF1
    put = new Put(ROW);
    for (int i=0; i < QUALIFIERS.length; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
    }
    ht.put(put);

    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
    }
    ht.put(put);

    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(12);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    //Exp: CF1:q0, ..., q9, CF2: q0, q1, q10, q11, ..., q19
    for (int i=0; i < 10; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=0; i < 2; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
      }
    for (int i=10; i < 20; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs");

    // Filters: ColumnRangeFilter and ColumnPrefixFilter
    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(3);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, null, true));
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs + CRF");

    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(7);
    get.setFilter(new ColumnPrefixFilter(QUALIFIERS[1]));
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[1], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[1], 1, VALUE));
    for (int i=10; i < 16; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs + PFF");

  }

  /**
   * Test from client side for scan with maxResultPerCF set
   *
   * @throws Exception
   */
  @Test
  public void testScanMaxResults() throws Exception {
    byte [] TABLE = Bytes.toBytes("testScanLimit-" + prefetching + "_" + maxSize);
    byte [][] ROWS = HTestConst.makeNAscii(ROW, 2);
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 10);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Put put;
    Scan scan;
    Result result;
    boolean toLog = true;
    List<KeyValue> kvListExp, kvListScan;

    kvListExp = new ArrayList<KeyValue>();

    for (int r=0; r < ROWS.length; r++) {
      put = new Put(ROWS[r]);
      for (int c=0; c < FAMILIES.length; c++) {
        for (int q=0; q < QUALIFIERS.length; q++) {
          KeyValue kv = new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], 1, VALUE);
          put.add(kv);
          if (q < 4) {
            kvListExp.add(kv);
          }
        }
      }
      ht.put(put);
    }

    scan = new Scan();
    scan.setCaching(1);
    scan.setPrefetching(prefetching);
    scan.setMaxResultsPerColumnFamily(4);
    ClientScanner scanner = (ClientScanner)ht.getScanner(scan);
    kvListScan = new ArrayList<KeyValue>();
    while ((result = scanner.next()) != null) {
      verifyPrefetching(scanner);
      for (KeyValue kv : result.list()) {
        kvListScan.add(kv);
      }
    }
    result = new Result(kvListScan);
    verifyResult(result, kvListExp, toLog, "Testing scan with maxResults");
  }

  /**
   * Test from client side for get with rowOffset
   *
   * @throws Exception
   */
  @Test
  public void testGetRowOffset() throws Exception {
    byte [] TABLE = Bytes.toBytes("testGetRowOffset-" + prefetching + "_" + maxSize);
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 20);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Get get;
    Put put;
    Result result;
    boolean toLog = true;
    List<KeyValue> kvListExp;

    // Insert one CF for row
    kvListExp = new ArrayList<KeyValue>();
    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
      // skipping first two kvs
      if (i < 2) continue;
      kvListExp.add(kv);
    }
    ht.put(put);

    //setting offset to 2
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(2);
    result = ht.get(get);
    verifyResult(result, kvListExp, toLog, "Testing basic setRowOffset");

    //setting offset to 20
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(20);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    verifyResult(result, kvListExp, toLog, "Testing offset > #kvs");

    //offset + maxResultPerCF
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(4);
    get.setMaxResultsPerColumnFamily(5);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    for (int i=4; i < 9; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog,
      "Testing offset + setMaxResultsPerCF");

    // Filters: ColumnRangeFilter
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(1);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5],
                                        true));
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[3], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing offset with CRF");

    // Insert into two more CFs for row
    // 10 columns for CF2, 10 columns for CF1
    for(int j=2; j > 0; j--) {
      put = new Put(ROW);
      for (int i=0; i < 10; i++) {
        KeyValue kv = new KeyValue(ROW, FAMILIES[j], QUALIFIERS[i], 1, VALUE);
        put.add(kv);
      }
      ht.put(put);
    }

    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(4);
    get.setMaxResultsPerColumnFamily(2);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    //Exp: CF1:q4, q5, CF2: q4, q5
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[5], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog,
       "Testing offset + multiple CFs + maxResults");
  }

  /**
   * For testing only, find a region scanner holder for a scan.
   */
  RegionScannerHolder findRegionScannerHolder(ClientScanner scanner) {
    long scannerId = scanner.currentScannerId();
    if (scannerId == -1L) return null;

    HRegionInfo expectedRegion = scanner.currentRegionInfo();
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    for (JVMClusterUtil.RegionServerThread rst: cluster.getLiveRegionServerThreads()) {
      RegionScannerHolder rsh = rst.getRegionServer().getScannerHolder(scannerId);
      if (rsh != null && rsh.getRegionInfo().equals(expectedRegion)) {
        return rsh;
      }
    }
    return null;
  }

  void verifyPrefetching(ClientScanner scanner) throws IOException {
    long scannerId = scanner.currentScannerId();
    if (scannerId == -1L) return; // scanner is already closed
    RegionScannerHolder rsh = findRegionScannerHolder(scanner);
    assertNotNull("We should be able to find the scanner", rsh);
    boolean isPrefetchSubmitted = rsh.isPrefetchSubmitted();
    if (prefetching && (RegionScannerHolder.getPrefetchedResultSize() < this.maxSize)) {
      assertTrue("Prefetching should be submitted or no more result",
        isPrefetchSubmitted || scanner.next() == null);
    } else if (isPrefetchSubmitted) {
      // Prefetch submitted, it must be because prefetching is enabled,
      // and there was still room before it's scheduled
      long sizeBefore = RegionScannerHolder.getPrefetchedResultSize()
        - rsh.currentPrefetchedResultSize();
      assertTrue("There should have room before prefetching is submitted",
        prefetching && sizeBefore < this.maxSize);
    }
    if (isPrefetchSubmitted && rsh.waitForPrefetchingDone()) {
      assertTrue("Prefetched result size should not be 0",
        rsh.currentPrefetchedResultSize() > 0);
    }
  }

  static void verifyResult(Result result, List<KeyValue> expKvList, boolean toLog,
      String msg) {

    LOG.info(msg);
    LOG.info("Expected count: " + expKvList.size());
    LOG.info("Actual count: " + result.size());
    if (expKvList.size() == 0)
      return;

    int i = 0;
    for (KeyValue kv : result.raw()) {
      if (i >= expKvList.size()) {
        break;  // we will check the size later
      }

      KeyValue kvExp = expKvList.get(i++);
      if (toLog) {
        LOG.info("get kv is: " + kv.toString());
        LOG.info("exp kv is: " + kvExp.toString());
      }
      assertTrue("Not equal", kvExp.equals(kv));
    }

    assertEquals(expKvList.size(), result.size());
  }
}
