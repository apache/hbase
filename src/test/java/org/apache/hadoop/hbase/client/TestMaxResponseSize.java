/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMaxResponseSize{
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  private static int NUM_RS = 3;
  private static int NUM_REGION = 10;
  private static int NUM_VERSION = 3;
  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(NUM_RS);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  public void createTestTable() throws Exception {
  }

  /**
   * Test from client side for scans that are cross multiple regions
   * @throws Exception
   */
  @Test
  public void testScanCrossRegion() throws Exception {
    byte [] TABLE = Bytes.toBytes("testScanCrossRegion");
    byte[][] FAMILIES = { Bytes.toBytes("MyCF1") };
    List<KeyValue> kvListExp = new ArrayList<KeyValue>();

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, NUM_VERSION,
        Bytes.toBytes("row0"), Bytes.toBytes("row99"), NUM_REGION);
    
    TEST_UTIL.waitUntilAllRegionsAssigned(NUM_REGION);
   
    Random rand = new Random(System.currentTimeMillis());
    for (int iRow = 0; iRow < 100; ++iRow) {
      final byte[] row = Bytes.toBytes(String.format("row%02d", iRow));
      Put put = new Put(row);
      final long ts = System.currentTimeMillis();
      for (int iCol = 0; iCol < 10; ++iCol) {
        final byte[] cf = FAMILIES[0];

        final byte[] qual = Bytes.toBytes("col" + iCol);
        final byte[] value = Bytes.toBytes("value_for_row_" + iRow + "_cf_"
            + Bytes.toStringBinary(cf) + "_col_" + iCol + "_ts_" + ts
            + "_random_" + rand.nextLong());
        KeyValue kv = new KeyValue(row, cf, qual, ts, value);
        put.add(kv);
        kvListExp.add(kv);
      }
      ht.put(put);
      ht.flushCommits();
    }

    boolean toLog = true;
    Scan scan = new Scan();
    Result result;
    // each region have 10 rows, we fetch 5 rows at a time
    scan.setCaching(5);
    ResultScanner scanner;
    List<KeyValue> kvListScan = new ArrayList<KeyValue>();
    scanner = ht.getScanner(scan);
    kvListScan.clear();
    // do a full scan of the table that is split among multiple regions
    while ((result = scanner.next()) != null) {
      for (KeyValue kv : result.list()) {
        kvListScan.add(kv);
      }
    }
    scanner.close();
    result = new Result(kvListScan);
    verifyResult(result, kvListExp, toLog, "testScanCrossRegion");
  }

  /**
   * Test from client side for scan with responseSize and partialRow
   * responseSize is small value
   * @throws Exception
   */
  @Test
  public void testScanMaxRequstSize() throws Exception {    
    byte [] TABLE = Bytes.toBytes("testScanMaxRequstSize");
    byte [][] ROWS= makeNAscii(ROW, 3);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 10);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    List<KeyValue> kvListExp = new ArrayList<KeyValue>();
    ht.setProfiling(true);
    Put put;
    int kvSize = (new KeyValue(ROWS[0], FAMILIES[0], QUALIFIERS[0], 1, VALUE))
      .getLength();
    int rowSize = (kvSize * (FAMILIES.length * QUALIFIERS.length));
    
    for (int r=0; r < ROWS.length; r++) {
      put = new Put(ROWS[r]);
      for (int c=0; c < FAMILIES.length; c++) { 
        for (int q=0; q < QUALIFIERS.length; q++) { 
          KeyValue kv = new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], 1, VALUE);
          put.add(kv);
          kvListExp.add(kv);
        }
      }
      ht.put(put);
    }
    
    /**
     * Test with the small bufferSize that is smaller rowSize 
     * The response size is set to only fit half a row.
     * if partialRow == true, the expected number of fetches is 6 in order to 
     * retrieve all the 3 rows, otherwise we can fetch an entire row each time,
     * which makes the expected number to be 3.
     * 
     * x x x x|x x x x     [2]
     * < rpc1 >< rpc2 >
     * x x x x|x x x x     [2]
     * < rpc3 >< rpc4 >
     * x x x x|x x x x     [2]
     * < rpc5 >< rpc6 >
     * 
     * x: kv pair
     * rpc#n#: n-th rpc call
     * [n]   : the number of scanner.next() per row 
     */
    int responseSize = rowSize / 2;
    // each row will take ceil(rowSize/responseSize) times to fetch
    // and the number of rows is ROWS.length,
    // therefore the total number of fetches is
    // ceil(rowSize/responseSize) * ROWS.length
    int scanCntExp = ((rowSize + responseSize - 1) / responseSize) * ROWS.length;
    // each scanner.next trigger a rpc call
    int rpcCntExp = scanCntExp;
    testScan(ht, rowSize, kvSize, scanCntExp, rpcCntExp, kvListExp,
        responseSize, true);
    
    /**
    * x x x x|x x x x     [1]
    * <     rpc1    >
    * x x x x|x x x x     [1]
    * <     rpc2    >
    * x x x x|x x x x     [1]
    * <     rpc3    >
    */
    scanCntExp = ROWS.length;
    rpcCntExp = ROWS.length;
    testScan(ht, rowSize, kvSize, scanCntExp, rpcCntExp, kvListExp,
        responseSize, false);
    
    /**
     * Test with a big responseSize across multiple rows
     * The response size is set to only fit one and a half rows.
     * If partialRow == true, the expected number of RPC calls is 2, and the number of
     * scan.next() is 4, since we need two scan.next() to finish each RPC call
     * If partialRow == false, the expected number of RPC calls is 2 and the number of 
     * scan.next() is 3 since we need 2 scan.next() to exhaust the first RPC call.
     * 
     * x x x x x x x x    [1]
     * <       rpc1
     * x x x x|x x x x    [2]
     *        ><
     * x x x x x x x x    [1]
     *   rpc2        >
     */
    responseSize = rowSize + rowSize / 2 ;
    // nbRows: the number of rows that responseSize can at most contain
    //         (including the last partial row)
    int nbRows = (responseSize + rowSize - 1) / rowSize;
    rpcCntExp = ROWS.length * rowSize / responseSize;
    scanCntExp = rpcCntExp * nbRows;
    testScan(ht, rowSize, kvSize, scanCntExp, rpcCntExp, kvListExp,
        responseSize, true);
    
    /**
    * x x x x x x x x   [1]
    * <        rpc1
    * x x x x x x x x   [1]
    *               >  
    * x x x x x x x x   [1]
    * <     rpc2    >
    */
    scanCntExp = ROWS.length;
    rpcCntExp = (ROWS.length + nbRows - 1) / nbRows;
    testScan(ht, rowSize, kvSize, scanCntExp, rpcCntExp, kvListExp,
        responseSize, false);
  } 

  void testScan(HTable ht, int rowSize, int kvSize,
      int scanCntExp, int rpcCntExp, List<KeyValue> kvListExp, 
      int responseSize, boolean partialRow) throws Exception{ 
    int scanCntAct = 0;
    boolean toLog  = true; 
    Scan scan = new Scan();
    Result result;
    ResultScanner scanner;
    List<KeyValue> kvListScan = new ArrayList<KeyValue>();  
    scan.setCaching(responseSize, partialRow);
    scanner = ht.getScanner(scan);
    kvListScan.clear();
    int rpcCntAct = 0;
    while ((result = scanner.next()) != null) {
      scanCntAct++;
      for (KeyValue kv : result.list()) {
        kvListScan.add(kv);
      }
      if (ht.getProfilingData() != null)
        rpcCntAct++;
    }
    scanner.close();
    assertEquals(scanCntExp, scanCntAct);
    assertEquals(rpcCntExp, rpcCntAct);
    result = new Result(kvListScan);
    verifyResult(result, kvListExp, toLog, 
        "Testing scan with responseSize = " + responseSize + 
        ", partialRow = " + partialRow);
  }
  
  private void verifyResult(Result result, List<KeyValue> kvList, boolean toLog,
    String msg) {
    LOG.info(msg);
    LOG.info("Exp cnt: " + kvList.size());
    LOG.info("True cnt is: " + result.size());
    assertEquals(kvList.size(), result.size());

    if (kvList.size() == 0) return;
    int i = 0;
    for (KeyValue kv : result.sorted()) {
      KeyValue kvExp = kvList.get(i++);
      if (toLog) {
        LOG.info("get kv is: " + kv.toString());
        LOG.info("exp kv is: " + kvExp.toString());
      }
      assertTrue("Not equal", kvExp.equals(kv));
    }
  }

  private byte [][] makeNAscii(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      byte [] tail = Bytes.toBytes(Integer.toString(i));
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }
}
