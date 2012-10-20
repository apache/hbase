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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class TestMaxResponseSize{
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  private static int SLAVES = 3;
  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
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

    Put put;
    long kvSize = (new KeyValue(ROWS[0], FAMILIES[0], QUALIFIERS[0], 1, VALUE))
      .getLength();
    long rowSize = (kvSize * (FAMILIES.length * QUALIFIERS.length));
    
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
     */
    long responseSize = rowSize / 2;
    long scanCntExp = ((rowSize + responseSize - 1) / responseSize) * ROWS.length;
    testScan(ht, rowSize, kvSize, scanCntExp, kvListExp, responseSize, true);
    
    scanCntExp = ROWS.length;
    testScan(ht, rowSize, kvSize, scanCntExp, kvListExp, responseSize, false);
    
    /**
     * Test with a big responseSize across mutliple rows
     * The response size is set to only fit one and a half rows.
     * if partialRow == true, the expected number of fetches is 4 
     * in order to retrieve all the 3 rows. 
     * if partialRow == false, the exptect number of fetch is 3 since each time 
     * it still fetches entire row at a time.
     */
    responseSize = rowSize + rowSize / 2 ;
    long nbRows = (responseSize + rowSize - 1)/ rowSize; 
    scanCntExp = (ROWS.length + nbRows - 1) / nbRows * nbRows;
    testScan(ht, rowSize, kvSize, scanCntExp, kvListExp, responseSize, true);
    
    scanCntExp = ROWS.length;
    testScan(ht, rowSize, kvSize, scanCntExp, kvListExp, responseSize, false);

  } 

  void testScan(HTable ht, long rowSize, long kvSize,
      long scanCntExp, List<KeyValue> kvListExp, 
      long responseSize, boolean partialRow) throws Exception{ 
    long availResponseSize = responseSize;
    long kvNumPerRow = rowSize/kvSize;
    int scanCntAct = 0;
    boolean toLog  = true; 
    Scan scan = new Scan();
    Result result;
    ResultScanner scanner;
    List<KeyValue> kvListScan = new ArrayList<KeyValue>();  
    scan.setResponseSetting(responseSize, partialRow);
    scanner = ht.getScanner(scan);
    kvListScan.clear();
    while ((result = scanner.next()) != null) {
      scanCntAct++;
      for (KeyValue kv : result.list()) {
        kvListScan.add(kv);
      }
    }
    
    System.out.println("total number of scans: " + scanCntAct + ", "
        + scanCntExp+ ","+responseSize+","+partialRow);
    assertEquals(scanCntExp, scanCntAct);
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
