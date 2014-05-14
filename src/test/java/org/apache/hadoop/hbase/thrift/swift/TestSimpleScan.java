/*
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.thrift.swift;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableAsync;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSimpleScan {
  private final Log LOG = LogFactory.getLog(TestSimpleScan.class);
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final int SLAVES = 1;
  byte[] tableName = Bytes.toBytes("testSimpleGetUsingSwift");
  byte[] family1 = Bytes.toBytes("family1");
  byte[] family2 = Bytes.toBytes("family2");
  byte[][] families = new byte[][] { family1, family2 };
  String rowPrefix1 = "r-f1-";
  String rowPrefix2 = "r-f2-";
  String valuePrefix1 = "val-f1-";
  String valuePrefix2 = "val-f2-";
  String rowFormat = "%s%03d";

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests the use of swift to perform a scan.
   * @throws IOException
   */
  @Test
  public void testSimpleScanUsingSwift() throws IOException {

    HTable ht = TEST_UTIL.createTable(tableName, families);
    // Inserting some rows in the family1
    // The rows look as follows : "r-f1-020"
    insertRows(ht, 100, family1, rowPrefix1, valuePrefix1);
    // Flushing the data to disk. rows look as follows : "r-f2-020"
    TEST_UTIL.flush(tableName);
    // Inserting some rows into family2
    insertRows(ht, 100, family2, rowPrefix2, valuePrefix2);

    Scan s = new Scan.Builder().addColumn(family1, null).setCaching(10)
        .setStartRow(Bytes.toBytes(String.format(rowFormat, rowPrefix1, 20))).create();

    ResultScanner scanner = ht.getScanner(s);
    int rowNumber = 19;

    // Given the scan, we expect the scan to go through r-f1-020, r-f1-021, ..
    // r-f1-100
    for (int i = 0; i < 100; i++) {
      Result r = scanner.next();
      LOG.debug(r);
      if (r == null) {
        // the scan should stop after we scan the row r-f1-100
        assertTrue(rowNumber >= 100);
        break;
      }
      rowNumber++;
      assertTrue(Bytes.BYTES_COMPARATOR.compare(r.getRow(),
          Bytes.toBytes(String.format(rowFormat, rowPrefix1, rowNumber))) == 0);
      assertTrue(Bytes.BYTES_COMPARATOR.compare(r.getValue(family1, null),
          Bytes.toBytes(String.format(rowFormat, valuePrefix1, rowNumber))) == 0);
    }
  }

  /**
   * Inserts Rows into the table. The row keys look as <rowPrefix><001>
   * @param table
   * @param numberOfRows
   * @param family
   * @param rowPrefix
   * @param valuePrefix
   * @throws IOException
   */
  private void insertRows(HTable table, int numberOfRows, byte[] family,
      String rowPrefix, String valuePrefix) throws IOException {
    for (int i = 1; i <= numberOfRows; i++) {
      Put put = new Put(Bytes.toBytes(String.format(rowFormat, rowPrefix, i)));
      put.add(family, null, Bytes.toBytes(String.format(rowFormat, valuePrefix, i)));
      table.put(put);
    }
  }

  /**
   * Tests the results of Scanning contains TRegionInfo
   */
  @Test
  public void testScanResultsTHReginoInfo() throws Exception {
    TEST_UTIL.createTable(tableName, families).close();
    TEST_UTIL.flush();

    HTable ht = new HTableAsync(TEST_UTIL.getConfiguration(),
        HConstants.META_TABLE_NAME);

    Scan s = new Scan.Builder().addFamily(HConstants.CATALOG_FAMILY)
        .create();

    int count = 0;
    ResultScanner scanner = ht.getScanner(s);
    while (true) {
      Result r = scanner.next();
      if (r == null) {
        break;
      }
      LOG.debug(r);
      count++;
      byte[] ri = r.getValue(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
      Assert.assertNotNull("REGIONINFO_QUALIFIER not exists", ri);
      byte[] tri = r.getValue(HConstants.CATALOG_FAMILY,
          HConstants.THRIFT_REGIONINFO_QUALIFIER);
      Assert.assertNotNull("THRIFT_REGIONINFO_QUALIFIER not exists", tri);
    }
    Assert.assertEquals("Number of rows", 1, count);
  }
}
