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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestScannerCursor {

  private static final Log LOG =
      LogFactory.getLog(TestScannerCursor.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Table TABLE = null;

  /**
   * Table configuration
   */
  private static TableName TABLE_NAME = TableName.valueOf("TestScannerCursor");

  private static int NUM_ROWS = 5;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  private static int NUM_FAMILIES = 2;
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  private static int NUM_QUALIFIERS = 2;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  private static int VALUE_SIZE = 10;
  private static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);

  private static final int TIMEOUT = 4000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, TIMEOUT);

    // Check the timeout condition after every cell
    conf.setLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK, 1);
    TEST_UTIL.startMiniCluster(1);

    TABLE = createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);

  }

  static Table createTestTable(TableName name, byte[][] rows, byte[][] families,
      byte[][] qualifiers, byte[] cellValue) throws IOException {
    Table ht = TEST_UTIL.createTable(name, families);
    List<Put> puts = createPuts(rows, families, qualifiers, cellValue);
    ht.put(puts);
    return ht;
  }

  static ArrayList<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
      byte[] value) throws IOException {
    Put put;
    ArrayList<Put> puts = new ArrayList<>();

    for (int row = 0; row < rows.length; row++) {
      put = new Put(rows[row]);
      for (int fam = 0; fam < families.length; fam++) {
        for (int qual = 0; qual < qualifiers.length; qual++) {
          KeyValue kv = new KeyValue(rows[row], families[fam], qualifiers[qual], qual, value);
          put.add(kv);
        }
      }
      puts.add(put);
    }

    return puts;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static class SparseFilter extends FilterBase {

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
      Threads.sleep(TIMEOUT / 2 + 100);
      return Bytes.equals(CellUtil.cloneRow(v), ROWS[NUM_ROWS - 1]) ? ReturnCode.INCLUDE
          : ReturnCode.SKIP;
    }

    public static Filter parseFrom(final byte[] pbBytes) {
      return new SparseFilter();
    }
  }

  @Test
  public void testHeartbeatWithSparseFilter() throws Exception {
    Scan scan = new Scan();
    scan.setMaxResultSize(Long.MAX_VALUE);
    scan.setCaching(Integer.MAX_VALUE);
    scan.setNeedCursorResult(true);
    scan.setAllowPartialResults(true);
    scan.setFilter(new SparseFilter());
    try(ResultScanner scanner = TABLE.getScanner(scan)) {
      int num = 0;
      Result r;
      while ((r = scanner.next()) != null) {

        if (num < (NUM_ROWS - 1) * NUM_FAMILIES * NUM_QUALIFIERS) {
          Assert.assertTrue(r.isCursor());
          Assert.assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS], r.getCursor().getRow());
        } else {
          Assert.assertFalse(r.isCursor());
          Assert.assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS], r.getRow());
        }
        num++;
      }
    }
  }

  @Test
  public void testSizeLimit() throws IOException {
    Scan scan = new Scan();
    scan.setMaxResultSize(1);
    scan.setCaching(Integer.MAX_VALUE);
    scan.setNeedCursorResult(true);
    try (ResultScanner scanner = TABLE.getScanner(scan)) {
      int num = 0;
      Result r;
      while ((r = scanner.next()) != null) {

        if (num % (NUM_FAMILIES * NUM_QUALIFIERS) != (NUM_FAMILIES * NUM_QUALIFIERS)-1) {
          Assert.assertTrue(r.isCursor());
          Assert.assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS], r.getCursor().getRow());
        } else {
          Assert.assertFalse(r.isCursor());
          Assert.assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS], r.getRow());
        }
        num++;
      }
    }
  }

}
