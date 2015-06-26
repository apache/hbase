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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(LargeTests.class)
public class TestSizeFailures {
  private static final Log LOG = LogFactory.getLog(TestSizeFailures.class);
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  protected static int SLAVES = 1;
  private static TableName TABLENAME;
  private static final int NUM_ROWS = 1000 * 1000, NUM_COLS = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)RpcClient.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.table.sanity.checks", true); // ignore sanity checks in the server
    TEST_UTIL.startMiniCluster(SLAVES);

    // Write a bunch of data
    TABLENAME = TableName.valueOf("testSizeFailures");
    List<byte[]> qualifiers = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      qualifiers.add(Bytes.toBytes(Integer.toString(i)));
    }

    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    HTableDescriptor desc = new HTableDescriptor(TABLENAME);
    desc.addFamily(hcd);
    byte[][] splits = new byte[9][2];
    for (int i = 1; i < 10; i++) {
      int split = 48 + i;
      splits[i - 1][0] = (byte) (split >>> 8);
      splits[i - 1][0] = (byte) (split);
    }
    TEST_UTIL.getHBaseAdmin().createTable(desc, splits);
    Connection conn = TEST_UTIL.getConnection();

    try (Table table = conn.getTable(TABLENAME)) {
      List<Put> puts = new LinkedList<>();
      for (int i = 0; i < NUM_ROWS; i++) {
        Put p = new Put(Bytes.toBytes(Integer.toString(i)));
        for (int j = 0; j < NUM_COLS; j++) {
          byte[] value = new byte[50];
          Bytes.random(value);
          p.addColumn(FAMILY, Bytes.toBytes(Integer.toString(j)), value);
        }
        puts.add(p);

        if (puts.size() == 1000) {
          table.batch(puts, null);
          puts.clear();
        }
      }

      if (puts.size() > 0) {
        table.batch(puts, null);
      }
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Basic client side validation of HBASE-13262
   */
  @Test
  public void testScannerSeesAllRecords() throws Exception {
    Connection conn = TEST_UTIL.getConnection();
    try (Table table = conn.getTable(TABLENAME)) {
      Scan s = new Scan();
      s.addFamily(FAMILY);
      s.setMaxResultSize(-1);
      s.setBatch(-1);
      s.setCaching(500);
      Entry<Long,Long> entry = sumTable(table.getScanner(s));
      long rowsObserved = entry.getKey();
      long entriesObserved = entry.getValue();

      // Verify that we see 1M rows and 10M cells
      assertEquals(NUM_ROWS, rowsObserved);
      assertEquals(NUM_ROWS * NUM_COLS, entriesObserved);
    }
  }

  /**
   * Basic client side validation of HBASE-13262
   */
  @Test
  public void testSmallScannerSeesAllRecords() throws Exception {
    Connection conn = TEST_UTIL.getConnection();
    try (Table table = conn.getTable(TABLENAME)) {
      Scan s = new Scan();
      s.setSmall(true);
      s.addFamily(FAMILY);
      s.setMaxResultSize(-1);
      s.setBatch(-1);
      s.setCaching(500);
      Entry<Long,Long> entry = sumTable(table.getScanner(s));
      long rowsObserved = entry.getKey();
      long entriesObserved = entry.getValue();

      // Verify that we see 1M rows and 10M cells
      assertEquals(NUM_ROWS, rowsObserved);
      assertEquals(NUM_ROWS * NUM_COLS, entriesObserved);
    }
  }

  /**
   * Count the number of rows and the number of entries from a scanner
   *
   * @param scanner
   *          The Scanner
   * @return An entry where the first item is rows observed and the second is entries observed.
   */
  private Entry<Long,Long> sumTable(ResultScanner scanner) {
    long rowsObserved = 0l;
    long entriesObserved = 0l;

    // Read all the records in the table
    for (Result result : scanner) {
      rowsObserved++;
      while (result.advance()) {
        entriesObserved++;
      }
    }
    return Maps.immutableEntry(rowsObserved,entriesObserved);
  }
}
