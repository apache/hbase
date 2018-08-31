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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestTableRecordReader {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableRecordReader.class);

  private static TableName TABLE_NAME = TableName.valueOf("TestTableRecordReader");

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

    createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);
  }

  private static void createTestTable(TableName name, byte[][] rows, byte[][] families,
      byte[][] qualifiers, byte[] cellValue) throws IOException {
    TEST_UTIL.createTable(name, families).put(createPuts(rows, families, qualifiers, cellValue));
  }

  private static List<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
      byte[] value) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (int row = 0; row < rows.length; row++) {
      Put put = new Put(rows[row]);
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

  @Test
  public void test() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Table table = conn.getTable(TABLE_NAME)) {
      org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl trr =
          new org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl();
      Scan scan =
          new Scan().setMaxResultSize(1).setCaching(Integer.MAX_VALUE).setNeedCursorResult(true);
      trr.setScan(scan);
      trr.setHTable(table);
      trr.initialize(null, null);
      int num = 0;
      while (trr.nextKeyValue()) {
        num++;
      }
      assertEquals(NUM_ROWS * NUM_FAMILIES * NUM_QUALIFIERS, num);
    }
  }
}
