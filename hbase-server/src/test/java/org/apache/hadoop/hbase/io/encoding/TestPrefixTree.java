/*
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

package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestPrefixTree {
  private static final String row4 = "a-b-B-2-1402397300-1402416535";
  private static final byte[] row4_bytes = Bytes.toBytes(row4);
  private static final String row3 = "a-b-A-1-1402397227-1402415999";
  private static final byte[] row3_bytes = Bytes.toBytes(row3);
  private static final String row2 = "a-b-A-1-1402329600-1402396277";
  private static final byte[] row2_bytes = Bytes.toBytes(row2);
  private static final String row1 = "a-b-A-1";
  private static final byte[] row1_bytes = Bytes.toBytes(row1);
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte[] fam = Bytes.toBytes("cf_1");
  private final static byte[] qual1 = Bytes.toBytes("qf_1");
  private final static byte[] qual2 = Bytes.toBytes("qf_2");
  public static Configuration conf;

  @Rule
  public final TestName TEST_NAME = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    conf.setBoolean("hbase.online.schema.update.enable", true);
    conf.setInt("hbase.client.scanner.timeout.period", 600000);
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHBASE11728() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Table table = null;
    try {
      Admin hBaseAdmin = TEST_UTIL.getHBaseAdmin();
      HColumnDescriptor colDesc = new HColumnDescriptor(fam);
      HTableDescriptor desc = new HTableDescriptor(tableName);
      colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
      colDesc.setTimeToLive(15552000);
      desc.addFamily(colDesc);
      hBaseAdmin.createTable(desc);
      table = new HTable(conf, tableName);
      Put put = new Put(Bytes.toBytes("a-b-0-0"));
      put.add(fam, qual1, Bytes.toBytes("c1-value"));
      table.put(put);
      put = new Put(row1_bytes);
      put.add(fam, qual1, Bytes.toBytes("c1-value"));
      table.put(put);
      put = new Put(row2_bytes);
      put.add(fam, qual2, Bytes.toBytes("c2-value"));
      table.put(put);
      put = new Put(row3_bytes);
      put.add(fam, qual2, Bytes.toBytes("c2-value-2"));
      table.put(put);
      put = new Put(row4_bytes);
      put.add(fam, qual2, Bytes.toBytes("c2-value-3"));
      table.put(put);
      hBaseAdmin.flush(tableName);
      String[] rows = new String[3];
      rows[0] = row1;
      rows[1] = row2;
      rows[2] = row3;
      byte[][] val = new byte[3][];
      val[0] = Bytes.toBytes("c1-value");
      val[1] = Bytes.toBytes("c2-value");
      val[2] = Bytes.toBytes("c2-value-2");
      Scan scan = new Scan();
      scan.setStartRow(row1_bytes);
      scan.setStopRow(Bytes.toBytes("a-b-A-1:"));
      ResultScanner scanner = table.getScanner(scan);
      Result[] next = scanner.next(10);
      assertEquals(3, next.length);
      int i = 0;
      for (Result res : next) {
        CellScanner cellScanner = res.cellScanner();
        while (cellScanner.advance()) {
          assertEquals(rows[i], Bytes.toString(cellScanner.current().getRowArray(), cellScanner
              .current().getRowOffset(), cellScanner.current().getRowLength()));
          assertEquals(Bytes.toString(val[i]), Bytes.toString(
              cellScanner.current().getValueArray(), cellScanner.current().getValueOffset(),
              cellScanner.current().getValueLength()));
        }
        i++;
      }
      scanner.close();
      // Add column
      scan = new Scan();
      scan.addColumn(fam, qual2);
      scan.setStartRow(row1_bytes);
      scan.setStopRow(Bytes.toBytes("a-b-A-1:"));
      scanner = table.getScanner(scan);
      next = scanner.next(10);
      assertEquals(2, next.length);
      i = 1;
      for (Result res : next) {
        CellScanner cellScanner = res.cellScanner();
        while (cellScanner.advance()) {
          assertEquals(rows[i], Bytes.toString(cellScanner.current().getRowArray(), cellScanner
              .current().getRowOffset(), cellScanner.current().getRowLength()));
        }
        i++;
      }
      scanner.close();
      i = 1;
      scan = new Scan();
      scan.addColumn(fam, qual2);
      scan.setStartRow(Bytes.toBytes("a-b-A-1-"));
      scan.setStopRow(Bytes.toBytes("a-b-A-1:"));
      scanner = table.getScanner(scan);
      next = scanner.next(10);
      assertEquals(2, next.length);
      for (Result res : next) {
        CellScanner cellScanner = res.cellScanner();
        while (cellScanner.advance()) {
          assertEquals(rows[i], Bytes.toString(cellScanner.current().getRowArray(), cellScanner
              .current().getRowOffset(), cellScanner.current().getRowLength()));
        }
        i++;
      }
      scanner.close();
      scan = new Scan();
      scan.addColumn(fam, qual2);
      scan.setStartRow(Bytes.toBytes("a-b-A-1-140239"));
      scan.setStopRow(Bytes.toBytes("a-b-A-1:"));
      scanner = table.getScanner(scan);
      next = scanner.next(10);
      assertEquals(1, next.length);
      scanner.close();
    } finally {
      table.close();
    }
  }
}
