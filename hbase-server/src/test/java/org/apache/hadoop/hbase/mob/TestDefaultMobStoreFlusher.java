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
package org.apache.hadoop.hbase.mob;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestDefaultMobStoreFlusher {

 private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
 private final static byte [] row1 = Bytes.toBytes("row1");
 private final static byte [] row2 = Bytes.toBytes("row2");
 private final static byte [] family = Bytes.toBytes("family");
 private final static byte [] qf1 = Bytes.toBytes("qf1");
 private final static byte [] qf2 = Bytes.toBytes("qf2");
 private final static byte [] value1 = Bytes.toBytes("value1");
 private final static byte [] value2 = Bytes.toBytes("value2");

 @BeforeClass
 public static void setUpBeforeClass() throws Exception {
   TEST_UTIL.startMiniCluster(1);
 }

 @AfterClass
 public static void tearDownAfterClass() throws Exception {
   TEST_UTIL.shutdownMiniCluster();
 }

 @Test
 public void testFlushNonMobFile() throws Exception {
   TableName tn = TableName.valueOf("testFlushNonMobFile");
   HTableDescriptor desc = new HTableDescriptor(tn);
   HColumnDescriptor hcd = new HColumnDescriptor(family);
   hcd.setMaxVersions(4);
   desc.addFamily(hcd);

   testFlushFile(desc);
 }

 @Test
 public void testFlushMobFile() throws Exception {
   TableName tn = TableName.valueOf("testFlushMobFile");
   HTableDescriptor desc = new HTableDescriptor(tn);
   HColumnDescriptor hcd = new HColumnDescriptor(family);
   hcd.setMobEnabled(true);
   hcd.setMobThreshold(3L);
   hcd.setMaxVersions(4);
   desc.addFamily(hcd);

   testFlushFile(desc);
 }

 private void testFlushFile(HTableDescriptor htd) throws Exception {
    Table table = null;
    try {
      table = TEST_UTIL.createTable(htd, null);

      //put data
      Put put0 = new Put(row1);
      put0.addColumn(family, qf1, 1, value1);
      table.put(put0);

      //put more data
      Put put1 = new Put(row2);
      put1.addColumn(family, qf2, 1, value2);
      table.put(put1);

      //flush
      TEST_UTIL.flush(htd.getTableName());

      //Scan
      Scan scan = new Scan();
      scan.addColumn(family, qf1);
      scan.setMaxVersions(4);
      ResultScanner scanner = table.getScanner(scan);

      //Compare
      int size = 0;
      for (Result result: scanner) {
        size++;
        List<Cell> cells = result.getColumnCells(family, qf1);
        // Verify the cell size
        Assert.assertEquals(1, cells.size());
        // Verify the value
        Assert.assertArrayEquals(value1, CellUtil.cloneValue(cells.get(0)));
      }
      scanner.close();
      Assert.assertEquals(1, size);
    } finally {
      table.close();
    }
  }
}
