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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
   TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
   TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);

   TEST_UTIL.startMiniCluster(1);
 }

 @AfterClass
 public static void tearDownAfterClass() throws Exception {
   TEST_UTIL.shutdownMiniCluster();
 }

 @Test
 public void testFlushNonMobFile() throws InterruptedException {
   String TN = "testFlushNonMobFile";
   HTable table = null;
   HBaseAdmin admin = null;

   try {
     HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
     HColumnDescriptor hcd = new HColumnDescriptor(family);
     hcd.setMaxVersions(4);
     desc.addFamily(hcd);

     admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
     admin.createTable(desc);
     table = new HTable(TEST_UTIL.getConfiguration(), TN);

     //Put data
     Put put0 = new Put(row1);
     put0.add(family, qf1, 1, value1);
     table.put(put0);

     //Put more data
     Put put1 = new Put(row2);
     put1.add(family, qf2, 1, value2);
     table.put(put1);

     //Flush
     table.flushCommits();
     admin.flush(TN);

     Scan scan = new Scan();
     scan.addColumn(family, qf1);
     scan.setMaxVersions(4);
     ResultScanner scanner = table.getScanner(scan);

     //Compare
     Result result = scanner.next();
     int size = 0;
     while (result != null) {
       size++;
       List<Cell> cells = result.getColumnCells(family, qf1);
       // Verify the cell size
       Assert.assertEquals(1, cells.size());
       // Verify the value
       Assert.assertEquals(Bytes.toString(value1),
           Bytes.toString(CellUtil.cloneValue(cells.get(0))));
       result = scanner.next();
     }
     scanner.close();
     Assert.assertEquals(1, size);
     admin.close();
   } catch (MasterNotRunningException e1) {
     e1.printStackTrace();
   } catch (ZooKeeperConnectionException e2) {
     e2.printStackTrace();
   } catch (IOException e3) {
     e3.printStackTrace();
   }
 }

 @Test
 public void testFlushMobFile() throws InterruptedException {
   String TN = "testFlushMobFile";
   HTable table = null;
   HBaseAdmin admin = null;

   try {
     HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
     HColumnDescriptor hcd = new HColumnDescriptor(family);
     hcd.setMobEnabled(true);
     hcd.setMobThreshold(3L);
     hcd.setMaxVersions(4);
     desc.addFamily(hcd);

     admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
     admin.createTable(desc);
     table = new HTable(TEST_UTIL.getConfiguration(), TN);

     //put data
     Put put0 = new Put(row1);
     put0.add(family, qf1, 1, value1);
     table.put(put0);

     //put more data
     Put put1 = new Put(row2);
     put1.add(family, qf2, 1, value2);
     table.put(put1);

     //flush
     table.flushCommits();
     admin.flush(TN);

     //Scan
     Scan scan = new Scan();
     scan.addColumn(family, qf1);
     scan.setMaxVersions(4);
     ResultScanner scanner = table.getScanner(scan);

     //Compare
     Result result = scanner.next();
     int size = 0;
     while (result != null) {
       size++;
       List<Cell> cells = result.getColumnCells(family, qf1);
       // Verify the the cell size
       Assert.assertEquals(1, cells.size());
       // Verify the value
       Assert.assertEquals(Bytes.toString(value1),
           Bytes.toString(CellUtil.cloneValue(cells.get(0))));
       result = scanner.next();
     }
     scanner.close();
     Assert.assertEquals(1, size);
     admin.close();
   } catch (MasterNotRunningException e1) {
     e1.printStackTrace();
   } catch (ZooKeeperConnectionException e2) {
     e2.printStackTrace();
   } catch (IOException e3) {
     e3.printStackTrace();
   }
 }
}
