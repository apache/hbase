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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, LargeTests.class})
public class TestWALSplitWithDeletedTableData {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
      .forClass(TestWALSplitWithDeletedTableData.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testWALSplitWithDeletedTableData() throws Exception {
    final byte[] CFNAME = Bytes.toBytes("f1");
    final byte[] QNAME = Bytes.toBytes("q1");
    final byte[] VALUE = Bytes.toBytes("v1");
    final TableName t1 = TableName.valueOf("t1");
    final TableName t2 = TableName.valueOf("t2");
    final byte[][] splitRows = { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c"),
        Bytes.toBytes("d") };
    TableDescriptorBuilder htdBuilder1 = TableDescriptorBuilder.newBuilder(t1);
    htdBuilder1.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CFNAME).build());
    Table tab1 = TEST_UTIL.createTable(htdBuilder1.build(), splitRows);
    TableDescriptorBuilder htdBuilder2 = TableDescriptorBuilder.newBuilder(t2);
    htdBuilder2.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CFNAME).build());
    Table tab2 = TEST_UTIL.createTable(htdBuilder2.build(), splitRows);
    List<Put> puts = new ArrayList<Put>(4);
    byte[][] rks = { Bytes.toBytes("ac"), Bytes.toBytes("ba"), Bytes.toBytes("ca"),
        Bytes.toBytes("dd") };
    for (byte[] rk : rks) {
      puts.add(new Put(rk).addColumn(CFNAME, QNAME, VALUE));
    }
    tab1.put(puts);
    tab2.put(puts);
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    TEST_UTIL.deleteTable(t1);
    Path tableDir = CommonFSUtils.getWALTableDir(TEST_UTIL.getConfiguration(), t1);
    // Dropping table 't1' removed the table directory from the WAL FS completely
    assertFalse(TEST_UTIL.getDFSCluster().getFileSystem().exists(tableDir));
    ServerName rs1 = cluster.getRegionServer(1).getServerName();
    // Kill one RS and wait for the WAL split and replay be over.
    cluster.killRegionServer(rs1);
    cluster.waitForRegionServerToStop(rs1, 60 * 1000);
    assertEquals(1, cluster.getNumLiveRegionServers());
    Thread.sleep(1 * 1000);
    TEST_UTIL.waitUntilNoRegionsInTransition(60 * 1000);
    // Table 't1' is dropped. Assert table directory does not exist in WAL FS after WAL split.
    assertFalse(TEST_UTIL.getDFSCluster().getFileSystem().exists(tableDir));
    // Assert the table t2 region's data getting replayed after WAL split and available
    for (byte[] rk : rks) {
      Result result = tab2.get(new Get(rk));
      assertFalse(result.isEmpty());
      Cell cell = result.getColumnLatestCell(CFNAME, QNAME);
      assertNotNull(cell);
      assertTrue(CellUtil.matchingValue(cell, VALUE));
    }
  }
}
