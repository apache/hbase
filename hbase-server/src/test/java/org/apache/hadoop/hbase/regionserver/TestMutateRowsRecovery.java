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

import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestMutateRowsRecovery {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMutateRowsRecovery.class);

  private MiniHBaseCluster cluster = null;
  private Connection connection = null;
  private static final int NB_SERVERS = 3;

  static final byte[] qual1 = Bytes.toBytes("qual1");
  static final byte[] qual2 = Bytes.toBytes("qual2");
  static final byte[] value1 = Bytes.toBytes("value1");
  static final byte[] value2 = Bytes.toBytes("value2");
  static final byte[] row1 = Bytes.toBytes("rowA");
  static final byte[] row2 = Bytes.toBytes("rowB");

  static final HBaseTestingUtility TESTING_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void before() throws Exception {
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
  }

  @AfterClass
  public static void after() throws Exception {
    TESTING_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    TESTING_UTIL.ensureSomeNonStoppedRegionServersAvailable(NB_SERVERS);
    this.connection = ConnectionFactory.createConnection(TESTING_UTIL.getConfiguration());
    this.cluster = TESTING_UTIL.getMiniHBaseCluster();
  }

  @After
  public void tearDown() throws IOException {
    if (this.connection != null) {
      this.connection.close();
    }
  }

  @Test
  public void MutateRowsAndCheckPostKill() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("test");
    Admin admin = null;
    Table hTable = null;
    try {
      admin = connection.getAdmin();
      hTable = connection.getTable(tableName);
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(fam1));
      admin.createTable(desc);

      // Add a multi
      RowMutations rm = new RowMutations(row1);
      Put p1 = new Put(row1);
      p1.addColumn(fam1, qual1, value1);
      p1.setDurability(Durability.SYNC_WAL);
      rm.add(p1);
      hTable.mutateRow(rm);

      // Add a put
      Put p2 = new Put(row1);
      p2.addColumn(fam1, qual2, value2);
      p2.setDurability(Durability.SYNC_WAL);
      hTable.put(p2);

      HRegionServer rs1 = TESTING_UTIL.getRSForFirstRegionInTable(tableName);
      long now = EnvironmentEdgeManager.currentTime();
      // Send the RS Load to ensure correct lastflushedseqid for stores
      rs1.tryRegionServerReport(now - 30000, now);
      // Kill the RS to trigger wal replay
      cluster.killRegionServer(rs1.serverName);

      // Ensure correct data exists
      Get g1 = new Get(row1);
      Result result = hTable.get(g1);
      assertTrue(result.getValue(fam1, qual1) != null);
      assertEquals(0, Bytes.compareTo(result.getValue(fam1, qual1), value1));
      assertTrue(result.getValue(fam1, qual2) != null);
      assertEquals(0, Bytes.compareTo(result.getValue(fam1, qual2), value2));
    } finally {
      if (admin != null) {
        admin.close();
      }
      if (hTable != null) {
        hTable.close();
      }
    }
  }
}
