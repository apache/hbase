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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-20742
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionServerCrashDisableWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerCrashDisableWAL.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("test");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    UTIL.getConfiguration().setBoolean(WALFactory.WAL_ENABLED, false);
    UTIL.startMiniCluster(2);
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME);
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    if (!rs.getRegions(TableName.META_TABLE_NAME).isEmpty()) {
      HRegionServer rs1 = UTIL.getOtherRegionServer(rs);
      UTIL.moveRegionAndWait(
        UTIL.getMiniHBaseCluster().getRegions(TABLE_NAME).get(0).getRegionInfo(),
        rs1.getServerName());
    }
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, IOException {
    HMaster master = UTIL.getMiniHBaseCluster().stopMaster(0).getMaster();
    // Shutdown master before shutting down rs
    UTIL.waitFor(30000, () -> !master.isAlive());
    RegionServerThread thread = null;
    for (RegionServerThread t : UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      if (!t.getRegionServer().getRegions(TABLE_NAME).isEmpty()) {
        thread = t;
        break;
      }
    }
    // shutdown rs
    thread.getRegionServer().abort("For testing");
    thread.join();
    // restart master
    UTIL.getMiniHBaseCluster().startMaster();
    // make sure that we can schedule a SCP for the crashed server which WAL is disabled and bring
    // the region online.
    try (Table table =
      UTIL.getConnection().getTableBuilder(TABLE_NAME, null).setOperationTimeout(30000).build()) {
      table.put(new Put(Bytes.toBytes(1)).addColumn(CF, CQ, Bytes.toBytes(1)));
      assertEquals(1, Bytes.toInt(table.get(new Get(Bytes.toBytes(1))).getValue(CF, CQ)));
    }
  }
}
