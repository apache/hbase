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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Class to test asynchronous region admin operations.
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncRegionAdminApi extends TestAsyncAdminBase {

  private void createTableWithDefaultConf(TableName TABLENAME) throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    admin.createTable(htd, null).get();
  }

  @Test
  public void testCloseRegion() throws Exception {
    TableName TABLENAME = TableName.valueOf("TestHBACloseRegion");
    createTableWithDefaultConf(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.getTable().isSystemTable()) {
        info = regionInfo;
        boolean closed = admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(),
          rs.getServerName().getServerName()).get();
        assertTrue(closed);
      }
    }
    boolean isInList = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()).contains(info);
    long timeout = System.currentTimeMillis() + 10000;
    while ((System.currentTimeMillis() < timeout) && (isInList)) {
      Thread.sleep(100);
      isInList = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()).contains(info);
    }

    assertFalse("The region should not be present in online regions list.", isInList);
  }

  @Test
  public void testCloseRegionIfInvalidRegionNameIsPassed() throws Exception {
    final String name = "TestHBACloseRegion1";
    byte[] TABLENAME = Bytes.toBytes(name);
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {
        if (regionInfo.getRegionNameAsString().contains(name)) {
          info = regionInfo;
          boolean catchNotServingException = false;
          try {
            admin.closeRegionWithEncodedRegionName("sample", rs.getServerName().getServerName())
                .get();
          } catch (Exception e) {
            catchNotServingException = true;
            // expected, ignore it
          }
          assertTrue(catchNotServingException);
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
      onlineRegions.contains(info));
  }

  @Test
  public void testCloseRegionWhenServerNameIsNull() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion3");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    try {
      List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaTable()) {
          if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion3")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(), null).get();
          }
        }
      }
      fail("The test should throw exception if the servername passed is null.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCloseRegionWhenServerNameIsEmpty() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegionWhenServerNameIsEmpty");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    try {
      List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaTable()) {
          if (regionInfo.getRegionNameAsString()
              .contains("TestHBACloseRegionWhenServerNameIsEmpty")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(), " ").get();
          }
        }
      }
      fail("The test should throw exception if the servername passed is empty.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCloseRegionWhenEncodedRegionNameIsNotGiven() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion4");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {
        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion4")) {
          info = regionInfo;
          boolean catchNotServingException = false;
          try {
            admin.closeRegionWithEncodedRegionName(regionInfo.getRegionNameAsString(),
              rs.getServerName().getServerName()).get();
          } catch (Exception e) {
            // expected, ignore it.
            catchNotServingException = true;
          }
          assertTrue(catchNotServingException);
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
      onlineRegions.contains(info));
  }

  @Test
  public void testGetRegion() throws Exception {
    AsyncHBaseAdmin rawAdmin = (AsyncHBaseAdmin) admin;

    final TableName tableName = TableName.valueOf("testGetRegion");
    LOG.info("Started " + tableName);
    TEST_UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY);

    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      HRegionLocation regionLocation = locator.getRegionLocation(Bytes.toBytes("mmm"));
      HRegionInfo region = regionLocation.getRegionInfo();
      byte[] regionName = region.getRegionName();
      Pair<HRegionInfo, ServerName> pair = rawAdmin.getRegion(regionName).get();
      assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
      pair = rawAdmin.getRegion(region.getEncodedNameAsBytes()).get();
      assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
    }
  }

  @Test
  public void testMergeRegions() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeRegions");
    HColumnDescriptor cd = new HColumnDescriptor("d");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(cd);
    byte[][] splitRows = new byte[][] { Bytes.toBytes("3"), Bytes.toBytes("6") };
    Admin syncAdmin = TEST_UTIL.getAdmin();
    try {
      TEST_UTIL.createTable(td, splitRows);
      TEST_UTIL.waitTableAvailable(tableName);

      List<HRegionInfo> tableRegions;
      HRegionInfo regionA;
      HRegionInfo regionB;

      // merge with full name
      tableRegions = syncAdmin.getTableRegions(tableName);
      assertEquals(3, syncAdmin.getTableRegions(tableName).size());
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

      assertEquals(2, syncAdmin.getTableRegions(tableName).size());

      // merge with encoded name
      tableRegions = syncAdmin.getTableRegions(tableName);
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

      assertEquals(1, syncAdmin.getTableRegions(tableName).size());
    } finally {
      syncAdmin.disableTable(tableName);
      syncAdmin.deleteTable(tableName);
    }
  }

  @Test
  public void testSplitTable() throws Exception {
    splitTests(TableName.valueOf("testSplitTable"), 3000, false, null);
    splitTests(TableName.valueOf("testSplitTableWithSplitPoint"), 3000, false, Bytes.toBytes("3"));
    splitTests(TableName.valueOf("testSplitRegion"), 3000, true, null);
    splitTests(TableName.valueOf("testSplitRegionWithSplitPoint"), 3000, true, Bytes.toBytes("3"));
  }

  private void splitTests(TableName tableName, int rowCount, boolean isSplitRegion,
      byte[] splitPoint) throws Exception {
    int count = 0;
    // create table
    HColumnDescriptor cd = new HColumnDescriptor("d");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(cd);
    Table table = TEST_UTIL.createTable(td, null);
    TEST_UTIL.waitTableAvailable(tableName);

    List<HRegionInfo> regions = TEST_UTIL.getAdmin().getTableRegions(tableName);
    assertEquals(regions.size(), 1);

    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(Bytes.toBytes("d"), null, Bytes.toBytes("value" + i));
      puts.add(put);
    }
    table.put(puts);

    if (isSplitRegion) {
      admin.splitRegion(regions.get(0).getRegionName(), splitPoint).get();
    } else {
      if (splitPoint == null) {
        admin.split(tableName).get();
      } else {
        admin.split(tableName, splitPoint).get();
      }
    }

    for (int i = 0; i < 45; i++) {
      try {
        List<HRegionInfo> hRegionInfos = TEST_UTIL.getAdmin().getTableRegions(tableName);
        count = hRegionInfos.size();
        if (count >= 2) {
          break;
        }
        Thread.sleep(1000L);
      } catch (Exception e) {
        LOG.error(e);
      }
    }

    assertEquals(count, 2);
  }

  @Test
  public void testAssignRegionAndUnassignRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignRegionAndUnassignRegion");
    try {
      // create test table
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc).get();

      // add region to meta.
      Table meta = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
      HRegionInfo hri =
          new HRegionInfo(desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      // assign region.
      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      AssignmentManager am = master.getAssignmentManager();
      admin.assign(hri.getRegionName()).get();
      am.waitForAssignment(hri);

      // assert region on server
      RegionStates regionStates = am.getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
      assertTrue(regionStates.getRegionState(hri).isOpened());

      // Region is assigned now. Let's assign it again.
      // Master should not abort, and region should be assigned.
      admin.assign(hri.getRegionName()).get();
      am.waitForAssignment(hri);
      assertTrue(regionStates.getRegionState(hri).isOpened());

      // unassign region
      admin.unassign(hri.getRegionName(), true).get();
      am.waitForAssignment(hri);
      assertTrue(regionStates.getRegionState(hri).isOpened());
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  HRegionInfo createTableAndGetOneRegion(final TableName tableName)
      throws IOException, InterruptedException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 5);

    // wait till the table is assigned
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    long timeoutTime = System.currentTimeMillis() + 3000;
    while (true) {
      List<HRegionInfo> regions =
          master.getAssignmentManager().getRegionStates().getRegionsOfTable(tableName);
      if (regions.size() > 3) {
        return regions.get(2);
      }
      long now = System.currentTimeMillis();
      if (now > timeoutTime) {
        fail("Could not find an online region");
      }
      Thread.sleep(10);
    }
  }

  @Test
  public void testOfflineRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testOfflineRegion");
    try {
      HRegionInfo hri = createTableAndGetOneRegion(tableName);

      RegionStates regionStates =
          TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
      admin.offline(hri.getRegionName()).get();

      long timeoutTime = System.currentTimeMillis() + 3000;
      while (true) {
        if (regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OFFLINE)
            .contains(hri))
          break;
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to offline the region in time");
          break;
        }
        Thread.sleep(10);
      }
      RegionState regionState = regionStates.getRegionState(hri);
      assertTrue(regionState.isOffline());
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testMoveRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testMoveRegion");
    try {
      HRegionInfo hri = createTableAndGetOneRegion(tableName);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      ServerManager serverManager = master.getServerManager();
      ServerName destServerName = null;
      List<JVMClusterUtil.RegionServerThread> regionServers =
          TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads();
      for (JVMClusterUtil.RegionServerThread regionServer : regionServers) {
        HRegionServer destServer = regionServer.getRegionServer();
        destServerName = destServer.getServerName();
        if (!destServerName.equals(serverName) && serverManager.isServerOnline(destServerName)) {
          break;
        }
      }
      assertTrue(destServerName != null && !destServerName.equals(serverName));
      admin.move(hri.getEncodedNameAsBytes(), Bytes.toBytes(destServerName.getServerName())).get();

      long timeoutTime = System.currentTimeMillis() + 30000;
      while (true) {
        ServerName sn = regionStates.getRegionServerOfRegion(hri);
        if (sn != null && sn.equals(destServerName)) {
          TEST_UTIL.assertRegionOnServer(hri, sn, 200);
          break;
        }
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to move the region in time: " + regionStates.getRegionState(hri));
        }
        regionStates.waitForUpdate(50);
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }
}
