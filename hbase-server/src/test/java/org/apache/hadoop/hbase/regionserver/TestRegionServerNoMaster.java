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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mortbay.log.Log;

import com.google.protobuf.ServiceException;

/**
 * Tests on the region server, without the master.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestRegionServerNoMaster {

  private static final int NB_SERVERS = 1;
  private static HTable table;
  private static final byte[] row = "ee".getBytes();

  private static HRegionInfo hri;

  private static byte[] regionName;
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();


  @BeforeClass
  public static void before() throws Exception {
    HTU.startMiniCluster(NB_SERVERS);
    final TableName tableName = TableName.valueOf(TestRegionServerNoMaster.class.getSimpleName());

    // Create table then get the single region for our new table.
    table = HTU.createTable(tableName,HConstants.CATALOG_FAMILY);
    Put p = new Put(row);
    p.add(HConstants.CATALOG_FAMILY, row, row);
    table.put(p);

    hri = table.getRegionLocation(row, false).getRegionInfo();
    regionName = hri.getRegionName();

    stopMasterAndAssignMeta(HTU);
  }

  public static void stopMasterAndAssignMeta(HBaseTestingUtility HTU)
      throws IOException, InterruptedException {
    // Stop master
    HMaster master = HTU.getHBaseCluster().getMaster();
    ServerName masterAddr = master.getServerName();
    master.stopMaster();

    Log.info("Waiting until master thread exits");
    while (HTU.getHBaseCluster().getMasterThread() != null
        && HTU.getHBaseCluster().getMasterThread().isAlive()) {
      Threads.sleep(100);
    }

    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = true;
    // Master is down, so is the meta. We need to assign it somewhere
    // so that regions can be assigned during the mocking phase.
    HRegionServer hrs = HTU.getHBaseCluster()
      .getLiveRegionServerThreads().get(0).getRegionServer();
    ZooKeeperWatcher zkw = hrs.getZooKeeper();
    MetaTableLocator mtl = new MetaTableLocator();
    ServerName sn = mtl.getMetaRegionLocation(zkw);
    if (sn != null && !masterAddr.equals(sn)) {
      return;
    }

    ProtobufUtil.openRegion(hrs.getRSRpcServices(),
      hrs.getServerName(), HRegionInfo.FIRST_META_REGIONINFO);
    while (true) {
      sn = mtl.getMetaRegionLocation(zkw);
      if (sn != null && sn.equals(hrs.getServerName())
          && hrs.onlineRegions.containsKey(
              HRegionInfo.FIRST_META_REGIONINFO.getEncodedName())) {
        break;
      }
      Thread.sleep(100);
    }
  }

  /** Flush the given region in the mini cluster. Since no master, we cannot use HBaseAdmin.flush() */
  public static void flushRegion(HBaseTestingUtility HTU, HRegionInfo regionInfo) throws IOException {
    for (RegionServerThread rst : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      Region region = rst.getRegionServer().getRegionByEncodedName(regionInfo.getEncodedName());
      if (region != null) {
        region.flush(true);
        return;
      }
    }
    throw new IOException("Region to flush cannot be found");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = false;
    table.close();
    HTU.shutdownMiniCluster();
  }

  private static HRegionServer getRS() {
    return HTU.getHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer();
  }


  public static void openRegion(HBaseTestingUtility HTU, HRegionServer rs, HRegionInfo hri)
      throws Exception {
    AdminProtos.OpenRegionRequest orr =
      RequestConverter.buildOpenRegionRequest(rs.getServerName(), hri, null, null);
    AdminProtos.OpenRegionResponse responseOpen = rs.rpcServices.openRegion(null, orr);

    Assert.assertTrue(responseOpen.getOpeningStateCount() == 1);
    Assert.assertTrue(responseOpen.getOpeningState(0).
        equals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED));


    checkRegionIsOpened(HTU, rs, hri);
  }

  public static void checkRegionIsOpened(HBaseTestingUtility HTU, HRegionServer rs,
      HRegionInfo hri) throws Exception {
    while (!rs.getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    Assert.assertTrue(rs.getRegion(hri.getRegionName()).isAvailable());
  }

  public static void closeRegion(HBaseTestingUtility HTU, HRegionServer rs, HRegionInfo hri)
      throws Exception {
    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(
      rs.getServerName(), hri.getEncodedName());
    AdminProtos.CloseRegionResponse responseClose = rs.rpcServices.closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());
    checkRegionIsClosed(HTU, rs, hri);
  }

  public static void checkRegionIsClosed(HBaseTestingUtility HTU, HRegionServer rs,
      HRegionInfo hri) throws Exception {
    while (!rs.getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    try {
      Assert.assertFalse(rs.getRegion(hri.getRegionName()).isAvailable());
    } catch (NotServingRegionException expected) {
      // That's how it work: if the region is closed we have an exception.
    }
  }

  /**
   * Close the region without using ZK
   */
  private void closeRegionNoZK() throws Exception {
    // no transition in ZK
    AdminProtos.CloseRegionRequest crr =
        RequestConverter.buildCloseRegionRequest(getRS().getServerName(), regionName);
    AdminProtos.CloseRegionResponse responseClose = getRS().rpcServices.closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    // now waiting & checking. After a while, the transition should be done and the region closed
    checkRegionIsClosed(HTU, getRS(), hri);
  }


  @Test(timeout = 60000)
  public void testCloseByRegionServer() throws Exception {
    closeRegionNoZK();
    openRegion(HTU, getRS(), hri);
  }

  @Test(timeout = 60000)
  public void testMultipleCloseFromMaster() throws Exception {
    for (int i = 0; i < 10; i++) {
      AdminProtos.CloseRegionRequest crr =
          RequestConverter.buildCloseRegionRequest(getRS().getServerName(), regionName, null);
      try {
        AdminProtos.CloseRegionResponse responseClose = getRS().rpcServices.closeRegion(null, crr);
        Assert.assertTrue("request " + i + " failed",
            responseClose.getClosed() || responseClose.hasClosed());
      } catch (ServiceException se) {
        Assert.assertTrue("The next queries may throw an exception.", i > 0);
      }
    }

    checkRegionIsClosed(HTU, getRS(), hri);

    openRegion(HTU, getRS(), hri);
  }

  /**
   * Test that if we do a close while opening it stops the opening.
   */
  @Test(timeout = 60000)
  public void testCancelOpeningWithoutZK() throws Exception {
    // We close
    closeRegionNoZK();
    checkRegionIsClosed(HTU, getRS(), hri);

    // Let do the initial steps, without having a handler
    getRS().getRegionsInTransitionInRS().put(hri.getEncodedNameAsBytes(), Boolean.TRUE);

    // That's a close without ZK.
    AdminProtos.CloseRegionRequest crr =
        RequestConverter.buildCloseRegionRequest(getRS().getServerName(), regionName);
    try {
      getRS().rpcServices.closeRegion(null, crr);
      Assert.assertTrue(false);
    } catch (ServiceException expected) {
    }

    // The state in RIT should have changed to close
    Assert.assertEquals(Boolean.FALSE, getRS().getRegionsInTransitionInRS().get(
        hri.getEncodedNameAsBytes()));

    // Let's start the open handler
    HTableDescriptor htd = getRS().tableDescriptors.get(hri.getTable());

    getRS().service.submit(new OpenRegionHandler(getRS(), getRS(), hri, htd, -1));

    // The open handler should have removed the region from RIT but kept the region closed
    checkRegionIsClosed(HTU, getRS(), hri);

    openRegion(HTU, getRS(), hri);
  }

  /**
   * Tests an on-the-fly RPC that was scheduled for the earlier RS on the same port
   * for openRegion. The region server should reject this RPC. (HBASE-9721)
   */
  @Test
  public void testOpenCloseRegionRPCIntendedForPreviousServer() throws Exception {
    Assert.assertTrue(getRS().getRegion(regionName).isAvailable());

    ServerName sn = getRS().getServerName();
    ServerName earlierServerName = ServerName.valueOf(sn.getHostname(), sn.getPort(), 1);

    try {
      CloseRegionRequest request = RequestConverter.buildCloseRegionRequest(earlierServerName, regionName);
      getRS().getRSRpcServices().closeRegion(null, request);
      Assert.fail("The closeRegion should have been rejected");
    } catch (ServiceException se) {
      Assert.assertTrue(se.getCause() instanceof IOException);
      Assert.assertTrue(se.getCause().getMessage().contains("This RPC was intended for a different server"));
    }

    //actual close
    closeRegionNoZK();
    try {
      AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(
        earlierServerName, hri, null, null);
      getRS().getRSRpcServices().openRegion(null, orr);
      Assert.fail("The openRegion should have been rejected");
    } catch (ServiceException se) {
      Assert.assertTrue(se.getCause() instanceof IOException);
      Assert.assertTrue(se.getCause().getMessage().contains("This RPC was intended for a different server"));
    } finally {
      openRegion(HTU, getRS(), hri);
    }
  }
}
