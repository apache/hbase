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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for region replicas. Sad that we cannot isolate these without bringing up a whole
 * cluster. See {@link org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster}.
 */
@Category(MediumTests.class)
public class TestReplicasClient {
  private static final Log LOG = LogFactory.getLog(TestReplicasClient.class);

  private static final int NB_SERVERS = 1;
  private static HTable table = null;
  private static final byte[] row = TestReplicasClient.class.getName().getBytes();

  private static HRegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  private final static int REFRESH_PERIOD = 1000;

  /**
   * This copro is used to synchronize the tests.
   */
  public static class SlowMeCopro extends BaseRegionObserver {
    static final AtomicLong sleepTime = new AtomicLong(0);
    static final AtomicReference<CountDownLatch> cdl =
        new AtomicReference<CountDownLatch>(new CountDownLatch(0));

    public SlowMeCopro() {
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
                         final Get get, final List<Cell> results) throws IOException {

      if (e.getEnvironment().getRegion().getRegionInfo().getReplicaId() == 0) {
        CountDownLatch latch = cdl.get();
        try {
          if (sleepTime.get() > 0) {
            LOG.info("Sleeping for " + sleepTime.get() + " ms");
            Thread.sleep(sleepTime.get());
          } else if (latch.getCount() > 0) {
            LOG.info("Waiting for the counterCountDownLatch");
            latch.await(2, TimeUnit.MINUTES); // To help the tests to finish.
            if (latch.getCount() > 0) {
              throw new RuntimeException("Can't wait more");
            }
          }
        } catch (InterruptedException e1) {
          LOG.error(e1);
        }
      } else {
        LOG.info("We're not the primary replicas.");
      }
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    // enable store file refreshing
    HTU.getConfiguration().setInt(
        StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, REFRESH_PERIOD);

    HTU.startMiniCluster(NB_SERVERS);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor(TestReplicasClient.class.getSimpleName());
    hdt.addCoprocessor(SlowMeCopro.class.getName());
    table = HTU.createTable(hdt, new byte[][]{f}, HTU.getConfiguration());

    hriPrimary = table.getRegionLocation(row, false).getRegionInfo();

    // mock a secondary region info to open
    hriSecondary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 1);

    // No master
    LOG.info("Master is going to be stopped");
    TestRegionServerNoMaster.stopMasterAndAssignMeta(HTU);
    Configuration c = new Configuration(HTU.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    HBaseAdmin ha = new HBaseAdmin(c);
    for (boolean masterRuns = true; masterRuns; ) {
      Thread.sleep(100);
      try {
        masterRuns = false;
        masterRuns = ha.isMasterRunning();
      } catch (MasterNotRunningException ignored) {
      }
    }
    LOG.info("Master has stopped");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (table != null) table.close();
    HTU.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    HTU.getHBaseAdmin().getConnection().clearRegionCache();
  }

  @After
  public void after() throws IOException, KeeperException {
    try {
      closeRegion(hriSecondary);
    } catch (Exception ignored) {
    }
    ZKAssign.deleteNodeFailSilent(HTU.getZooKeeperWatcher(), hriPrimary);
    ZKAssign.deleteNodeFailSilent(HTU.getZooKeeperWatcher(), hriSecondary);

    HTU.getHBaseAdmin().getConnection().clearRegionCache();
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private void openRegion(HRegionInfo hri) throws Exception {
    ZKAssign.createNodeOffline(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());
    // first version is '0'
    AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(
      getRS().getServerName(), hri, 0, null, null);
    AdminProtos.OpenRegionResponse responseOpen = getRS().getRSRpcServices().openRegion(null, orr);
    Assert.assertEquals(responseOpen.getOpeningStateCount(), 1);
    Assert.assertEquals(responseOpen.getOpeningState(0),
      AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED);
    checkRegionIsOpened(hri);
  }

  private void closeRegion(HRegionInfo hri) throws Exception {
    ZKAssign.createNodeClosing(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());

    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(
      getRS().getServerName(), hri.getEncodedName(), true);
    AdminProtos.CloseRegionResponse responseClose = getRS()
        .getRSRpcServices().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    checkRegionIsClosed(hri.getEncodedName());

    ZKAssign.deleteClosedNode(HTU.getZooKeeperWatcher(), hri.getEncodedName(), null);
  }

  private void checkRegionIsOpened(HRegionInfo hri) throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    Assert.assertTrue(
        ZKAssign.deleteOpenedNode(HTU.getZooKeeperWatcher(), hri.getEncodedName(), null));
  }

  private void checkRegionIsClosed(String encodedRegionName) throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    try {
      Assert.assertFalse(getRS().getRegionByEncodedName(encodedRegionName).isAvailable());
    } catch (NotServingRegionException expected) {
      // That's how it work: if the region is closed we have an exception.
    }

    // We don't delete the znode here, because there is not always a znode.
  }

  private void flushRegion(HRegionInfo regionInfo) throws IOException {
    for (RegionServerThread rst : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      HRegion region = rst.getRegionServer().getRegionByEncodedName(regionInfo.getEncodedName());
      if (region != null) {
        region.flushcache();
        return;
      }
    }
    throw new IOException("Region to flush cannot be found");
  }

  @Test
  public void testUseRegionWithoutReplica() throws Exception {
    byte[] b1 = "testUseRegionWithoutReplica".getBytes();
    openRegion(hriSecondary);
    SlowMeCopro.cdl.set(new CountDownLatch(0));
    try {
      Get g = new Get(b1);
      Result r = table.get(g);
      Assert.assertFalse(r.isStale());
    } finally {
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testLocations() throws Exception {
    byte[] b1 = "testLocations".getBytes();
    openRegion(hriSecondary);
    ClusterConnection hc = (ClusterConnection) HTU.getHBaseAdmin().getConnection();

    try {
      hc.clearRegionCache();
      RegionLocations rl = hc.locateRegion(table.getName(), b1, false, false);
      Assert.assertEquals(2, rl.size());

      rl = hc.locateRegion(table.getName(), b1, true, false);
      Assert.assertEquals(2, rl.size());

      hc.clearRegionCache();
      rl = hc.locateRegion(table.getName(), b1, true, false);
      Assert.assertEquals(2, rl.size());

      rl = hc.locateRegion(table.getName(), b1, false, false);
      Assert.assertEquals(2, rl.size());
    } finally {
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testGetNoResultNoStaleRegionWithReplica() throws Exception {
    byte[] b1 = "testGetNoResultNoStaleRegionWithReplica".getBytes();
    openRegion(hriSecondary);

    try {
      // A get works and is not stale
      Get g = new Get(b1);
      Result r = table.get(g);
      Assert.assertFalse(r.isStale());
    } finally {
      closeRegion(hriSecondary);
    }
  }


  @Test
  public void testGetNoResultStaleRegionWithReplica() throws Exception {
    byte[] b1 = "testGetNoResultStaleRegionWithReplica".getBytes();
    openRegion(hriSecondary);

    SlowMeCopro.cdl.set(new CountDownLatch(1));
    try {
      Get g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      Result r = table.get(g);
      Assert.assertTrue(r.isStale());
    } finally {
      SlowMeCopro.cdl.get().countDown();
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testGetNoResultNotStaleSleepRegionWithReplica() throws Exception {
    byte[] b1 = "testGetNoResultNotStaleSleepRegionWithReplica".getBytes();
    openRegion(hriSecondary);

    try {
      // We sleep; but we won't go to the stale region as we don't get the stale by default.
      SlowMeCopro.sleepTime.set(2000);
      Get g = new Get(b1);
      Result r = table.get(g);
      Assert.assertFalse(r.isStale());

    } finally {
      SlowMeCopro.sleepTime.set(0);
      closeRegion(hriSecondary);
    }
  }


  @Test
  public void testFlushTable() throws Exception {
    openRegion(hriSecondary);
    try {
      flushRegion(hriPrimary);
      flushRegion(hriSecondary);

      Put p = new Put(row);
      p.add(f, row, row);
      table.put(p);

      flushRegion(hriPrimary);
      flushRegion(hriSecondary);
    } finally {
      Delete d = new Delete(row);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testFlushPrimary() throws Exception {
    openRegion(hriSecondary);

    try {
      flushRegion(hriPrimary);

      Put p = new Put(row);
      p.add(f, row, row);
      table.put(p);

      flushRegion(hriPrimary);
    } finally {
      Delete d = new Delete(row);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testFlushSecondary() throws Exception {
    openRegion(hriSecondary);
    try {
      flushRegion(hriSecondary);

      Put p = new Put(row);
      p.add(f, row, row);
      table.put(p);

      flushRegion(hriSecondary);
    } catch (TableNotFoundException expected) {
    } finally {
      Delete d = new Delete(row);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testUseRegionWithReplica() throws Exception {
    byte[] b1 = "testUseRegionWithReplica".getBytes();
    openRegion(hriSecondary);

    try {
      // A simple put works, even if there here a second replica
      Put p = new Put(b1);
      p.add(f, b1, b1);
      table.put(p);
      LOG.info("Put done");

      // A get works and is not stale
      Get g = new Get(b1);
      Result r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertFalse(r.getColumnCells(f, b1).isEmpty());
      LOG.info("get works and is not stale done");

      // Even if it we have to wait a little on the main region
      SlowMeCopro.sleepTime.set(2000);
      g = new Get(b1);
      r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertFalse(r.getColumnCells(f, b1).isEmpty());
      SlowMeCopro.sleepTime.set(0);
      LOG.info("sleep and is not stale done");

      // But if we ask for stale we will get it
      SlowMeCopro.cdl.set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertTrue(r.getColumnCells(f, b1).isEmpty());
      SlowMeCopro.cdl.get().countDown();

      LOG.info("stale done");

      // exists works and is not stale
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertTrue(r.getExists());
      LOG.info("exists not stale done");

      // exists works on stale but don't see the put
      SlowMeCopro.cdl.set(new CountDownLatch(1));
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertFalse("The secondary has stale data", r.getExists());
      SlowMeCopro.cdl.get().countDown();
      LOG.info("exists stale before flush done");

      flushRegion(hriPrimary);
      flushRegion(hriSecondary);
      LOG.info("flush done");
      Thread.sleep(1000 + REFRESH_PERIOD * 2);

      // get works and is not stale
      SlowMeCopro.cdl.set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertFalse(r.isEmpty());
      SlowMeCopro.cdl.get().countDown();
      LOG.info("stale done");

      // exists works on stale and we see the put after the flush
      SlowMeCopro.cdl.set(new CountDownLatch(1));
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertTrue(r.getExists());
      SlowMeCopro.cdl.get().countDown();
      LOG.info("exists stale after flush done");

    } finally {
      SlowMeCopro.cdl.get().countDown();
      SlowMeCopro.sleepTime.set(0);
      Delete d = new Delete(b1);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }
}