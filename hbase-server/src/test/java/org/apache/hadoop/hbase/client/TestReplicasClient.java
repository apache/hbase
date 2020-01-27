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

import com.codahale.metrics.Counter;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

/**
 * Tests for region replicas. Sad that we cannot isolate these without bringing up a whole
 * cluster. See {@link org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster}.
 */
@Category({MediumTests.class, ClientTests.class})
@SuppressWarnings("deprecation")
public class TestReplicasClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicasClient.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicasClient.class);

  private static final int NB_SERVERS = 1;
  private static TableName TABLE_NAME;
  private Table table = null;
  private static final byte[] row = Bytes.toBytes(TestReplicasClient.class.getName());;

  private static RegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  private final static int REFRESH_PERIOD = 1000;

  /**
   * This copro is used to synchronize the tests.
   */
  public static class SlowMeCopro implements RegionCoprocessor, RegionObserver {
    static final AtomicLong sleepTime = new AtomicLong(0);
    static final AtomicBoolean slowDownNext = new AtomicBoolean(false);
    static final AtomicInteger countOfNext = new AtomicInteger(0);
    private static final AtomicReference<CountDownLatch> primaryCdl =
        new AtomicReference<>(new CountDownLatch(0));
    private static final AtomicReference<CountDownLatch> secondaryCdl =
        new AtomicReference<>(new CountDownLatch(0));
    public SlowMeCopro() {
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
                         final Get get, final List<Cell> results) throws IOException {
      slowdownCode(e);
    }

    @Override
    public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Scan scan) throws IOException {
      slowdownCode(e);
    }

    @Override
    public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
        final InternalScanner s, final List<Result> results,
        final int limit, final boolean hasMore) throws IOException {
      //this will slow down a certain next operation if the conditions are met. The slowness
      //will allow the call to go to a replica
      if (slowDownNext.get()) {
        //have some "next" return successfully from the primary; hence countOfNext checked
        if (countOfNext.incrementAndGet() == 2) {
          sleepTime.set(2000);
          slowdownCode(e);
        }
      }
      return true;
    }

    private void slowdownCode(final ObserverContext<RegionCoprocessorEnvironment> e) {
      if (e.getEnvironment().getRegion().getRegionInfo().getReplicaId() == 0) {
        LOG.info("We're the primary replicas.");
        CountDownLatch latch = getPrimaryCdl().get();
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
          LOG.error(e1.toString(), e1);
        }
      } else {
        LOG.info("We're not the primary replicas.");
        CountDownLatch latch = getSecondaryCdl().get();
        try {
          if (latch.getCount() > 0) {
            LOG.info("Waiting for the secondary counterCountDownLatch");
            latch.await(2, TimeUnit.MINUTES); // To help the tests to finish.
            if (latch.getCount() > 0) {
              throw new RuntimeException("Can't wait more");
            }
          }
        } catch (InterruptedException e1) {
          LOG.error(e1.toString(), e1);
        }
      }
    }

    public static AtomicReference<CountDownLatch> getPrimaryCdl() {
      return primaryCdl;
    }

    public static AtomicReference<CountDownLatch> getSecondaryCdl() {
      return secondaryCdl;
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    // enable store file refreshing
    HTU.getConfiguration().setInt(
        StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, REFRESH_PERIOD);
    HTU.getConfiguration().setBoolean("hbase.client.log.scanner.activity", true);
    HTU.getConfiguration().setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(1).
        numAlwaysStandByMasters(1).numMasters(1).build();
    HTU.startMiniCluster(option);

    // Create table then get the single region for our new table.
    HTableDescriptor hdt = HTU.createTableDescriptor(TestReplicasClient.class.getSimpleName());
    hdt.addCoprocessor(SlowMeCopro.class.getName());
    HTU.createTable(hdt, new byte[][]{f}, null);
    TABLE_NAME = hdt.getTableName();
    try (RegionLocator locator = HTU.getConnection().getRegionLocator(hdt.getTableName())) {
      hriPrimary = locator.getRegionLocation(row, false).getRegion();
    }

    // mock a secondary region info to open
    hriSecondary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 1);

    // No master
    LOG.info("Master is going to be stopped");
    TestRegionServerNoMaster.stopMasterAndAssignMeta(HTU);
    Configuration c = new Configuration(HTU.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    LOG.info("Master has stopped");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = false;
    HTU.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    HTU.getConnection().clearRegionLocationCache();
    try {
      openRegion(hriPrimary);
    } catch (Exception ignored) {
    }
    try {
      openRegion(hriSecondary);
    } catch (Exception ignored) {
    }
    table = HTU.getConnection().getTable(TABLE_NAME);
  }

  @After
  public void after() throws IOException, KeeperException {
    try {
      closeRegion(hriSecondary);
    } catch (Exception ignored) {
    }
    try {
      closeRegion(hriPrimary);
    } catch (Exception ignored) {
    }
    HTU.getConnection().clearRegionLocationCache();
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private void openRegion(RegionInfo hri) throws Exception {
    try {
      if (isRegionOpened(hri)) return;
    } catch (Exception e){}
    // first version is '0'
    AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(
      getRS().getServerName(), hri, null);
    AdminProtos.OpenRegionResponse responseOpen = getRS().getRSRpcServices().openRegion(null, orr);
    Assert.assertEquals(1, responseOpen.getOpeningStateCount());
    Assert.assertEquals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED,
        responseOpen.getOpeningState(0));
    checkRegionIsOpened(hri);
  }

  private void closeRegion(RegionInfo hri) throws Exception {
    AdminProtos.CloseRegionRequest crr = ProtobufUtil.buildCloseRegionRequest(
      getRS().getServerName(), hri.getRegionName());
    AdminProtos.CloseRegionResponse responseClose = getRS()
        .getRSRpcServices().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    checkRegionIsClosed(hri.getEncodedName());
  }

  private void checkRegionIsOpened(RegionInfo hri) throws Exception {
    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }
  }

  private boolean isRegionOpened(RegionInfo hri) throws Exception {
    return getRS().getRegionByEncodedName(hri.getEncodedName()).isAvailable();
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

  private void flushRegion(RegionInfo regionInfo) throws IOException {
    TestRegionServerNoMaster.flushRegion(HTU, regionInfo);
  }

  @Test
  public void testUseRegionWithoutReplica() throws Exception {
    byte[] b1 = Bytes.toBytes("testUseRegionWithoutReplica");
    openRegion(hriSecondary);
    SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(0));
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
    byte[] b1 = Bytes.toBytes("testLocations");
    openRegion(hriSecondary);

    try (Connection conn = ConnectionFactory.createConnection(HTU.getConfiguration());
        RegionLocator locator = conn.getRegionLocator(TABLE_NAME)) {
      conn.clearRegionLocationCache();
      List<HRegionLocation> rl = locator.getRegionLocations(b1, true);
      Assert.assertEquals(2, rl.size());

      rl = locator.getRegionLocations(b1, false);
      Assert.assertEquals(2, rl.size());

      conn.clearRegionLocationCache();
      rl = locator.getRegionLocations(b1, false);
      Assert.assertEquals(2, rl.size());

      rl = locator.getRegionLocations(b1, true);
      Assert.assertEquals(2, rl.size());
    } finally {
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testGetNoResultNoStaleRegionWithReplica() throws Exception {
    byte[] b1 = Bytes.toBytes("testGetNoResultNoStaleRegionWithReplica");
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
    byte[] b1 = Bytes.toBytes("testGetNoResultStaleRegionWithReplica");
    openRegion(hriSecondary);

    SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
    try {
      Get g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      Result r = table.get(g);
      Assert.assertTrue(r.isStale());
    } finally {
      SlowMeCopro.getPrimaryCdl().get().countDown();
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testGetNoResultNotStaleSleepRegionWithReplica() throws Exception {
    byte[] b1 = Bytes.toBytes("testGetNoResultNotStaleSleepRegionWithReplica");
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
      p.addColumn(f, row, row);
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
      p.addColumn(f, row, row);
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
      p.addColumn(f, row, row);
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
    byte[] b1 = Bytes.toBytes("testUseRegionWithReplica");
    openRegion(hriSecondary);

    try {
      // A simple put works, even if there here a second replica
      Put p = new Put(b1);
      p.addColumn(f, b1, b1);
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
      SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertTrue(r.getColumnCells(f, b1).isEmpty());
      SlowMeCopro.getPrimaryCdl().get().countDown();

      LOG.info("stale done");

      // exists works and is not stale
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertTrue(r.getExists());
      LOG.info("exists not stale done");

      // exists works on stale but don't see the put
      SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertFalse("The secondary has stale data", r.getExists());
      SlowMeCopro.getPrimaryCdl().get().countDown();
      LOG.info("exists stale before flush done");

      flushRegion(hriPrimary);
      flushRegion(hriSecondary);
      LOG.info("flush done");
      Thread.sleep(1000 + REFRESH_PERIOD * 2);

      // get works and is not stale
      SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertFalse(r.isEmpty());
      SlowMeCopro.getPrimaryCdl().get().countDown();
      LOG.info("stale done");

      // exists works on stale and we see the put after the flush
      SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
      g = new Get(b1);
      g.setCheckExistenceOnly(true);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertTrue(r.getExists());
      SlowMeCopro.getPrimaryCdl().get().countDown();
      LOG.info("exists stale after flush done");

    } finally {
      SlowMeCopro.getPrimaryCdl().get().countDown();
      SlowMeCopro.sleepTime.set(0);
      Delete d = new Delete(b1);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }

  @Test
  public void testHedgedRead() throws Exception {
    byte[] b1 = Bytes.toBytes("testHedgedRead");
    openRegion(hriSecondary);

    try {
      // A simple put works, even if there here a second replica
      Put p = new Put(b1);
      p.addColumn(f, b1, b1);
      table.put(p);
      LOG.info("Put done");

      // A get works and is not stale
      Get g = new Get(b1);
      Result r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertFalse(r.getColumnCells(f, b1).isEmpty());
      LOG.info("get works and is not stale done");

      //reset
      AsyncConnectionImpl conn = (AsyncConnectionImpl) HTU.getConnection().toAsyncConnection();
      Counter hedgedReadOps = conn.getConnectionMetrics().get().hedgedReadOps;
      Counter hedgedReadWin = conn.getConnectionMetrics().get().hedgedReadWin;
      hedgedReadOps.dec(hedgedReadOps.getCount());
      hedgedReadWin.dec(hedgedReadWin.getCount());

      // Wait a little on the main region, just enough to happen once hedged read
      // and hedged read did not returned faster
      long primaryCallTimeoutNs = conn.connConf.getPrimaryCallTimeoutNs();
      // The resolution of our timer is 10ms, so we need to sleep a bit more otherwise we may not
      // trigger the hedged read...
      SlowMeCopro.sleepTime.set(TimeUnit.NANOSECONDS.toMillis(primaryCallTimeoutNs) + 100);
      SlowMeCopro.getSecondaryCdl().set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertFalse(r.isStale());
      Assert.assertFalse(r.getColumnCells(f, b1).isEmpty());
      Assert.assertEquals(1, hedgedReadOps.getCount());
      Assert.assertEquals(0, hedgedReadWin.getCount());
      SlowMeCopro.sleepTime.set(0);
      SlowMeCopro.getSecondaryCdl().get().countDown();
      LOG.info("hedged read occurred but not faster");


      // But if we ask for stale we will get it and hedged read returned faster
      SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      r = table.get(g);
      Assert.assertTrue(r.isStale());
      Assert.assertTrue(r.getColumnCells(f, b1).isEmpty());
      Assert.assertEquals(2, hedgedReadOps.getCount());
      // we update the metrics after we finish the request so we use a waitFor here, use assert
      // directly may cause failure if we run too fast.
      HTU.waitFor(10000, () -> hedgedReadWin.getCount() == 1);
      SlowMeCopro.getPrimaryCdl().get().countDown();
      LOG.info("hedged read occurred and faster");

    } finally {
      SlowMeCopro.getPrimaryCdl().get().countDown();
      SlowMeCopro.getSecondaryCdl().get().countDown();
      SlowMeCopro.sleepTime.set(0);
      Delete d = new Delete(b1);
      table.delete(d);
      closeRegion(hriSecondary);
    }
  }
}
