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
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.log4j.Level;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for region replicas. Sad that we cannot isolate these without bringing up a whole
 * cluster. See {@link org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster}.
 */
@Category(MediumTests.class)
public class TestReplicasClient {
  private static final Log LOG = LogFactory.getLog(TestReplicasClient.class);

  static {
    ((Log4JLogger)RpcRetryingCaller.LOG).getLogger().setLevel(Level.ALL);
  }

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
    static final AtomicBoolean slowDownNext = new AtomicBoolean(false);
    static final AtomicInteger countOfNext = new AtomicInteger(0);
    static final AtomicReference<CountDownLatch> cdl =
        new AtomicReference<CountDownLatch>(new CountDownLatch(0));
    Random r = new Random();
    public SlowMeCopro() {
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
                         final Get get, final List<Cell> results) throws IOException {
      slowdownCode(e);
    }

    @Override
    public RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Scan scan, final RegionScanner s) throws IOException {
      slowdownCode(e);
      return s;
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
    HTU.getConfiguration().setBoolean("hbase.client.log.scanner.activity", true);
    ConnectionUtils.setupMasterlessConnection(HTU.getConfiguration());
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
    try {
      openRegion(hriPrimary);
    } catch (Exception ignored) {
    }
    try {
      openRegion(hriSecondary);
    } catch (Exception ignored) {
    }
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
    ZKAssign.deleteNodeFailSilent(HTU.getZooKeeperWatcher(), hriPrimary);
    ZKAssign.deleteNodeFailSilent(HTU.getZooKeeperWatcher(), hriSecondary);

    HTU.getHBaseAdmin().getConnection().clearRegionCache();
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private void openRegion(HRegionInfo hri) throws Exception {
    try {
      if (isRegionOpened(hri)) return;
    } catch (Exception e){}
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

  private boolean isRegionOpened(HRegionInfo hri) throws Exception {
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

  private void flushRegion(HRegionInfo regionInfo) throws IOException {
    TestRegionServerNoMaster.flushRegion(HTU, regionInfo);
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

  @Test
  public void testScanWithReplicas() throws Exception {
    //simple scan
    runMultipleScansOfOneType(false, false);
  }

  @Test
  public void testSmallScanWithReplicas() throws Exception {
    //small scan
    runMultipleScansOfOneType(false, true);
  }

  @Test
  public void testReverseScanWithReplicas() throws Exception {
    //reverse scan
    runMultipleScansOfOneType(true, false);
  }

  private void runMultipleScansOfOneType(boolean reversed, boolean small) throws Exception {
    openRegion(hriSecondary);
    int NUMROWS = 100;
    try {
      for (int i = 0; i < NUMROWS; i++) {
        byte[] b1 = Bytes.toBytes("testUseRegionWithReplica" + i);
        Put p = new Put(b1);
        p.add(f, b1, b1);
        table.put(p);
      }
      LOG.debug("PUT done");
      int caching = 20;
      byte[] start;
      if (reversed) start = Bytes.toBytes("testUseRegionWithReplica" + (NUMROWS - 1));
      else start = Bytes.toBytes("testUseRegionWithReplica" + 0);

      scanWithReplicas(reversed, small, Consistency.TIMELINE, caching, start, NUMROWS, false, false);

      //Even if we were to slow the server down, unless we ask for stale
      //we won't get it
      SlowMeCopro.sleepTime.set(5000);
      scanWithReplicas(reversed, small, Consistency.STRONG, caching, start, NUMROWS, false, false);
      SlowMeCopro.sleepTime.set(0);

      flushRegion(hriPrimary);
      LOG.info("flush done");
      Thread.sleep(1000 + REFRESH_PERIOD * 2);

      //Now set the flag to get a response even if stale
      SlowMeCopro.sleepTime.set(5000);
      scanWithReplicas(reversed, small, Consistency.TIMELINE, caching, start, NUMROWS, true, false);
      SlowMeCopro.sleepTime.set(0);

      // now make some 'next' calls slow
      SlowMeCopro.slowDownNext.set(true);
      SlowMeCopro.countOfNext.set(0);
      scanWithReplicas(reversed, small, Consistency.TIMELINE, caching, start, NUMROWS, true, true);
      SlowMeCopro.slowDownNext.set(false);
      SlowMeCopro.countOfNext.set(0);
    } finally {
      SlowMeCopro.cdl.get().countDown();
      SlowMeCopro.sleepTime.set(0);
      SlowMeCopro.slowDownNext.set(false);
      SlowMeCopro.countOfNext.set(0);
      for (int i = 0; i < NUMROWS; i++) {
        byte[] b1 = Bytes.toBytes("testUseRegionWithReplica" + i);
        Delete d = new Delete(b1);
        table.delete(d);
      }
      closeRegion(hriSecondary);
    }
  }

  private void scanWithReplicas(boolean reversed, boolean small, Consistency consistency,
      int caching, byte[] startRow, int numRows, boolean staleExpected, boolean slowNext)
          throws Exception {
    Scan scan = new Scan(startRow);
    scan.setCaching(caching);
    scan.setReversed(reversed);
    scan.setSmall(small);
    scan.setConsistency(consistency);
    ResultScanner scanner = table.getScanner(scan);
    Iterator<Result> iter = scanner.iterator();
    HashMap<String, Boolean> map = new HashMap<String, Boolean>();
    int count = 0;
    int countOfStale = 0;
    while (iter.hasNext()) {
      count++;
      Result r = iter.next();
      if (map.containsKey(new String(r.getRow()))) {
        throw new Exception("Unexpected scan result. Repeated row " + Bytes.toString(r.getRow()));
      }
      map.put(new String(r.getRow()), true);
      if (!slowNext) Assert.assertTrue(r.isStale() == staleExpected);
      if (r.isStale()) countOfStale++;
    }
    LOG.debug("Count of rows " + count + " num rows expected " + numRows);
    Assert.assertTrue(count == numRows);
    if (slowNext) {
      LOG.debug("Count of Stale " + countOfStale);
      Assert.assertTrue(countOfStale > 1 && countOfStale < numRows);
    }
  }
}
