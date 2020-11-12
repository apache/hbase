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

import com.codahale.metrics.Counter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
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
@Category({LargeTests.class, ClientTests.class})
public class TestReplicasClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicasClient.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicasClient.class);

  private static TableName TABLE_NAME;
  private Table table = null;
  private static final byte[] row = TestReplicasClient.class.getName().getBytes();

  private static RegionInfo hriPrimary;
  private static RegionInfo hriSecondary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  private final static int REFRESH_PERIOD = 1000;

  /**
   * This copro is used to synchronize the tests.
   */
  public static class SlowMeCopro implements RegionCoprocessor, RegionObserver {
    static final AtomicInteger primaryCountOfScan = new AtomicInteger(0);
    static final AtomicInteger secondaryCountOfScan = new AtomicInteger(0);
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
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e, final Get get,
      final List<Cell> results) throws IOException {
      slowdownCode(e);
    }

    private void incrementScanCount(ObserverContext<RegionCoprocessorEnvironment> e) {
      LOG.info("==========scan {} ", e.getEnvironment().getRegion().getRegionInfo().getReplicaId(),
        new Exception());
      if (e.getEnvironment().getRegion().getRegionInfo().getReplicaId() == 0) {
        primaryCountOfScan.incrementAndGet();
      } else {
        secondaryCountOfScan.incrementAndGet();
      }
    }

    @Override
    public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Scan scan) throws IOException {
      incrementScanCount(e);
      slowdownCode(e);
    }

    @Override
    public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final List<Result> results, final int limit, final boolean hasMore)
      throws IOException {
      incrementScanCount(e);
      // this will slow down a certain next operation if the conditions are met. The slowness
      // will allow the call to go to a replica
      if (slowDownNext.get()) {
        // have some "next" return successfully from the primary; hence countOfNext checked
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
    ConnectionUtils.setupMasterlessConnection(HTU.getConfiguration());
    StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(1).
        numAlwaysStandByMasters(1).numMasters(1).build();
    HTU.startMiniCluster(option);

    // Create table then get the single region for our new table.
    TABLE_NAME = TableName.valueOf(TestReplicasClient.class.getSimpleName());
    HTableDescriptor hdt = HTU.createTableDescriptor(TABLE_NAME);
    hdt.addCoprocessor(SlowMeCopro.class.getName());
    HTU.createTable(hdt, new byte[][]{f}, null);

    try (RegionLocator locator = HTU.getConnection().getRegionLocator(TABLE_NAME)) {
      hriPrimary = locator.getRegionLocation(row, false).getRegion();
    }

    // mock a secondary region info to open
    hriSecondary =  RegionReplicaUtil.getRegionInfoForReplica(hriPrimary, 1);

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
    try {
      openRegion(hriPrimary);
    } catch (Exception ignored) {
    }
    try {
      openRegion(hriSecondary);
    } catch (Exception ignored) {
    }
    SlowMeCopro.slowDownNext.set(false);
    SlowMeCopro.sleepTime.set(0);
    SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(0));
    SlowMeCopro.getSecondaryCdl().set(new CountDownLatch(0));
    table = HTU.getConnection().getTable(TABLE_NAME);
    try (ResultScanner scanner = table.getScanner(new Scan())) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        table.delete(new Delete(result.getRow()));
      }
    }
    flushRegion(hriPrimary);
    HTU.getConnection().clearRegionLocationCache();
    SlowMeCopro.primaryCountOfScan.set(0);
    SlowMeCopro.secondaryCountOfScan.set(0);
    SlowMeCopro.countOfNext.set(0);
  }

  @After
  public void after() throws IOException, KeeperException {
    SlowMeCopro.getPrimaryCdl().get().countDown();
    SlowMeCopro.getSecondaryCdl().get().countDown();
    try {
      closeRegion(hriSecondary);
    } catch (Exception ignored) {
    }
    try {
      closeRegion(hriPrimary);
    } catch (Exception ignored) {
    }
    if (table != null) {
      table.close();
    }
    HTU.getConnection().clearRegionLocationCache();
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private void openRegion(RegionInfo hri) throws Exception {
    try {
      if (isRegionOpened(hri)) {
        return;
      }
    } catch (Exception e) {
    }
    // first version is '0'
    AdminProtos.OpenRegionRequest orr =
      RequestConverter.buildOpenRegionRequest(getRS().getServerName(), hri, null);
    AdminProtos.OpenRegionResponse responseOpen = getRS().getRSRpcServices().openRegion(null, orr);
    assertEquals(1, responseOpen.getOpeningStateCount());
    assertEquals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED,
      responseOpen.getOpeningState(0));
    checkRegionIsOpened(hri);
  }

  private void closeRegion(RegionInfo hri) throws Exception {
    AdminProtos.CloseRegionRequest crr = ProtobufUtil.buildCloseRegionRequest(
      getRS().getServerName(), hri.getRegionName());
    AdminProtos.CloseRegionResponse responseClose = getRS()
        .getRSRpcServices().closeRegion(null, crr);
    assertTrue(responseClose.getClosed());

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
      assertFalse(getRS().getRegionByEncodedName(encodedRegionName).isAvailable());
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
    byte[] b1 = "testUseRegionWithoutReplica".getBytes();
    Get g = new Get(b1);
    Result r = table.get(g);
    assertFalse(r.isStale());
  }

  @Test
  public void testLocations() throws Exception {
    byte[] b1 = "testLocations".getBytes();
    ClusterConnection hc = (ClusterConnection) HTU.getAdmin().getConnection();
    hc.clearRegionLocationCache();
    RegionLocations rl = hc.locateRegion(table.getName(), b1, false, false);
    assertEquals(2, rl.size());

    rl = hc.locateRegion(table.getName(), b1, true, false);
    assertEquals(2, rl.size());

    hc.clearRegionLocationCache();
    rl = hc.locateRegion(table.getName(), b1, true, false);
    assertEquals(2, rl.size());

    rl = hc.locateRegion(table.getName(), b1, false, false);
    assertEquals(2, rl.size());
  }

  @Test
  public void testGetNoResultNoStaleRegionWithReplica() throws Exception {
    byte[] b1 = "testGetNoResultNoStaleRegionWithReplica".getBytes();
    // A get works and is not stale
    Get g = new Get(b1);
    Result r = table.get(g);
    assertFalse(r.isStale());
  }

  @Test
  public void testGetNoResultStaleRegionWithReplica() throws Exception {
    byte[] b1 = "testGetNoResultStaleRegionWithReplica".getBytes();
    openRegion(hriSecondary);

    SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
    Get g = new Get(b1);
    g.setConsistency(Consistency.TIMELINE);
    Result r = table.get(g);
    assertTrue(r.isStale());
  }

  @Test
  public void testGetNoResultNotStaleSleepRegionWithReplica() throws Exception {
    byte[] b1 = "testGetNoResultNotStaleSleepRegionWithReplica".getBytes();
    // We sleep; but we won't go to the stale region as we don't get the stale by default.
    SlowMeCopro.sleepTime.set(2000);
    Get g = new Get(b1);
    Result r = table.get(g);
    assertFalse(r.isStale());
  }

  @Test
  public void testFlushTable() throws Exception {
    flushRegion(hriPrimary);
    flushRegion(hriSecondary);

    Put p = new Put(row);
    p.addColumn(f, row, row);
    table.put(p);

    flushRegion(hriPrimary);
    flushRegion(hriSecondary);
  }

  @Test
  public void testFlushPrimary() throws Exception {
    flushRegion(hriPrimary);

    Put p = new Put(row);
    p.addColumn(f, row, row);
    table.put(p);

    flushRegion(hriPrimary);
  }

  @Test
  public void testFlushSecondary() throws Exception {
    flushRegion(hriSecondary);

    Put p = new Put(row);
    p.addColumn(f, row, row);
    table.put(p);

    flushRegion(hriSecondary);
  }

  @Test
  public void testUseRegionWithReplica() throws Exception {
    byte[] b1 = "testUseRegionWithReplica".getBytes();
    // A simple put works, even if there here a second replica
    Put p = new Put(b1);
    p.addColumn(f, b1, b1);
    table.put(p);
    LOG.info("Put done");

    // A get works and is not stale
    Get g = new Get(b1);
    Result r = table.get(g);
    assertFalse(r.isStale());
    assertFalse(r.getColumnCells(f, b1).isEmpty());
    LOG.info("get works and is not stale done");

    // Even if it we have to wait a little on the main region
    SlowMeCopro.sleepTime.set(2000);
    g = new Get(b1);
    r = table.get(g);
    assertFalse(r.isStale());
    assertFalse(r.getColumnCells(f, b1).isEmpty());
    SlowMeCopro.sleepTime.set(0);
    LOG.info("sleep and is not stale done");

    // But if we ask for stale we will get it
    SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
    g = new Get(b1);
    g.setConsistency(Consistency.TIMELINE);
    r = table.get(g);
    assertTrue(r.isStale());
    assertTrue(r.getColumnCells(f, b1).isEmpty());
    SlowMeCopro.getPrimaryCdl().get().countDown();

    LOG.info("stale done");

    // exists works and is not stale
    g = new Get(b1);
    g.setCheckExistenceOnly(true);
    r = table.get(g);
    assertFalse(r.isStale());
    assertTrue(r.getExists());
    LOG.info("exists not stale done");

    // exists works on stale but don't see the put
    SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
    g = new Get(b1);
    g.setCheckExistenceOnly(true);
    g.setConsistency(Consistency.TIMELINE);
    r = table.get(g);
    assertTrue(r.isStale());
    assertFalse("The secondary has stale data", r.getExists());
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
    assertTrue(r.isStale());
    assertFalse(r.isEmpty());
    SlowMeCopro.getPrimaryCdl().get().countDown();
    LOG.info("stale done");

    // exists works on stale and we see the put after the flush
    SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
    g = new Get(b1);
    g.setCheckExistenceOnly(true);
    g.setConsistency(Consistency.TIMELINE);
    r = table.get(g);
    assertTrue(r.isStale());
    assertTrue(r.getExists());
    SlowMeCopro.getPrimaryCdl().get().countDown();
    LOG.info("exists stale after flush done");
  }

  @Test
  public void testHedgedRead() throws Exception {
    byte[] b1 = "testHedgedRead".getBytes();
    // A simple put works, even if there here a second replica
    Put p = new Put(b1);
    p.addColumn(f, b1, b1);
    table.put(p);
    LOG.info("Put done");

    // A get works and is not stale
    Get g = new Get(b1);
    Result r = table.get(g);
    assertFalse(r.isStale());
    assertFalse(r.getColumnCells(f, b1).isEmpty());
    LOG.info("get works and is not stale done");

    // reset
    ClusterConnection connection = (ClusterConnection) HTU.getConnection();
    Counter hedgedReadOps = connection.getConnectionMetrics().hedgedReadOps;
    Counter hedgedReadWin = connection.getConnectionMetrics().hedgedReadWin;
    hedgedReadOps.dec(hedgedReadOps.getCount());
    hedgedReadWin.dec(hedgedReadWin.getCount());

    // Wait a little on the main region, just enough to happen once hedged read
    // and hedged read did not returned faster
    int primaryCallTimeoutMicroSecond =
      connection.getConnectionConfiguration().getPrimaryCallTimeoutMicroSecond();
    SlowMeCopro.sleepTime.set(TimeUnit.MICROSECONDS.toMillis(primaryCallTimeoutMicroSecond));
    SlowMeCopro.getSecondaryCdl().set(new CountDownLatch(1));
    g = new Get(b1);
    g.setConsistency(Consistency.TIMELINE);
    r = table.get(g);
    assertFalse(r.isStale());
    assertFalse(r.getColumnCells(f, b1).isEmpty());
    assertEquals(1, hedgedReadOps.getCount());
    assertEquals(0, hedgedReadWin.getCount());
    SlowMeCopro.sleepTime.set(0);
    SlowMeCopro.getSecondaryCdl().get().countDown();
    LOG.info("hedged read occurred but not faster");

    // But if we ask for stale we will get it and hedged read returned faster
    SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
    g = new Get(b1);
    g.setConsistency(Consistency.TIMELINE);
    r = table.get(g);
    assertTrue(r.isStale());
    assertTrue(r.getColumnCells(f, b1).isEmpty());
    assertEquals(2, hedgedReadOps.getCount());
    assertEquals(1, hedgedReadWin.getCount());
    SlowMeCopro.getPrimaryCdl().get().countDown();
    LOG.info("hedged read occurred and faster");
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

  @Test
  public void testCancelOfScan() throws Exception {
    int numRows = 100;
    for (int i = 0; i < numRows; i++) {
      byte[] b1 = Bytes.toBytes("testUseRegionWithReplica" + i);
      Put p = new Put(b1);
      p.addColumn(f, b1, b1);
      table.put(p);
    }
    LOG.debug("PUT done");
    int caching = 20;
    byte[] start;
    start = Bytes.toBytes("testUseRegionWithReplica" + 0);

    flushRegion(hriPrimary);
    LOG.info("flush done");
    Thread.sleep(1000 + REFRESH_PERIOD * 2);

    // now make some 'next' calls slow
    SlowMeCopro.slowDownNext.set(true);
    SlowMeCopro.countOfNext.set(0);
    SlowMeCopro.sleepTime.set(5000);

    Scan scan = new Scan().withStartRow(start);
    scan.setCaching(caching);
    scan.setConsistency(Consistency.TIMELINE);
    ResultScanner scanner = table.getScanner(scan);
    Iterator<Result> iter = scanner.iterator();
    iter.next();
    assertTrue(((ClientScanner) scanner).isAnyRPCcancelled());
    SlowMeCopro.slowDownNext.set(false);
    SlowMeCopro.countOfNext.set(0);
  }

  // make sure the scan will only go to the specific replica
  @Test
  public void testScanOnSpecificReplica() throws Exception {
    Scan scan = new Scan().setReplicaId(1).setConsistency(Consistency.TIMELINE);
    try (ResultScanner scanner = table.getScanner(scan)) {
      scanner.next();
    }
    assertTrue(SlowMeCopro.secondaryCountOfScan.get() > 0);
    assertEquals(0, SlowMeCopro.primaryCountOfScan.get());
  }

  // make sure the scan will only go to the specific replica
  @Test
  public void testReverseScanOnSpecificReplica() throws Exception {
    Scan scan = new Scan().setReversed(true).setReplicaId(1).setConsistency(Consistency.TIMELINE);
    try (ResultScanner scanner = table.getScanner(scan)) {
      scanner.next();
    }
    assertTrue(SlowMeCopro.secondaryCountOfScan.get() > 0);
    assertEquals(0, SlowMeCopro.primaryCountOfScan.get());
  }

  private void runMultipleScansOfOneType(boolean reversed, boolean small) throws Exception {
    int numRows = 100;
    int numCols = 10;
    for (int i = 0; i < numRows; i++) {
      byte[] b1 = Bytes.toBytes("testUseRegionWithReplica" + i);
      for (int col = 0; col < numCols; col++) {
        Put p = new Put(b1);
        String qualifier = "qualifer" + col;
        KeyValue kv = new KeyValue(b1, f, qualifier.getBytes());
        p.add(kv);
        table.put(p);
      }
    }
    LOG.debug("PUT done");
    int caching = 20;
    long maxResultSize = Long.MAX_VALUE;

    byte[] start;
    if (reversed) {
      start = Bytes.toBytes("testUseRegionWithReplica" + (numRows - 1));
    } else {
      start = Bytes.toBytes("testUseRegionWithReplica" + 0);
    }

    scanWithReplicas(reversed, small, Consistency.TIMELINE, caching, maxResultSize, start, numRows,
      numCols, false, false);

    // Even if we were to slow the server down, unless we ask for stale
    // we won't get it
    SlowMeCopro.sleepTime.set(5000);
    scanWithReplicas(reversed, small, Consistency.STRONG, caching, maxResultSize, start, numRows,
      numCols, false, false);
    SlowMeCopro.sleepTime.set(0);

    flushRegion(hriPrimary);
    LOG.info("flush done");
    Thread.sleep(1000 + REFRESH_PERIOD * 2);

    // Now set the flag to get a response even if stale
    SlowMeCopro.sleepTime.set(5000);
    scanWithReplicas(reversed, small, Consistency.TIMELINE, caching, maxResultSize, start, numRows,
      numCols, true, false);
    SlowMeCopro.sleepTime.set(0);

    // now make some 'next' calls slow
    SlowMeCopro.slowDownNext.set(true);
    SlowMeCopro.countOfNext.set(0);
    scanWithReplicas(reversed, small, Consistency.TIMELINE, caching, maxResultSize, start, numRows,
      numCols, true, true);
    SlowMeCopro.slowDownNext.set(false);
    SlowMeCopro.countOfNext.set(0);

    // Make sure we do not get stale data..
    SlowMeCopro.sleepTime.set(5000);
    scanWithReplicas(reversed, small, Consistency.STRONG, caching, maxResultSize, start, numRows,
      numCols, false, false);
    SlowMeCopro.sleepTime.set(0);

    // While the next calls are slow, set maxResultSize to 1 so that some partial results will be
    // returned from the server before the replica switch occurs.
    maxResultSize = 1;
    SlowMeCopro.slowDownNext.set(true);
    SlowMeCopro.countOfNext.set(0);
    scanWithReplicas(reversed, small, Consistency.TIMELINE, caching, maxResultSize, start, numRows,
      numCols, true, true);
    maxResultSize = Long.MAX_VALUE;
    SlowMeCopro.slowDownNext.set(false);
    SlowMeCopro.countOfNext.set(0);
  }

  private void scanWithReplicas(boolean reversed, boolean small, Consistency consistency,
      int caching, long maxResultSize, byte[] startRow, int numRows, int numCols,
      boolean staleExpected, boolean slowNext)
          throws Exception {
    Scan scan = new Scan().withStartRow(startRow);
    scan.setCaching(caching);
    scan.setMaxResultSize(maxResultSize);
    scan.setReversed(reversed);
    scan.setSmall(small);
    scan.setConsistency(consistency);
    ResultScanner scanner = table.getScanner(scan);
    Iterator<Result> iter = scanner.iterator();

    // Maps of row keys that we have seen so far
    HashMap<String, Boolean> map = new HashMap<>();

    // Tracked metrics
    int rowCount = 0;
    int cellCount = 0;
    int countOfStale = 0;

    while (iter.hasNext()) {
      rowCount++;
      Result r = iter.next();
      String row = new String(r.getRow());

      if (map.containsKey(row)) {
        throw new Exception("Unexpected scan result. Repeated row " + Bytes.toString(r.getRow()));
      }

      map.put(row, true);
      cellCount += r.rawCells().length;

      if (!slowNext) {
        assertTrue(r.isStale() == staleExpected);
      }
      if (r.isStale()) {
        countOfStale++;
      }
    }
    assertTrue("Count of rows " + rowCount + " num rows expected " + numRows,
      rowCount == numRows);
    assertTrue("Count of cells: " + cellCount + " cells expected: " + numRows * numCols,
      cellCount == (numRows * numCols));

    if (slowNext) {
      LOG.debug("Count of Stale " + countOfStale);
      assertTrue(countOfStale > 1);

      // If the scan was configured in such a way that a full row was NOT retrieved before the
      // replica switch occurred, then it is possible that all rows were stale
      if (maxResultSize != Long.MAX_VALUE) {
        assertTrue(countOfStale <= numRows);
      } else {
        assertTrue(countOfStale < numRows);
      }
    }
  }
}
