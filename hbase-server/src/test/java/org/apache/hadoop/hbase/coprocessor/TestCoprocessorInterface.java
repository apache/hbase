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
package org.apache.hadoop.hbase.coprocessor;

import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam2;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam3;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestCoprocessorInterface {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorInterface.class);

  @Rule public TestName name = new TestName();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  static final Path DIR = TEST_UTIL.getDataTestDir();

  private static class CustomScanner implements RegionScanner {

    private RegionScanner delegate;

    public CustomScanner(RegionScanner delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      return delegate.next(results);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext)
        throws IOException {
      return delegate.next(result, scannerContext);
    }

    @Override
    public boolean nextRaw(List<Cell> result)
        throws IOException {
      return delegate.nextRaw(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext context)
        throws IOException {
      return delegate.nextRaw(result, context);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public RegionInfo getRegionInfo() {
      return delegate.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() throws IOException {
      return delegate.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
      return false;
    }

    @Override
    public long getMaxResultSize() {
      return delegate.getMaxResultSize();
    }

    @Override
    public long getMvccReadPoint() {
      return delegate.getMvccReadPoint();
    }

    @Override
    public int getBatch() {
      return delegate.getBatch();
    }
  }

  public static class CoprocessorImpl implements RegionCoprocessor, RegionObserver {

    private boolean startCalled;
    private boolean stopCalled;
    private boolean preOpenCalled;
    private boolean postOpenCalled;
    private boolean preCloseCalled;
    private boolean postCloseCalled;
    private boolean preCompactCalled;
    private boolean postCompactCalled;
    private boolean preFlushCalled;
    private boolean postFlushCalled;
    private ConcurrentMap<String, Object> sharedData;

    @Override
    public void start(CoprocessorEnvironment e) {
      sharedData = ((RegionCoprocessorEnvironment)e).getSharedData();
      // using new String here, so that there will be new object on each invocation
      sharedData.putIfAbsent("test1", new Object());
      startCalled = true;
    }

    @Override
    public void stop(CoprocessorEnvironment e) {
      sharedData = null;
      stopCalled = true;
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
      preOpenCalled = true;
    }
    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
      postOpenCalled = true;
    }
    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
      preCloseCalled = true;
    }
    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
      postCloseCalled = true;
    }
    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
        Store store, InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
        CompactionRequest request) {
      preCompactCalled = true;
      return scanner;
    }
    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
        Store store, StoreFile resultFile, CompactionLifeCycleTracker tracker,
        CompactionRequest request) {
      postCompactCalled = true;
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e,
        FlushLifeCycleTracker tracker) {
      preFlushCalled = true;
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,
        FlushLifeCycleTracker tracker) {
      postFlushCalled = true;
    }

    @Override
    public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Scan scan, final RegionScanner s) throws IOException {
      return new CustomScanner(s);
    }

    boolean wasStarted() {
      return startCalled;
    }
    boolean wasStopped() {
      return stopCalled;
    }
    boolean wasOpened() {
      return (preOpenCalled && postOpenCalled);
    }
    boolean wasClosed() {
      return (preCloseCalled && postCloseCalled);
    }
    boolean wasFlushed() {
      return (preFlushCalled && postFlushCalled);
    }
    boolean wasCompacted() {
      return (preCompactCalled && postCompactCalled);
    }
    Map<String, Object> getSharedData() {
      return sharedData;
    }
  }

  public static class CoprocessorII implements RegionCoprocessor {
    private ConcurrentMap<String, Object> sharedData;

    @Override
    public void start(CoprocessorEnvironment e) {
      sharedData = ((RegionCoprocessorEnvironment)e).getSharedData();
      sharedData.putIfAbsent("test2", new Object());
    }

    @Override
    public void stop(CoprocessorEnvironment e) {
      sharedData = null;
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(new RegionObserver() {
        @Override
        public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
            final Get get, final List<Cell> results) throws IOException {
          throw new RuntimeException();
        }
      });
    }

    Map<String, Object> getSharedData() {
      return sharedData;
    }
  }

  @Test
  public void testSharedData() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] families = { fam1, fam2, fam3 };

    Configuration hc = initConfig();
    HRegion region = initHRegion(tableName, name.getMethodName(), hc, new Class<?>[]{}, families);

    for (int i = 0; i < 3; i++) {
      HBaseTestCase.addContent(region, fam3);
      region.flush(true);
    }

    region.compact(false);

    region = reopenRegion(region, CoprocessorImpl.class, CoprocessorII.class);

    Coprocessor c = region.getCoprocessorHost().findCoprocessor(CoprocessorImpl.class);
    Coprocessor c2 = region.getCoprocessorHost().findCoprocessor(CoprocessorII.class);
    Object o = ((CoprocessorImpl)c).getSharedData().get("test1");
    Object o2 = ((CoprocessorII)c2).getSharedData().get("test2");
    assertNotNull(o);
    assertNotNull(o2);
    // to coprocessors get different sharedDatas
    assertFalse(((CoprocessorImpl)c).getSharedData() == ((CoprocessorII)c2).getSharedData());
    c = region.getCoprocessorHost().findCoprocessor(CoprocessorImpl.class);
    c2 = region.getCoprocessorHost().findCoprocessor(CoprocessorII.class);
    // make sure that all coprocessor of a class have identical sharedDatas
    assertTrue(((CoprocessorImpl)c).getSharedData().get("test1") == o);
    assertTrue(((CoprocessorII)c2).getSharedData().get("test2") == o2);

    // now have all Environments fail
    try {
      byte [] r = region.getRegionInfo().getStartKey();
      if (r == null || r.length <= 0) {
        // Its the start row.  Can't ask for null.  Ask for minimal key instead.
        r = new byte [] {0};
      }
      Get g = new Get(r);
      region.get(g);
      fail();
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException xc) {
    }
    assertNull(region.getCoprocessorHost().findCoprocessor(CoprocessorII.class));
    c = region.getCoprocessorHost().findCoprocessor(CoprocessorImpl.class);
    assertTrue(((CoprocessorImpl)c).getSharedData().get("test1") == o);
    c = c2 = null;
    // perform a GC
    System.gc();
    // reopen the region
    region = reopenRegion(region, CoprocessorImpl.class, CoprocessorII.class);
    c = region.getCoprocessorHost().findCoprocessor(CoprocessorImpl.class);
    // CPimpl is unaffected, still the same reference
    assertTrue(((CoprocessorImpl)c).getSharedData().get("test1") == o);
    c2 = region.getCoprocessorHost().findCoprocessor(CoprocessorII.class);
    // new map and object created, hence the reference is different
    // hence the old entry was indeed removed by the GC and new one has been created
    Object o3 = ((CoprocessorII)c2).getSharedData().get("test2");
    assertFalse(o3 == o2);
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  @Test
  public void testCoprocessorInterface() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] families = { fam1, fam2, fam3 };

    Configuration hc = initConfig();
    HRegion region = initHRegion(tableName, name.getMethodName(), hc,
      new Class<?>[]{CoprocessorImpl.class}, families);
    for (int i = 0; i < 3; i++) {
      HBaseTestCase.addContent(region, fam3);
      region.flush(true);
    }

    region.compact(false);

    // HBASE-4197
    Scan s = new Scan();
    RegionScanner scanner = region.getCoprocessorHost().postScannerOpen(s, region.getScanner(s));
    assertTrue(scanner instanceof CustomScanner);
    // this would throw an exception before HBASE-4197
    scanner.next(new ArrayList<>());

    HBaseTestingUtility.closeRegionAndWAL(region);
    Coprocessor c = region.getCoprocessorHost().findCoprocessor(CoprocessorImpl.class);

    assertTrue("Coprocessor not started", ((CoprocessorImpl)c).wasStarted());
    assertTrue("Coprocessor not stopped", ((CoprocessorImpl)c).wasStopped());
    assertTrue(((CoprocessorImpl)c).wasOpened());
    assertTrue(((CoprocessorImpl)c).wasClosed());
    assertTrue(((CoprocessorImpl)c).wasFlushed());
    assertTrue(((CoprocessorImpl)c).wasCompacted());
  }

  HRegion reopenRegion(final HRegion closedRegion, Class<?> ... implClasses)
      throws IOException {
    //RegionInfo info = new RegionInfo(tableName, null, null, false);
    HRegion r = HRegion.openHRegion(closedRegion, null);

    // this following piece is a hack. currently a coprocessorHost
    // is secretly loaded at OpenRegionHandler. we don't really
    // start a region server here, so just manually create cphost
    // and set it to region.
    Configuration conf = TEST_UTIL.getConfiguration();
    RegionCoprocessorHost host = new RegionCoprocessorHost(r,
        Mockito.mock(RegionServerServices.class), conf);
    r.setCoprocessorHost(host);

    for (Class<?> implClass : implClasses) {
      host.load(implClass.asSubclass(RegionCoprocessor.class), Coprocessor.PRIORITY_USER, conf);
    }
    // we need to manually call pre- and postOpen here since the
    // above load() is not the real case for CP loading. A CP is
    // expected to be loaded by default from 1) configuration; or 2)
    // HTableDescriptor. If it's loaded after HRegion initialized,
    // the pre- and postOpen() won't be triggered automatically.
    // Here we have to call pre and postOpen explicitly.
    host.preOpen();
    host.postOpen();
    return r;
  }

  HRegion initHRegion (TableName tableName, String callingMethod,
      Configuration conf, Class<?> [] implClasses, byte [][] families)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(null)
        .setEndKey(null)
        .setSplit(false)
        .build();
    Path path = new Path(DIR + callingMethod);
    HRegion r = HBaseTestingUtility.createRegionAndWAL(info, path, conf, htd);

    // this following piece is a hack.
    RegionCoprocessorHost host =
        new RegionCoprocessorHost(r, Mockito.mock(RegionServerServices.class), conf);
    r.setCoprocessorHost(host);

    for (Class<?> implClass : implClasses) {
      host.load(implClass.asSubclass(RegionCoprocessor.class), Coprocessor.PRIORITY_USER, conf);
      Coprocessor c = host.findCoprocessor(implClass.getName());
      assertNotNull(c);
    }

    // Here we have to call pre and postOpen explicitly.
    host.preOpen();
    host.postOpen();
    return r;
  }

  private Configuration initConfig() {
    // Always compact if there is more than one store file.
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 2);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 10 * 1000);
    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 15 * 1000);
    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    TEST_UTIL.getConfiguration().setLong(HConstants.HREGION_MAX_FILESIZE,
        1024 * 128);
    TEST_UTIL.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);

    return TEST_UTIL.getConfiguration();
  }
}
