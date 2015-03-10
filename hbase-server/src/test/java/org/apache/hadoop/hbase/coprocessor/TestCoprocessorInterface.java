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

package org.apache.hadoop.hbase.coprocessor;

import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam2;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam3;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.regionserver.SplitTransactionFactory;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestCoprocessorInterface {
  @Rule public TestName name = new TestName();
  static final Log LOG = LogFactory.getLog(TestCoprocessorInterface.class);
  private static final HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();
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
    public HRegionInfo getRegionInfo() {
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

  public static class CoprocessorImpl extends BaseRegionObserver {

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
    private boolean preSplitCalled;
    private boolean postSplitCalled;
    private boolean preSplitWithSplitRowCalled;
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
        Store store, InternalScanner scanner, ScanType scanType) {
      preCompactCalled = true;
      return scanner;
    }
    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
        Store store, StoreFile resultFile) {
      postCompactCalled = true;
    }
    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) {
      preFlushCalled = true;
    }
    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) {
      postFlushCalled = true;
    }
    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) {
      preSplitCalled = true;
    }
    
    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,
        byte[] splitRow) throws IOException {
      preSplitWithSplitRowCalled = true;
    }
    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, Region l, Region r) {
      postSplitCalled = true;
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
    boolean wasSplit() {
      return (preSplitCalled && postSplitCalled && preSplitWithSplitRowCalled);
    }
    Map<String, Object> getSharedData() {
      return sharedData;
    }
  }

  public static class CoprocessorII extends BaseRegionObserver {
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
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Get get, final List<Cell> results) throws IOException {
      if (1/0 == 1) {
        e.complete();
      }
    }

    Map<String, Object> getSharedData() {
      return sharedData;
    }
  }

  @Test
  public void testSharedData() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] families = { fam1, fam2, fam3 };

    Configuration hc = initSplit();
    Region region = initHRegion(tableName, name.getMethodName(), hc,
      new Class<?>[]{}, families);

    for (int i = 0; i < 3; i++) {
      HBaseTestCase.addContent(region, fam3);
      region.flush(true);
    }

    region.compact(false);

    byte [] splitRow = ((HRegion)region).checkSplit();
    assertNotNull(splitRow);
    Region [] regions = split(region, splitRow);
    for (int i = 0; i < regions.length; i++) {
      regions[i] = reopenRegion(regions[i], CoprocessorImpl.class, CoprocessorII.class);
    }
    Coprocessor c = regions[0].getCoprocessorHost().
        findCoprocessor(CoprocessorImpl.class.getName());
    Coprocessor c2 = regions[0].getCoprocessorHost().
        findCoprocessor(CoprocessorII.class.getName());
    Object o = ((CoprocessorImpl)c).getSharedData().get("test1");
    Object o2 = ((CoprocessorII)c2).getSharedData().get("test2");
    assertNotNull(o);
    assertNotNull(o2);
    // to coprocessors get different sharedDatas
    assertFalse(((CoprocessorImpl)c).getSharedData() == ((CoprocessorII)c2).getSharedData());
    for (int i = 1; i < regions.length; i++) {
      c = regions[i].getCoprocessorHost().
          findCoprocessor(CoprocessorImpl.class.getName());
      c2 = regions[i].getCoprocessorHost().
          findCoprocessor(CoprocessorII.class.getName());
      // make sure that all coprocessor of a class have identical sharedDatas
      assertTrue(((CoprocessorImpl)c).getSharedData().get("test1") == o);
      assertTrue(((CoprocessorII)c2).getSharedData().get("test2") == o2);
    }
    // now have all Environments fail
    for (int i = 0; i < regions.length; i++) {
      try {
        byte [] r = regions[i].getRegionInfo().getStartKey();
        if (r == null || r.length <= 0) {
          // Its the start row.  Can't ask for null.  Ask for minimal key instead.
          r = new byte [] {0};
        }
        Get g = new Get(r);
        regions[i].get(g);
        fail();
      } catch (org.apache.hadoop.hbase.DoNotRetryIOException xc) {
      }
      assertNull(regions[i].getCoprocessorHost().
          findCoprocessor(CoprocessorII.class.getName()));
    }
    c = regions[0].getCoprocessorHost().
        findCoprocessor(CoprocessorImpl.class.getName());
    assertTrue(((CoprocessorImpl)c).getSharedData().get("test1") == o);
    c = c2 = null;
    // perform a GC
    System.gc();
    // reopen the region
    region = reopenRegion(regions[0], CoprocessorImpl.class, CoprocessorII.class);
    c = region.getCoprocessorHost().
        findCoprocessor(CoprocessorImpl.class.getName());
    // CPimpl is unaffected, still the same reference
    assertTrue(((CoprocessorImpl)c).getSharedData().get("test1") == o);
    c2 = region.getCoprocessorHost().
        findCoprocessor(CoprocessorII.class.getName());
    // new map and object created, hence the reference is different
    // hence the old entry was indeed removed by the GC and new one has been created
    Object o3 = ((CoprocessorII)c2).getSharedData().get("test2");
    assertFalse(o3 == o2);
  }

  @Test
  public void testCoprocessorInterface() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] families = { fam1, fam2, fam3 };

    Configuration hc = initSplit();
    Region region = initHRegion(tableName, name.getMethodName(), hc,
      new Class<?>[]{CoprocessorImpl.class}, families);
    for (int i = 0; i < 3; i++) {
      HBaseTestCase.addContent(region, fam3);
      region.flush(true);
    }

    region.compact(false);

    byte [] splitRow = ((HRegion)region).checkSplit();

    assertNotNull(splitRow);
    Region [] regions = split(region, splitRow);
    for (int i = 0; i < regions.length; i++) {
      regions[i] = reopenRegion(regions[i], CoprocessorImpl.class);
    }
    HRegion.closeHRegion((HRegion)region);
    Coprocessor c = region.getCoprocessorHost().
      findCoprocessor(CoprocessorImpl.class.getName());

    // HBASE-4197
    Scan s = new Scan();
    RegionScanner scanner = regions[0].getCoprocessorHost().postScannerOpen(s, regions[0].getScanner(s));
    assertTrue(scanner instanceof CustomScanner);
    // this would throw an exception before HBASE-4197
    scanner.next(new ArrayList<Cell>());

    assertTrue("Coprocessor not started", ((CoprocessorImpl)c).wasStarted());
    assertTrue("Coprocessor not stopped", ((CoprocessorImpl)c).wasStopped());
    assertTrue(((CoprocessorImpl)c).wasOpened());
    assertTrue(((CoprocessorImpl)c).wasClosed());
    assertTrue(((CoprocessorImpl)c).wasFlushed());
    assertTrue(((CoprocessorImpl)c).wasCompacted());
    assertTrue(((CoprocessorImpl)c).wasSplit());

    for (int i = 0; i < regions.length; i++) {
      HRegion.closeHRegion((HRegion)regions[i]);
      c = region.getCoprocessorHost()
            .findCoprocessor(CoprocessorImpl.class.getName());
      assertTrue("Coprocessor not started", ((CoprocessorImpl)c).wasStarted());
      assertTrue("Coprocessor not stopped", ((CoprocessorImpl)c).wasStopped());
      assertTrue(((CoprocessorImpl)c).wasOpened());
      assertTrue(((CoprocessorImpl)c).wasClosed());
      assertTrue(((CoprocessorImpl)c).wasCompacted());
    }
  }

  Region reopenRegion(final Region closedRegion, Class<?> ... implClasses)
      throws IOException {
    //HRegionInfo info = new HRegionInfo(tableName, null, null, false);
    Region r = HRegion.openHRegion(closedRegion, null);

    // this following piece is a hack. currently a coprocessorHost
    // is secretly loaded at OpenRegionHandler. we don't really
    // start a region server here, so just manually create cphost
    // and set it to region.
    Configuration conf = TEST_UTIL.getConfiguration();
    RegionCoprocessorHost host = new RegionCoprocessorHost(r, null, conf);
    ((HRegion)r).setCoprocessorHost(host);

    for (Class<?> implClass : implClasses) {
      host.load(implClass, Coprocessor.PRIORITY_USER, conf);
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

  Region initHRegion (TableName tableName, String callingMethod,
      Configuration conf, Class<?> [] implClasses, byte [][] families)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(tableName, null, null, false);
    Path path = new Path(DIR + callingMethod);
    HRegion r = HRegion.createHRegion(info, path, conf, htd);

    // this following piece is a hack.
    RegionCoprocessorHost host = new RegionCoprocessorHost(r, null, conf);
    r.setCoprocessorHost(host);

    for (Class<?> implClass : implClasses) {
      host.load(implClass, Coprocessor.PRIORITY_USER, conf);
      Coprocessor c = host.findCoprocessor(implClass.getName());
      assertNotNull(c);
    }

    // Here we have to call pre and postOpen explicitly.
    host.preOpen();
    host.postOpen();
    return r;
  }

  Configuration initSplit() {
    // Always compact if there is more than one store file.
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 2);
    // Make lease timeout longer, lease checks less frequent
    TEST_UTIL.getConfiguration().setInt(
        "hbase.master.lease.thread.wakefrequency", 5 * 1000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 10 * 1000);
    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 15 * 1000);
    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    TEST_UTIL.getConfiguration().setLong(HConstants.HREGION_MAX_FILESIZE,
        1024 * 128);
    TEST_UTIL.getConfiguration().setBoolean("hbase.testing.nocluster",
        true);
    TEST_UTIL.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);

    return TEST_UTIL.getConfiguration();
  }

  private Region [] split(final Region r, final byte [] splitRow) throws IOException {
    Region[] regions = new Region[2];

    SplitTransaction st = new SplitTransactionFactory(TEST_UTIL.getConfiguration())
      .create(r, splitRow);
    int i = 0;

    if (!st.prepare()) {
      // test fails.
      assertTrue(false);
    }
    try {
      Server mockServer = Mockito.mock(Server.class);
      when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
      PairOfSameType<Region> daughters = st.execute(mockServer, null);
      for (Region each_daughter: daughters) {
        regions[i] = each_daughter;
        i++;
      }
    } catch (IOException ioe) {
      LOG.info("Split transaction of " + r.getRegionInfo().getRegionNameAsString() +
          " failed:" + ioe.getMessage());
      assertTrue(false);
    } catch (RuntimeException e) {
      LOG.info("Failed rollback of failed split of " +
          r.getRegionInfo().getRegionNameAsString() + e.getMessage());
    }

    assertTrue(i == 2);
    return regions;
  }

}