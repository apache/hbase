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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import com.google.protobuf.Service;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.quotas.RegionServerQuotaManager;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.wal.RegionGroupingProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableList;

/**
 * Test the {@link SplitTransactionImpl} class against an HRegion (as opposed to
 * running cluster).
 */
@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class TestSplitTransaction {
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final Path testdir =
    TEST_UTIL.getDataTestDir(this.getClass().getName());
  private HRegion parent;
  private WALFactory wals;
  private FileSystem fs;
  private static final byte [] STARTROW = new byte [] {'a', 'a', 'a'};
  // '{' is next ascii after 'z'.
  private static final byte [] ENDROW = new byte [] {'{', '{', '{'};
  private static final byte [] GOOD_SPLIT_ROW = new byte [] {'d', 'd', 'd'};
  private static final byte [] CF = HConstants.CATALOG_FAMILY;
  
  private static boolean preRollBackCalled = false;
  private static boolean postRollBackCalled = false;

  private String walProvider;
  private String strategy;

  @Parameterized.Parameters
  public static final Collection<Object[]> parameters() throws IOException {
    List<Object[]> params = new ArrayList<>(4);
    params.add(new Object[] { "filesystem", "" });
    params.add(new Object[] { "multiwal", "identity" });
    params.add(new Object[] { "multiwal", "bounded" });
    params.add(new Object[] { "multiwal", "namespace" });
    return params;
  }

  public TestSplitTransaction(String walProvider, String strategy) {
    this.walProvider = walProvider;
    this.strategy = strategy;
  }

  @Before
  public void setup() throws IOException {
    this.fs = FileSystem.get(TEST_UTIL.getConfiguration());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, CustomObserver.class.getName());
    this.fs.delete(this.testdir, true);
    TEST_UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, walProvider);
    if (!strategy.isEmpty()) {
      TEST_UTIL.getConfiguration().set(RegionGroupingProvider.REGION_GROUPING_STRATEGY, strategy);
    }
    final Configuration walConf = new Configuration(TEST_UTIL.getConfiguration());
    FSUtils.setRootDir(walConf, this.testdir);
    this.wals = new WALFactory(walConf, null, this.getClass().getName());
    this.parent = createRegion(this.testdir, this.wals);
    RegionCoprocessorHost host = new RegionCoprocessorHost(this.parent, null, TEST_UTIL.getConfiguration());
    this.parent.setCoprocessorHost(host);
    TEST_UTIL.getConfiguration().setBoolean("hbase.testing.nocluster", true);
  }

  @After
  public void teardown() throws IOException {
    if (this.parent != null && !this.parent.isClosed()) this.parent.close();
    if (this.parent != null) {
      Path regionDir = this.parent.getRegionFileSystem().getRegionDir();
      if (this.fs.exists(regionDir) && !this.fs.delete(regionDir, true)) {
        throw new IOException("Failed delete of " + regionDir);
      }
    }
    if (this.wals != null) {
      this.wals.close();
    }
    this.fs.delete(this.testdir, true);
  }

  @Test
  public void testFailAfterPONR() throws IOException, KeeperException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Start transaction.
    SplitTransactionImpl st = prepareGOOD_SPLIT_ROW();
    SplitTransactionImpl spiedUponSt = spy(st);
    Mockito
        .doThrow(new MockedFailedDaughterOpen())
        .when(spiedUponSt)
        .openDaughterRegion((Server) Mockito.anyObject(),
            (HRegion) Mockito.anyObject());

    // Run the execute.  Look at what it returns.
    boolean expectedException = false;
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    try {
      spiedUponSt.execute(mockServer, null);
    } catch (IOException e) {
      if (e.getCause() != null &&
          e.getCause() instanceof MockedFailedDaughterOpen) {
        expectedException = true;
      }
    }
    assertTrue(expectedException);
    // Run rollback returns that we should restart.
    assertFalse(spiedUponSt.rollback(null, null));
    // Make sure that region a and region b are still in the filesystem, that
    // they have not been removed; this is supposed to be the case if we go
    // past point of no return.
    Path tableDir =  this.parent.getRegionFileSystem().getTableDir();
    Path daughterADir = new Path(tableDir, spiedUponSt.getFirstDaughter().getEncodedName());
    Path daughterBDir = new Path(tableDir, spiedUponSt.getSecondDaughter().getEncodedName());
    assertTrue(TEST_UTIL.getTestFileSystem().exists(daughterADir));
    assertTrue(TEST_UTIL.getTestFileSystem().exists(daughterBDir));
  }

  /**
   * Test straight prepare works.  Tries to split on {@link #GOOD_SPLIT_ROW}
   * @throws IOException
   */
  @Test
  public void testPrepare() throws IOException {
    prepareGOOD_SPLIT_ROW();
  }

  private SplitTransactionImpl prepareGOOD_SPLIT_ROW() throws IOException {
    return prepareGOOD_SPLIT_ROW(this.parent);
  }

  private SplitTransactionImpl prepareGOOD_SPLIT_ROW(final HRegion parentRegion)
      throws IOException {
    SplitTransactionImpl st = new SplitTransactionImpl(parentRegion, GOOD_SPLIT_ROW);
    assertTrue(st.prepare());
    return st;
  }

  /**
   * Pass a reference store
   */
  @Test
  public void testPrepareWithRegionsWithReference() throws IOException {
    HStore storeMock = Mockito.mock(HStore.class);
    when(storeMock.hasReferences()).thenReturn(true);
    when(storeMock.getFamily()).thenReturn(new HColumnDescriptor("cf"));
    when(storeMock.close()).thenReturn(ImmutableList.<StoreFile>of());
    this.parent.stores.put(Bytes.toBytes(""), storeMock);

    SplitTransactionImpl st = new SplitTransactionImpl(this.parent, GOOD_SPLIT_ROW);

    assertFalse("a region should not be splittable if it has instances of store file references",
                st.prepare());
  }

  /**
   * Test SplitTransactionListener
   */
  @Test
  public void testSplitTransactionListener() throws IOException {
    SplitTransactionImpl st = new SplitTransactionImpl(this.parent, GOOD_SPLIT_ROW);
    SplitTransaction.TransactionListener listener =
            Mockito.mock(SplitTransaction.TransactionListener.class);
    st.registerTransactionListener(listener);
    st.prepare();
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    PairOfSameType<Region> daughters = st.execute(mockServer, null);
    verify(listener).transition(st, SplitTransaction.SplitTransactionPhase.STARTED,
            SplitTransaction.SplitTransactionPhase.PREPARED);
    verify(listener, times(15)).transition(any(SplitTransaction.class),
            any(SplitTransaction.SplitTransactionPhase.class),
            any(SplitTransaction.SplitTransactionPhase.class));
    verifyNoMoreInteractions(listener);
  }

  /**
   * Pass an unreasonable split row.
   */
  @Test
  public void testPrepareWithBadSplitRow() throws IOException {
    // Pass start row as split key.
    SplitTransactionImpl st = new SplitTransactionImpl(this.parent, STARTROW);
    assertFalse(st.prepare());
    st = new SplitTransactionImpl(this.parent, HConstants.EMPTY_BYTE_ARRAY);
    assertFalse(st.prepare());
    st = new SplitTransactionImpl(this.parent, new byte [] {'A', 'A', 'A'});
    assertFalse(st.prepare());
    st = new SplitTransactionImpl(this.parent, ENDROW);
    assertFalse(st.prepare());
  }

  @Test
  public void testPrepareWithClosedRegion() throws IOException {
    this.parent.close();
    SplitTransactionImpl st = new SplitTransactionImpl(this.parent, GOOD_SPLIT_ROW);
    assertFalse(st.prepare());
  }

  @Test
  public void testWholesomeSplit() throws IOException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF, true);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Pretend region's blocks are not in the cache, used for
    // testWholesomeSplitWithHFileV1
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    ((LruBlockCache) cacheConf.getBlockCache()).clearCache();

    // Start transaction.
    SplitTransactionImpl st = prepareGOOD_SPLIT_ROW();

    // Run the execute.  Look at what it returns.
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    PairOfSameType<Region> daughters = st.execute(mockServer, null);
    // Do some assertions about execution.
    assertTrue(this.fs.exists(this.parent.getRegionFileSystem().getSplitsDir()));
    // Assert the parent region is closed.
    assertTrue(this.parent.isClosed());

    // Assert splitdir is empty -- because its content will have been moved out
    // to be under the daughter region dirs.
    assertEquals(0, this.fs.listStatus(this.parent.getRegionFileSystem().getSplitsDir()).length);
    // Check daughters have correct key span.
    assertTrue(Bytes.equals(parent.getRegionInfo().getStartKey(),
      daughters.getFirst().getRegionInfo().getStartKey()));
    assertTrue(Bytes.equals(GOOD_SPLIT_ROW, daughters.getFirst().getRegionInfo().getEndKey()));
    assertTrue(Bytes.equals(daughters.getSecond().getRegionInfo().getStartKey(), GOOD_SPLIT_ROW));
    assertTrue(Bytes.equals(parent.getRegionInfo().getEndKey(),
      daughters.getSecond().getRegionInfo().getEndKey()));
    // Count rows. daughters are already open
    int daughtersRowCount = 0;
    for (Region openRegion: daughters) {
      try {
        int count = countRows(openRegion);
        assertTrue(count > 0 && count != rowcount);
        daughtersRowCount += count;
      } finally {
        ((HRegion) openRegion).close();
      }
    }
    assertEquals(rowcount, daughtersRowCount);
    // Assert the write lock is no longer held on parent
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());
  }

  @Test
  public void testCountReferencesFailsSplit() throws IOException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Start transaction.
    HRegion spiedRegion = spy(this.parent);
    SplitTransactionImpl st = prepareGOOD_SPLIT_ROW(spiedRegion);
    SplitTransactionImpl spiedUponSt = spy(st);
    doThrow(new IOException("Failing split. Expected reference file count isn't equal."))
        .when(spiedUponSt).assertReferenceFileCount(anyInt(),
        eq(new Path(this.parent.getRegionFileSystem().getTableDir(),
            st.getSecondDaughter().getEncodedName())));

    // Run the execute.  Look at what it returns.
    boolean expectedException = false;
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    try {
      spiedUponSt.execute(mockServer, null);
    } catch (IOException e) {
      expectedException = true;
    }
    assertTrue(expectedException);
  }

  @Test
  public void testRollback() throws IOException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Start transaction.
    HRegion spiedRegion = spy(this.parent);
    SplitTransactionImpl st = prepareGOOD_SPLIT_ROW(spiedRegion);
    SplitTransactionImpl spiedUponSt = spy(st);
    doNothing().when(spiedUponSt).assertReferenceFileCount(anyInt(),
        eq(parent.getRegionFileSystem().getSplitsDir(st.getFirstDaughter())));
    when(spiedRegion.createDaughterRegionFromSplits(spiedUponSt.getSecondDaughter())).
        thenThrow(new MockedFailedDaughterCreation());
    // Run the execute.  Look at what it returns.
    boolean expectedException = false;
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    try {
      spiedUponSt.execute(mockServer, null);
    } catch (MockedFailedDaughterCreation e) {
      expectedException = true;
    }
    assertTrue(expectedException);
    // Run rollback
    assertTrue(spiedUponSt.rollback(null, null));

    // Assert I can scan parent.
    int parentRowCount2 = countRows(this.parent);
    assertEquals(parentRowCount, parentRowCount2);

    // Assert rollback cleaned up stuff in fs
    assertTrue(!this.fs.exists(FSUtils.getRegionDirFromRootDir(this.testdir,
      st.getFirstDaughter())));
    assertTrue(!this.fs.exists(FSUtils.getRegionDirFromRootDir(this.testdir,
      st.getSecondDaughter())));
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());

    // Now retry the split but do not throw an exception this time.
    assertTrue(st.prepare());
    PairOfSameType<Region> daughters = st.execute(mockServer, null);
    // Count rows. daughters are already open
    int daughtersRowCount = 0;
    for (Region openRegion: daughters) {
      try {
        int count = countRows(openRegion);
        assertTrue(count > 0 && count != rowcount);
        daughtersRowCount += count;
      } finally {
        ((HRegion) openRegion).close();
      }
    }
    assertEquals(rowcount, daughtersRowCount);
    // Assert the write lock is no longer held on parent
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());
    assertTrue("Rollback hooks should be called.", wasRollBackHookCalled());
  }
  
  private boolean wasRollBackHookCalled(){
    return (preRollBackCalled && postRollBackCalled);
  }

  /**
   * Exception used in this class only.
   */
  @SuppressWarnings("serial")
  private class MockedFailedDaughterCreation extends IOException {}
  private class MockedFailedDaughterOpen extends IOException {}

  private int countRows(final Region r) throws IOException {
    int rowcount = 0;
    InternalScanner scanner = r.getScanner(new Scan());
    try {
      List<Cell> kvs = new ArrayList<Cell>();
      boolean hasNext = true;
      while (hasNext) {
        hasNext = scanner.next(kvs);
        if (!kvs.isEmpty()) rowcount++;
      }
    } finally {
      scanner.close();
    }
    return rowcount;
  }

  HRegion createRegion(final Path testdir, final WALFactory wals)
  throws IOException {
    // Make a region with start and end keys. Use 'aaa', to 'AAA'.  The load
    // region utility will add rows between 'aaa' and 'zzz'.
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("table"));
    HColumnDescriptor hcd = new HColumnDescriptor(CF);
    htd.addFamily(hcd);
    HRegionInfo hri = new HRegionInfo(htd.getTableName(), STARTROW, ENDROW);
    Configuration conf = TEST_UTIL.getConfiguration();
    HRegion r = HRegion.createHRegion(hri, testdir, conf, htd);
    HRegion.closeHRegion(r);
    ServerName sn = ServerName.valueOf("testSplitTransaction", 100, 42);
    final RegionServerServices rss = TEST_UTIL.createMockRegionServerService(sn);
    MockRegionServerServicesWithWALs rsw =
      new MockRegionServerServicesWithWALs(rss, wals.getWALProvider());
    return HRegion.openHRegion(testdir, hri, htd,
      wals.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace()),
      conf, rsw, null);
  }
  
  public static class CustomObserver extends BaseRegionObserver{
    @Override
    public void preRollBackSplit(
        ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
      preRollBackCalled = true;
    }
    
    @Override
    public void postRollBackSplit(
        ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
      postRollBackCalled = true;
    }
  }

  static class MockRegionServerServicesWithWALs implements RegionServerServices {
    WALProvider provider;
    RegionServerServices rss;

    MockRegionServerServicesWithWALs(RegionServerServices rss, WALProvider provider) {
      this.rss = rss;
      this.provider = provider;
    }

    @Override
    public boolean isStopping() {
      return rss.isStopping();
    }

    @Override
    public WAL getWAL(HRegionInfo hri) throws IOException {
      return provider.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace());
    }

    @Override
    public CompactionRequestor getCompactionRequester() {
      return rss.getCompactionRequester();
    }

    @Override
    public FlushRequester getFlushRequester() {
      return rss.getFlushRequester();
    }

    @Override
    public RegionServerAccounting getRegionServerAccounting() {
      return rss.getRegionServerAccounting();
    }

    @Override
    public TableLockManager getTableLockManager() {
      return rss.getTableLockManager();
    }

    @Override
    public RegionServerQuotaManager getRegionServerQuotaManager() {
      return rss.getRegionServerQuotaManager();
    }

    @Override
    public void postOpenDeployTasks(PostOpenDeployContext context)
        throws KeeperException, IOException {
      rss.postOpenDeployTasks(context);
    }

    @Override
    public void postOpenDeployTasks(Region r) throws KeeperException, IOException {
      rss.postOpenDeployTasks(r);
    }

    @Override
    public boolean reportRegionStateTransition(RegionStateTransitionContext context) {
      return rss.reportRegionStateTransition(context);
    }

    @Override
    public boolean reportRegionStateTransition(
        RegionServerStatusProtos.RegionStateTransition.TransitionCode code, long openSeqNum,
        HRegionInfo... hris) {
      return rss.reportRegionStateTransition(code, openSeqNum, hris);
    }

    @Override
    public boolean reportRegionStateTransition(
        RegionServerStatusProtos.RegionStateTransition.TransitionCode code, HRegionInfo... hris) {
      return rss.reportRegionStateTransition(code, hris);
    }

    @Override
    public RpcServerInterface getRpcServer() {
      return rss.getRpcServer();
    }

    @Override
    public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
      return rss.getRegionsInTransitionInRS();
    }

    @Override
    public FileSystem getFileSystem() {
      return rss.getFileSystem();
    }

    @Override
    public Leases getLeases() {
      return rss.getLeases();
    }

    @Override
    public ExecutorService getExecutorService() {
      return rss.getExecutorService();
    }

    @Override
    public Map<String, Region> getRecoveringRegions() {
      return rss.getRecoveringRegions();
    }

    @Override
    public ServerNonceManager getNonceManager() {
      return rss.getNonceManager();
    }

    @Override
    public boolean registerService(Service service) {
      return rss.registerService(service);
    }

    @Override
    public HeapMemoryManager getHeapMemoryManager() {
      return rss.getHeapMemoryManager();
    }

    @Override
    public double getCompactionPressure() {
      return rss.getCompactionPressure();
    }

    @Override
    public Set<TableName> getOnlineTables() {
      return rss.getOnlineTables();
    }

    @Override
    public ThroughputController getFlushThroughputController() {
      return rss.getFlushThroughputController();
    }

    @Override
    public double getFlushPressure() {
      return rss.getFlushPressure();
    }

    @Override
    public MetricsRegionServer getMetrics() {
      return rss.getMetrics();
    }

    @Override
    public void unassign(byte[] regionName) throws IOException {
      rss.unassign(regionName);
    }

    @Override
    public void addToOnlineRegions(Region r) {
      rss.addToOnlineRegions(r);
    }

    @Override
    public boolean removeFromOnlineRegions(Region r, ServerName destination) {
      return rss.removeFromOnlineRegions(r, destination);
    }

    @Override
    public Region getFromOnlineRegions(String encodedRegionName) {
      return rss.getFromOnlineRegions(encodedRegionName);
    }

    @Override
    public List<Region> getOnlineRegions(TableName tableName) throws IOException {
      return rss.getOnlineRegions(tableName);
    }

    @Override
    public List<Region> getOnlineRegions() {
      return rss.getOnlineRegions();
    }

    @Override
    public Configuration getConfiguration() {
      return rss.getConfiguration();
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return rss.getZooKeeper();
    }

    @Override
    public ClusterConnection getConnection() {
      return rss.getConnection();
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
      return rss.getMetaTableLocator();
    }

    @Override
    public ServerName getServerName() {
      return rss.getServerName();
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return rss.getCoordinatedStateManager();
    }

    @Override
    public ChoreService getChoreService() {
      return rss.getChoreService();
    }

    @Override
    public void abort(String why, Throwable e) {
      rss.abort(why, e);
    }

    @Override
    public boolean isAborted() {
      return rss.isAborted();
    }

    @Override
    public void stop(String why) {
      rss.stop(why);
    }

    @Override
    public boolean isStopped() {
      return rss.isStopped();
    }

    @Override
    public void updateRegionFavoredNodesMapping(String encodedRegionName,
        List<HBaseProtos.ServerName> favoredNodes) {
      rss.updateRegionFavoredNodesMapping(encodedRegionName, favoredNodes);
    }

    @Override
    public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
      return rss.getFavoredNodesForRegion(encodedRegionName);
    }
  }
}
