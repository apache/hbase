/*
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
package org.apache.hadoop.hbase.replication;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationGlobalSourceSource;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationGlobalSourceSourceImpl;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationSourceImpl;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationSourceSource;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationSourceSourceImpl;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationTableSource;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests ReplicationSource and ReplicationEndpoint interactions
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationEndpoint extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationEndpoint.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationEndpoint.class);

  static int numRegionServers;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();
    numRegionServers = UTIL1.getHBaseCluster().getRegionServerThreads().size();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestReplicationBase.tearDownAfterClass();
    // check stop is called
    Assert.assertTrue(ReplicationEndpointForTest.stoppedCount.get() > 0);
  }

  @Before
  public void setup() throws Exception {
    ReplicationEndpointForTest.contructedCount.set(0);
    ReplicationEndpointForTest.startedCount.set(0);
    ReplicationEndpointForTest.replicateCount.set(0);
    ReplicationEndpointReturningFalse.replicated.set(false);
    ReplicationEndpointForTest.lastEntries = null;
    final List<RegionServerThread> rsThreads =
        UTIL1.getMiniHBaseCluster().getRegionServerThreads();
    for (RegionServerThread rs : rsThreads) {
      UTIL1.getAdmin().rollWALWriter(rs.getRegionServer().getServerName());
    }
    // Wait for  all log roll to finish
    UTIL1.waitFor(3000, new Waiter.ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        for (RegionServerThread rs : rsThreads) {
          if (!rs.getRegionServer().walRollRequestFinished()) {
            return false;
          }
        }
        return true;
      }

      @Override
      public String explainFailure() throws Exception {
        List<String> logRollInProgressRsList = new ArrayList<>();
        for (RegionServerThread rs : rsThreads) {
          if (!rs.getRegionServer().walRollRequestFinished()) {
            logRollInProgressRsList.add(rs.getRegionServer().toString());
          }
        }
        return "Still waiting for log roll on regionservers: " + logRollInProgressRsList;
      }
    });
  }

  @Test
  public void testCustomReplicationEndpoint() throws Exception {
    // test installing a custom replication endpoint other than the default one.
    admin.addPeer("testCustomReplicationEndpoint",
        new ReplicationPeerConfig().setClusterKey(ZKConfig.getZooKeeperClusterKey(CONF1))
            .setReplicationEndpointImpl(ReplicationEndpointForTest.class.getName()), null);

    // check whether the class has been constructed and started
    Waiter.waitFor(CONF1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ReplicationEndpointForTest.contructedCount.get() >= numRegionServers;
      }
    });

    Waiter.waitFor(CONF1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ReplicationEndpointForTest.startedCount.get() >= numRegionServers;
      }
    });

    Assert.assertEquals(0, ReplicationEndpointForTest.replicateCount.get());

    // now replicate some data.
    doPut(Bytes.toBytes("row42"));

    Waiter.waitFor(CONF1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ReplicationEndpointForTest.replicateCount.get() >= 1;
      }
    });

    doAssert(Bytes.toBytes("row42"));

    admin.removePeer("testCustomReplicationEndpoint");
  }

  @Test
  public void testReplicationEndpointReturnsFalseOnReplicate() throws Exception {
    Assert.assertEquals(0, ReplicationEndpointForTest.replicateCount.get());
    Assert.assertTrue(!ReplicationEndpointReturningFalse.replicated.get());
    int peerCount = admin.getPeersCount();
    final String id = "testReplicationEndpointReturnsFalseOnReplicate";
    admin.addPeer(id,
      new ReplicationPeerConfig().setClusterKey(ZKConfig.getZooKeeperClusterKey(CONF1))
        .setReplicationEndpointImpl(ReplicationEndpointReturningFalse.class.getName()), null);
    // This test is flakey and then there is so much stuff flying around in here its, hard to
    // debug.  Peer needs to be up for the edit to make it across. This wait on
    // peer count seems to be a hack that has us not progress till peer is up.
    if (admin.getPeersCount() <= peerCount) {
      LOG.info("Waiting on peercount to go up from " + peerCount);
      Threads.sleep(100);
    }
    // now replicate some data
    doPut(row);

    Waiter.waitFor(CONF1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        // Looks like replication endpoint returns false unless we put more than 10 edits. We
        // only send over one edit.
        int count = ReplicationEndpointForTest.replicateCount.get();
        LOG.info("count=" + count);
        return ReplicationEndpointReturningFalse.replicated.get();
      }
    });
    if (ReplicationEndpointReturningFalse.ex.get() != null) {
      throw ReplicationEndpointReturningFalse.ex.get();
    }

    admin.removePeer("testReplicationEndpointReturnsFalseOnReplicate");
  }

  @Test
  public void testInterClusterReplication() throws Exception {
    final String id = "testInterClusterReplication";

    List<HRegion> regions = UTIL1.getHBaseCluster().getRegions(tableName);
    // This trick of waiting on peer to show up is taken from test above.
    int peerCount = admin.getPeersCount();
    admin.addPeer(id,
        new ReplicationPeerConfig().setClusterKey(ZKConfig.getZooKeeperClusterKey(CONF2))
            .setReplicationEndpointImpl(InterClusterReplicationEndpointForTest.class.getName()),
        null);
    // This test is flakey and then there is so much stuff flying around in here its, hard to
    // debug.  Peer needs to be up for the edit to make it across. This wait on
    // peer count seems to be a hack that has us not progress till peer is up.
    if (admin.getPeersCount() <= peerCount) {
      LOG.info("Waiting on peercount to go up from " + peerCount);
      Threads.sleep(100);
    }

    int totEdits = 0;

    // Make sure edits are spread across regions because we do region based batching
    // before shipping edits.
    for (HRegion region: regions) {
      RegionInfo hri = region.getRegionInfo();
      byte[] row = hri.getStartKey();
      for (int i = 0; i < 100; i++) {
        if (row.length > 0) {
          Put put = new Put(row);
          put.addColumn(famName, row, row);
          region.put(put);
          totEdits++;
        }
      }
    }

    final int numEdits = totEdits;
    LOG.info("Waiting on replication of {}", numEdits);
    Waiter.waitFor(CONF1, 30000, new Waiter.ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return InterClusterReplicationEndpointForTest.replicateCount.get() == numEdits;
      }

      @Override
      public String explainFailure() throws Exception {
        String failure = "Failed to replicate all edits, expected = " + numEdits
            + " replicated = " + InterClusterReplicationEndpointForTest.replicateCount.get();
        return failure;
      }
    });

    admin.removePeer("testInterClusterReplication");
    UTIL1.deleteTableData(tableName);
  }

  @Test
  public void testWALEntryFilterFromReplicationEndpoint() throws Exception {
    ReplicationPeerConfig rpc =
      new ReplicationPeerConfig().setClusterKey(ZKConfig.getZooKeeperClusterKey(CONF1))
        .setReplicationEndpointImpl(ReplicationEndpointWithWALEntryFilter.class.getName());
    // test that we can create mutliple WALFilters reflectively
    rpc.getConfiguration().put(BaseReplicationEndpoint.REPLICATION_WALENTRYFILTER_CONFIG_KEY,
      EverythingPassesWALEntryFilter.class.getName() + "," +
        EverythingPassesWALEntryFilterSubclass.class.getName());
    admin.addPeer("testWALEntryFilterFromReplicationEndpoint", rpc);
    // now replicate some data.
    try (Connection connection = ConnectionFactory.createConnection(CONF1)) {
      doPut(connection, Bytes.toBytes("row1"));
      doPut(connection, row);
      doPut(connection, Bytes.toBytes("row2"));
    }

    Waiter.waitFor(CONF1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ReplicationEndpointForTest.replicateCount.get() >= 1;
      }
    });

    Assert.assertNull(ReplicationEndpointWithWALEntryFilter.ex.get());
    //make sure our reflectively created filter is in the filter chain
    Assert.assertTrue(EverythingPassesWALEntryFilter.hasPassedAnEntry());
    admin.removePeer("testWALEntryFilterFromReplicationEndpoint");
  }

  @Test(expected = IOException.class)
  public void testWALEntryFilterAddValidation() throws Exception {
    ReplicationPeerConfig rpc =
      new ReplicationPeerConfig().setClusterKey(ZKConfig.getZooKeeperClusterKey(CONF1))
        .setReplicationEndpointImpl(ReplicationEndpointWithWALEntryFilter.class.getName());
    // test that we can create mutliple WALFilters reflectively
    rpc.getConfiguration().put(BaseReplicationEndpoint.REPLICATION_WALENTRYFILTER_CONFIG_KEY,
      "IAmNotARealWalEntryFilter");
    admin.addPeer("testWALEntryFilterAddValidation", rpc);
  }

  @Test(expected = IOException.class)
  public void testWALEntryFilterUpdateValidation() throws Exception {
    ReplicationPeerConfig rpc =
      new ReplicationPeerConfig().setClusterKey(ZKConfig.getZooKeeperClusterKey(CONF1))
        .setReplicationEndpointImpl(ReplicationEndpointWithWALEntryFilter.class.getName());
    // test that we can create mutliple WALFilters reflectively
    rpc.getConfiguration().put(BaseReplicationEndpoint.REPLICATION_WALENTRYFILTER_CONFIG_KEY,
      "IAmNotARealWalEntryFilter");
    admin.updatePeerConfig("testWALEntryFilterUpdateValidation", rpc);
  }

  @Test
  public void testMetricsSourceBaseSourcePassThrough() {
    /*
     * The replication MetricsSource wraps a MetricsReplicationTableSourceImpl,
     * MetricsReplicationSourceSourceImpl and a MetricsReplicationGlobalSourceSource,
     * so that metrics get written to both namespaces. Both of those classes wrap a
     * MetricsReplicationSourceImpl that implements BaseSource, which allows
     * for custom JMX metrics. This test checks to make sure the BaseSource decorator logic on
     * MetricsSource actually calls down through the two layers of wrapping to the actual
     * BaseSource.
     */
    String id = "id";
    DynamicMetricsRegistry mockRegistry = mock(DynamicMetricsRegistry.class);
    MetricsReplicationSourceImpl singleRms = mock(MetricsReplicationSourceImpl.class);
    when(singleRms.getMetricsRegistry()).thenReturn(mockRegistry);
    MetricsReplicationSourceImpl globalRms = mock(MetricsReplicationSourceImpl.class);
    when(globalRms.getMetricsRegistry()).thenReturn(mockRegistry);

    MetricsReplicationSourceSource singleSourceSource =
      new MetricsReplicationSourceSourceImpl(singleRms, id);
    MetricsReplicationGlobalSourceSource globalSourceSource =
      new MetricsReplicationGlobalSourceSourceImpl(globalRms);
    MetricsReplicationGlobalSourceSource spyglobalSourceSource = spy(globalSourceSource);
    doNothing().when(spyglobalSourceSource).incrFailedRecoveryQueue();

    Map<String, MetricsReplicationTableSource> singleSourceSourceByTable =
      new HashMap<>();
    MetricsSource source = new MetricsSource(id, singleSourceSource,
      spyglobalSourceSource, singleSourceSourceByTable);


    String gaugeName = "gauge";
    String singleGaugeName = "source.id." + gaugeName;
    String globalGaugeName = "source." + gaugeName;
    long delta = 1;
    String counterName = "counter";
    String singleCounterName = "source.id." + counterName;
    String globalCounterName = "source." + counterName;
    long count = 2;
    source.decGauge(gaugeName, delta);
    source.getMetricsContext();
    source.getMetricsDescription();
    source.getMetricsJmxContext();
    source.getMetricsName();
    source.incCounters(counterName, count);
    source.incGauge(gaugeName, delta);
    source.init();
    source.removeMetric(gaugeName);
    source.setGauge(gaugeName, delta);
    source.updateHistogram(counterName, count);
    source.incrFailedRecoveryQueue();


    verify(singleRms).decGauge(singleGaugeName, delta);
    verify(globalRms).decGauge(globalGaugeName, delta);
    verify(globalRms).getMetricsContext();
    verify(globalRms).getMetricsJmxContext();
    verify(globalRms).getMetricsName();
    verify(singleRms).incCounters(singleCounterName, count);
    verify(globalRms).incCounters(globalCounterName, count);
    verify(singleRms).incGauge(singleGaugeName, delta);
    verify(globalRms).incGauge(globalGaugeName, delta);
    verify(globalRms).init();
    verify(singleRms).removeMetric(singleGaugeName);
    verify(globalRms).removeMetric(globalGaugeName);
    verify(singleRms).setGauge(singleGaugeName, delta);
    verify(globalRms).setGauge(globalGaugeName, delta);
    verify(singleRms).updateHistogram(singleCounterName, count);
    verify(globalRms).updateHistogram(globalCounterName, count);
    verify(spyglobalSourceSource).incrFailedRecoveryQueue();

    //check singleSourceSourceByTable metrics.
    // singleSourceSourceByTable map entry will be created only
    // after calling #setAgeOfLastShippedOpByTable
    boolean containsRandomNewTable = source.getSingleSourceSourceByTable()
        .containsKey("RandomNewTable");
    Assert.assertEquals(false, containsRandomNewTable);
    source.updateTableLevelMetrics(createWALEntriesWithSize("RandomNewTable"));
    containsRandomNewTable = source.getSingleSourceSourceByTable()
        .containsKey("RandomNewTable");
    Assert.assertEquals(true, containsRandomNewTable);
    MetricsReplicationTableSource msr = source.getSingleSourceSourceByTable()
        .get("RandomNewTable");

    // age should be greater than zero we created the entry with time in the past
    Assert.assertTrue(msr.getLastShippedAge() > 0);
    Assert.assertTrue(msr.getShippedBytes() > 0);

  }

  private List<Pair<Entry, Long>> createWALEntriesWithSize(String tableName) {
    List<Pair<Entry, Long>> walEntriesWithSize = new ArrayList<>();
    byte[] a = new byte[] { 'a' };
    Entry entry = createEntry(tableName, null, a);
    walEntriesWithSize.add(new Pair<>(entry, 10L));
    return walEntriesWithSize;
  }

  private Entry createEntry(String tableName, TreeMap<byte[], Integer> scopes, byte[]... kvs) {
    WALKeyImpl key1 = new WALKeyImpl(new byte[0], TableName.valueOf(tableName),
        System.currentTimeMillis() - 1L,
        scopes);
    WALEdit edit1 = new WALEdit();

    for (byte[] kv : kvs) {
      edit1.add(new KeyValue(kv, kv, kv));
    }
    return new Entry(key1, edit1);
  }

  private void doPut(byte[] row) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(CONF1)) {
      doPut(connection, row);
    }
  }

  private void doPut(final Connection connection, final byte [] row) throws IOException {
    try (Table t = connection.getTable(tableName)) {
      Put put = new Put(row);
      put.addColumn(famName, row, row);
      t.put(put);
    }
  }

  private static void doAssert(byte[] row) throws Exception {
    if (ReplicationEndpointForTest.lastEntries == null) {
      return; // first call
    }
    Assert.assertEquals(1, ReplicationEndpointForTest.lastEntries.size());
    List<Cell> cells = ReplicationEndpointForTest.lastEntries.get(0).getEdit().getCells();
    Assert.assertEquals(1, cells.size());
    Assert.assertTrue(Bytes.equals(cells.get(0).getRowArray(), cells.get(0).getRowOffset(),
      cells.get(0).getRowLength(), row, 0, row.length));
  }

  public static class ReplicationEndpointForTest extends BaseReplicationEndpoint {
    static UUID uuid = UTIL1.getRandomUUID();
    static AtomicInteger contructedCount = new AtomicInteger();
    static AtomicInteger startedCount = new AtomicInteger();
    static AtomicInteger stoppedCount = new AtomicInteger();
    static AtomicInteger replicateCount = new AtomicInteger();
    static volatile List<Entry> lastEntries = null;

    public ReplicationEndpointForTest() {
      replicateCount.set(0);
      contructedCount.incrementAndGet();
    }

    @Override
    public UUID getPeerUUID() {
      return uuid;
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      replicateCount.incrementAndGet();
      lastEntries = new ArrayList<>(replicateContext.entries);
      return true;
    }

    @Override
    public void start() {
      startAsync();
    }

    @Override
    public void stop() {
      stopAsync();
    }

    @Override
    protected void doStart() {
      startedCount.incrementAndGet();
      notifyStarted();
    }

    @Override
    protected void doStop() {
      stoppedCount.incrementAndGet();
      notifyStopped();
    }

    @Override
    public boolean canReplicateToSameCluster() {
      return true;
    }
  }

  /**
   * Not used by unit tests, helpful for manual testing with replication.
   * <p>
   * Snippet for `hbase shell`:
   * <pre>
   * create 't', 'f'
   * add_peer '1', ENDPOINT_CLASSNAME =&gt; 'org.apache.hadoop.hbase.replication.' + \
   *    'TestReplicationEndpoint$SleepingReplicationEndpointForTest'
   * alter 't', {NAME=&gt;'f', REPLICATION_SCOPE=&gt;1}
   * </pre>
   */
  public static class SleepingReplicationEndpointForTest extends ReplicationEndpointForTest {
    private long duration;
    public SleepingReplicationEndpointForTest() {
      super();
    }

    @Override
    public void init(Context context) throws IOException {
      super.init(context);
      if (this.ctx != null) {
        duration = this.ctx.getConfiguration().getLong(
            "hbase.test.sleep.replication.endpoint.duration.millis", 5000L);
      }
    }

    @Override
    public boolean replicate(ReplicateContext context) {
      try {
        Thread.sleep(duration);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      return super.replicate(context);
    }
  }

  public static class InterClusterReplicationEndpointForTest
      extends HBaseInterClusterReplicationEndpoint {

    static AtomicInteger replicateCount = new AtomicInteger();
    static boolean failedOnce;

    public InterClusterReplicationEndpointForTest() {
      replicateCount.set(0);
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      boolean success = super.replicate(replicateContext);
      if (success) {
        replicateCount.addAndGet(replicateContext.entries.size());
      }
      return success;
    }

    @Override
    protected Callable<Integer> createReplicator(List<Entry> entries, int ordinal, int timeout) {
      // Fail only once, we don't want to slow down the test.
      if (failedOnce) {
        return () -> ordinal;
      } else {
        failedOnce = true;
        return () -> {
          throw new IOException("Sample Exception: Failed to replicate.");
        };
      }
    }
  }

  public static class ReplicationEndpointReturningFalse extends ReplicationEndpointForTest {
    static int COUNT = 10;
    static AtomicReference<Exception> ex = new AtomicReference<>(null);
    static AtomicBoolean replicated = new AtomicBoolean(false);
    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      try {
        // check row
        doAssert(row);
      } catch (Exception e) {
        ex.set(e);
      }

      super.replicate(replicateContext);
      LOG.info("Replicated " + Bytes.toString(row) + ", count=" + replicateCount.get());

      replicated.set(replicateCount.get() > COUNT); // first 10 times, we return false
      return replicated.get();
    }
  }

  // return a WALEntry filter which only accepts "row", but not other rows
  public static class ReplicationEndpointWithWALEntryFilter extends ReplicationEndpointForTest {
    static AtomicReference<Exception> ex = new AtomicReference<>(null);

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      try {
        super.replicate(replicateContext);
        doAssert(row);
      } catch (Exception e) {
        ex.set(e);
      }
      return true;
    }

    @Override
    public WALEntryFilter getWALEntryfilter() {
      return new ChainWALEntryFilter(super.getWALEntryfilter(), new WALEntryFilter() {
        @Override
        public Entry filter(Entry entry) {
          ArrayList<Cell> cells = entry.getEdit().getCells();
          int size = cells.size();
          for (int i = size-1; i >= 0; i--) {
            Cell cell = cells.get(i);
            if (!Bytes.equals(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
              row, 0, row.length)) {
              cells.remove(i);
            }
          }
          return entry;
        }
      });
    }
  }

  public static class EverythingPassesWALEntryFilter implements WALEntryFilter {
    private static boolean passedEntry = false;
    @Override
    public Entry filter(Entry entry) {
      passedEntry = true;
      return entry;
    }

    public static boolean hasPassedAnEntry() {
      return passedEntry;
    }
  }

  public static class EverythingPassesWALEntryFilterSubclass
      extends EverythingPassesWALEntryFilter {
  }
}
