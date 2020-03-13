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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster.closeRegion;
import static org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster.openRegion;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint.ReplicateContext;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Tests RegionReplicaReplicationEndpoint. Unlike TestRegionReplicaReplicationEndpoint this
 * class contains lower level tests using callables.
 */
@Category({ReplicationTests.class, MediumTests.class})
public class TestRegionReplicaReplicationEndpointNoMaster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicaReplicationEndpointNoMaster.class);

  private static final int NB_SERVERS = 2;
  private static TableName tableName = TableName.valueOf(
    TestRegionReplicaReplicationEndpointNoMaster.class.getSimpleName());
  private static Table table;
  private static final byte[] row = Bytes.toBytes("TestRegionReplicaReplicator");

  private static HRegionServer rs0;
  private static HRegionServer rs1;

  private static RegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY, false);

    // install WALObserver coprocessor for tests
    String walCoprocs = HTU.getConfiguration().get(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY);
    if (walCoprocs == null) {
      walCoprocs = WALEditCopro.class.getName();
    } else {
      walCoprocs += "," + WALEditCopro.class.getName();
    }
    HTU.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
      walCoprocs);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numAlwaysStandByMasters(1).
        numRegionServers(NB_SERVERS).numDataNodes(NB_SERVERS).build();
    HTU.startMiniCluster(option);

    // Create table then get the single region for our new table.
    HTableDescriptor htd = HTU.createTableDescriptor(TableName.valueOf(tableName.getNameAsString()),
      HColumnDescriptor.DEFAULT_MIN_VERSIONS, 3, HConstants.FOREVER,
      HColumnDescriptor.DEFAULT_KEEP_DELETED);
    table = HTU.createTable(htd, new byte[][]{f}, null);

    try (RegionLocator locator = HTU.getConnection().getRegionLocator(tableName)) {
      hriPrimary = locator.getRegionLocation(row, false).getRegion();
    }

    // mock a secondary region info to open
    hriSecondary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 1);

    // No master
    TestRegionServerNoMaster.stopMasterAndAssignMeta(HTU);
    rs0 = HTU.getMiniHBaseCluster().getRegionServer(0);
    rs1 = HTU.getMiniHBaseCluster().getRegionServer(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    table.close();
    HTU.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception{
    entries.clear();
  }

  @After
  public void after() throws Exception {
  }

  static ConcurrentLinkedQueue<Entry> entries = new ConcurrentLinkedQueue<>();

  public static class WALEditCopro implements WALCoprocessor, WALObserver {
    public WALEditCopro() {
      entries.clear();
    }

    @Override
    public Optional<WALObserver> getWALObserver() {
      return Optional.of(this);
    }

    @Override
    public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
                             RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
      // only keep primary region's edits
      if (logKey.getTableName().equals(tableName) && info.getReplicaId() == 0) {
        // Presume type is a WALKeyImpl
        entries.add(new Entry((WALKeyImpl)logKey, logEdit));
      }
    }
  }

  @Test
  public void testReplayCallable() throws Exception {
    // tests replaying the edits to a secondary region replica using the Callable directly
    openRegion(HTU, rs0, hriSecondary);

    // load some data to primary
    HTU.loadNumericRows(table, f, 0, 1000);

    Assert.assertEquals(1000, entries.size());
    try (AsyncClusterConnection conn = ClusterConnectionFactory
      .createAsyncClusterConnection(HTU.getConfiguration(), null, User.getCurrent())) {
      // replay the edits to the secondary using replay callable
      replicateUsingCallable(conn, entries);
    }

    Region region = rs0.getRegion(hriSecondary.getEncodedName());
    HTU.verifyNumericRows(region, f, 0, 1000);

    HTU.deleteNumericRows(table, f, 0, 1000);
    closeRegion(HTU, rs0, hriSecondary);
  }

  private void replicateUsingCallable(AsyncClusterConnection connection, Queue<Entry> entries)
      throws IOException, ExecutionException, InterruptedException {
    Entry entry;
    while ((entry = entries.poll()) != null) {
      byte[] row = CellUtil.cloneRow(entry.getEdit().getCells().get(0));
      RegionLocations locations = connection.getRegionLocations(tableName, row, true).get();
      connection
        .replay(tableName, locations.getRegionLocation(1).getRegion().getEncodedNameAsBytes(), row,
          Collections.singletonList(entry), 1, Integer.MAX_VALUE, TimeUnit.SECONDS.toNanos(10))
        .get();
    }
  }

  @Test
  public void testReplayCallableWithRegionMove() throws Exception {
    // tests replaying the edits to a secondary region replica using the Callable directly while
    // the region is moved to another location.It tests handling of RME.
    try (AsyncClusterConnection conn = ClusterConnectionFactory
      .createAsyncClusterConnection(HTU.getConfiguration(), null, User.getCurrent())) {
      openRegion(HTU, rs0, hriSecondary);
      // load some data to primary
      HTU.loadNumericRows(table, f, 0, 1000);

      Assert.assertEquals(1000, entries.size());

      // replay the edits to the secondary using replay callable
      replicateUsingCallable(conn, entries);

      Region region = rs0.getRegion(hriSecondary.getEncodedName());
      HTU.verifyNumericRows(region, f, 0, 1000);

      HTU.loadNumericRows(table, f, 1000, 2000); // load some more data to primary

      // move the secondary region from RS0 to RS1
      closeRegion(HTU, rs0, hriSecondary);
      openRegion(HTU, rs1, hriSecondary);

      // replicate the new data
      replicateUsingCallable(conn, entries);

      region = rs1.getRegion(hriSecondary.getEncodedName());
      // verify the new data. old data may or may not be there
      HTU.verifyNumericRows(region, f, 1000, 2000);

      HTU.deleteNumericRows(table, f, 0, 2000);
      closeRegion(HTU, rs1, hriSecondary);
    }
  }

  @Test
  public void testRegionReplicaReplicationEndpointReplicate() throws Exception {
    // tests replaying the edits to a secondary region replica using the RRRE.replicate()
    openRegion(HTU, rs0, hriSecondary);
    RegionReplicaReplicationEndpoint replicator = new RegionReplicaReplicationEndpoint();

    ReplicationEndpoint.Context context = mock(ReplicationEndpoint.Context.class);
    when(context.getConfiguration()).thenReturn(HTU.getConfiguration());
    when(context.getMetrics()).thenReturn(mock(MetricsSource.class));
    when(context.getServer()).thenReturn(rs0);
    when(context.getTableDescriptors()).thenReturn(rs0.getTableDescriptors());
    replicator.init(context);
    replicator.startAsync();

    //load some data to primary
    HTU.loadNumericRows(table, f, 0, 1000);

    Assert.assertEquals(1000, entries.size());
    // replay the edits to the secondary using replay callable
    final String fakeWalGroupId = "fakeWALGroup";
    replicator.replicate(new ReplicateContext().setEntries(Lists.newArrayList(entries))
        .setWalGroupId(fakeWalGroupId));
    replicator.stop();
    Region region = rs0.getRegion(hriSecondary.getEncodedName());
    HTU.verifyNumericRows(region, f, 0, 1000);

    HTU.deleteNumericRows(table, f, 0, 1000);
    closeRegion(HTU, rs0, hriSecondary);
  }
}
