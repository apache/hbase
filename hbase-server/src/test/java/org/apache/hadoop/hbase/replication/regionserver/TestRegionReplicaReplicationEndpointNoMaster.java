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
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint.ReplicateContext;
import org.apache.hadoop.hbase.replication.regionserver.RegionReplicaReplicationEndpoint.RegionReplicaReplayCallable;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;

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
  private static final byte[] row = "TestRegionReplicaReplicator".getBytes();

  private static HRegionServer rs0;
  private static HRegionServer rs1;

  private static HRegionInfo hriPrimary;
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
    HTU.startMiniCluster(NB_SERVERS);

    // Create table then get the single region for our new table.
    HTableDescriptor htd = HTU.createTableDescriptor(tableName.getNameAsString());
    table = HTU.createTable(htd, new byte[][]{f}, null);

    try (RegionLocator locator = HTU.getConnection().getRegionLocator(tableName)) {
      hriPrimary = locator.getRegionLocation(row, false).getRegionInfo();
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
    ClusterConnection connection =
        (ClusterConnection) ConnectionFactory.createConnection(HTU.getConfiguration());

    //load some data to primary
    HTU.loadNumericRows(table, f, 0, 1000);

    Assert.assertEquals(1000, entries.size());
    // replay the edits to the secondary using replay callable
    replicateUsingCallable(connection, entries);

    Region region = rs0.getRegion(hriSecondary.getEncodedName());
    HTU.verifyNumericRows(region, f, 0, 1000);

    HTU.deleteNumericRows(table, f, 0, 1000);
    closeRegion(HTU, rs0, hriSecondary);
    connection.close();
  }

  private void replicateUsingCallable(ClusterConnection connection, Queue<Entry> entries)
      throws IOException, RuntimeException {
    Entry entry;
    while ((entry = entries.poll()) != null) {
      byte[] row = CellUtil.cloneRow(entry.getEdit().getCells().get(0));
      RegionLocations locations = connection.locateRegion(tableName, row, true, true);
      RegionReplicaReplayCallable callable = new RegionReplicaReplayCallable(connection,
        RpcControllerFactory.instantiate(connection.getConfiguration()),
        table.getName(), locations.getRegionLocation(1),
        locations.getRegionLocation(1).getRegionInfo(), row, Lists.newArrayList(entry),
        new AtomicLong());

      RpcRetryingCallerFactory factory = RpcRetryingCallerFactory.instantiate(
        connection.getConfiguration());
      factory.<ReplicateWALEntryResponse> newCaller().callWithRetries(callable, 10000);
    }
  }

  @Test
  public void testReplayCallableWithRegionMove() throws Exception {
    // tests replaying the edits to a secondary region replica using the Callable directly while
    // the region is moved to another location.It tests handling of RME.
    openRegion(HTU, rs0, hriSecondary);
    ClusterConnection connection =
        (ClusterConnection) ConnectionFactory.createConnection(HTU.getConfiguration());
    //load some data to primary
    HTU.loadNumericRows(table, f, 0, 1000);

    Assert.assertEquals(1000, entries.size());
    // replay the edits to the secondary using replay callable
    replicateUsingCallable(connection, entries);

    Region region = rs0.getRegion(hriSecondary.getEncodedName());
    HTU.verifyNumericRows(region, f, 0, 1000);

    HTU.loadNumericRows(table, f, 1000, 2000); // load some more data to primary

    // move the secondary region from RS0 to RS1
    closeRegion(HTU, rs0, hriSecondary);
    openRegion(HTU, rs1, hriSecondary);

    // replicate the new data
    replicateUsingCallable(connection, entries);

    region = rs1.getRegion(hriSecondary.getEncodedName());
    // verify the new data. old data may or may not be there
    HTU.verifyNumericRows(region, f, 1000, 2000);

    HTU.deleteNumericRows(table, f, 0, 2000);
    closeRegion(HTU, rs1, hriSecondary);
    connection.close();
  }

  @Test
  public void testRegionReplicaReplicationEndpointReplicate() throws Exception {
    // tests replaying the edits to a secondary region replica using the RRRE.replicate()
    openRegion(HTU, rs0, hriSecondary);
    ClusterConnection connection =
        (ClusterConnection) ConnectionFactory.createConnection(HTU.getConfiguration());
    RegionReplicaReplicationEndpoint replicator = new RegionReplicaReplicationEndpoint();

    ReplicationEndpoint.Context context = mock(ReplicationEndpoint.Context.class);
    when(context.getConfiguration()).thenReturn(HTU.getConfiguration());
    when(context.getMetrics()).thenReturn(mock(MetricsSource.class));

    replicator.init(context);
    replicator.startAsync();

    //load some data to primary
    HTU.loadNumericRows(table, f, 0, 1000);

    Assert.assertEquals(1000, entries.size());
    // replay the edits to the secondary using replay callable
    final String fakeWalGroupId = "fakeWALGroup";
    replicator.replicate(new ReplicateContext().setEntries(Lists.newArrayList(entries))
        .setWalGroupId(fakeWalGroupId));

    Region region = rs0.getRegion(hriSecondary.getEncodedName());
    HTU.verifyNumericRows(region, f, 0, 1000);

    HTU.deleteNumericRows(table, f, 0, 1000);
    closeRegion(HTU, rs0, hriSecondary);
    connection.close();
  }
}
