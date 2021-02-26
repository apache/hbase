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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Tests RegionReplicaReplicationEndpoint class by setting up region replicas and verifying
 * async wal replication replays the edits to the secondary region in various scenarios.
 */
@Category({FlakeyTests.class, LargeTests.class})
public class TestRegionReplicaReplicationEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicaReplicationEndpoint.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegionReplicaReplicationEndpoint.class);

  private static final int NB_SERVERS = 2;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setFloat("hbase.regionserver.logroll.multiplier", 0.0003f);
    conf.setInt("replication.source.size.capacity", 10240);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5); // less number of retries is needed
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);

    HTU.startMiniCluster(NB_SERVERS);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testRegionReplicaReplicationPeerIsCreated() throws IOException, ReplicationException {
    // create a table with region replicas. Check whether the replication peer is created
    // and replication started.
    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
      Admin admin = connection.getAdmin()) {
      String peerId = ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER;

      ReplicationPeerConfig peerConfig = null;
      try {
        peerConfig = admin.getReplicationPeerConfig(peerId);
      } catch (ReplicationPeerNotFoundException e) {
        LOG.warn("Region replica replication peer id=" + peerId + " not exist", e);
      }

      try {
        peerConfig = admin.getReplicationPeerConfig(peerId);
      } catch (ReplicationPeerNotFoundException e) {
        LOG.warn("Region replica replication peer id=" + peerId + " not exist", e);
      }

      if (peerConfig != null) {
        admin.removeReplicationPeer(peerId);
        peerConfig = null;
      }

      HTableDescriptor htd = HTU.createTableDescriptor(
        "testReplicationPeerIsCreated_no_region_replicas");
      createOrEnableTableWithRetries(htd, true);
      try {
        peerConfig = admin.getReplicationPeerConfig(peerId);
        fail("Should throw ReplicationException, because replication peer id=" + peerId
            + " not exist");
      } catch (ReplicationPeerNotFoundException e) {
      }
      assertNull(peerConfig);

      htd = HTU.createTableDescriptor("testReplicationPeerIsCreated");
      htd.setRegionReplication(2);
      createOrEnableTableWithRetries(htd, true);

      // assert peer configuration is correct
      peerConfig = admin.getReplicationPeerConfig(peerId);
      assertNotNull(peerConfig);
      assertEquals(peerConfig.getClusterKey(), ZKConfig.getZooKeeperClusterKey(
          HTU.getConfiguration()));
      assertEquals(RegionReplicaReplicationEndpoint.class.getName(),
          peerConfig.getReplicationEndpointImpl());
    }  
  }

  @Test
  public void testRegionReplicaReplicationPeerIsCreatedForModifyTable() throws Exception {
    // modify a table by adding region replicas. Check whether the replication peer is created
    // and replication started.
    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
      Admin admin = connection.getAdmin()) {
      String peerId = ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER;
      ReplicationPeerConfig peerConfig = null;
      try {
        peerConfig = admin.getReplicationPeerConfig(peerId);
      } catch (ReplicationPeerNotFoundException e) {
        LOG.warn("Region replica replication peer id=" + peerId + " not exist", e);
      }

      if (peerConfig != null) {
        admin.removeReplicationPeer(peerId);
        peerConfig = null;
      }

      HTableDescriptor htd = HTU.createTableDescriptor("testRegionReplicaReplicationPeerIsCreatedForModifyTable");
      createOrEnableTableWithRetries(htd, true);

      // assert that replication peer is not created yet
      try {
        peerConfig = admin.getReplicationPeerConfig(peerId);
        fail("Should throw ReplicationException, because replication peer id=" + peerId
          + " not exist");
      } catch (ReplicationPeerNotFoundException e) {
      }
      assertNull(peerConfig);

      HTU.getAdmin().disableTable(htd.getTableName());
      htd.setRegionReplication(2);
      HTU.getAdmin().modifyTable(htd.getTableName(), htd);
      createOrEnableTableWithRetries(htd, false);

      // assert peer configuration is correct
      peerConfig = admin.getReplicationPeerConfig(peerId);
      assertNotNull(peerConfig);
      assertEquals(peerConfig.getClusterKey(), ZKConfig.getZooKeeperClusterKey(HTU.getConfiguration()));
      assertEquals(RegionReplicaReplicationEndpoint.class.getName(),
        peerConfig.getReplicationEndpointImpl());
      admin.close();
    }
  }

  public void testRegionReplicaReplication(int regionReplication) throws Exception {
    // test region replica replication. Create a table with single region, write some data
    // ensure that data is replicated to the secondary region
    TableName tableName = TableName.valueOf("testRegionReplicaReplicationWithReplicas_"
        + regionReplication);
    HTableDescriptor htd = HTU.createTableDescriptor(tableName.toString());
    htd.setRegionReplication(regionReplication);
    createOrEnableTableWithRetries(htd, true);
    TableName tableNameNoReplicas =
        TableName.valueOf("testRegionReplicaReplicationWithReplicas_NO_REPLICAS");
    HTU.deleteTableIfAny(tableNameNoReplicas);
    HTU.createTable(tableNameNoReplicas, HBaseTestingUtility.fam1);

    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);
    Table tableNoReplicas = connection.getTable(tableNameNoReplicas);

    try {
      // load some data to the non-replicated table
      HTU.loadNumericRows(tableNoReplicas, HBaseTestingUtility.fam1, 6000, 7000);

      // load the data to the table
      HTU.loadNumericRows(table, HBaseTestingUtility.fam1, 0, 1000);

      verifyReplication(tableName, regionReplication, 0, 1000);

    } finally {
      table.close();
      tableNoReplicas.close();
      HTU.deleteTableIfAny(tableNameNoReplicas);
      connection.close();
    }
  }

  private void verifyReplication(TableName tableName, int regionReplication,
      final int startRow, final int endRow) throws Exception {
    verifyReplication(tableName, regionReplication, startRow, endRow, true);
  }

  private void verifyReplication(TableName tableName, int regionReplication,
      final int startRow, final int endRow, final boolean present) throws Exception {
    // find the regions
    final Region[] regions = new Region[regionReplication];

    for (int i=0; i < NB_SERVERS; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getRegions(tableName);
      for (HRegion region : onlineRegions) {
        regions[region.getRegionInfo().getReplicaId()] = region;
      }
    }

    for (Region region : regions) {
      assertNotNull(region);
    }

    for (int i = 1; i < regionReplication; i++) {
      final Region region = regions[i];
      // wait until all the data is replicated to all secondary regions
      Waiter.waitFor(HTU.getConfiguration(), 90000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          LOG.info("verifying replication for region replica:" + region.getRegionInfo());
          try {
            HTU.verifyNumericRows(region, HBaseTestingUtility.fam1, startRow, endRow, present);
          } catch(Throwable ex) {
            LOG.warn("Verification from secondary region is not complete yet", ex);
            // still wait
            return false;
          }
          return true;
        }
      });
    }
  }

  @Test
  public void testRegionReplicaReplicationWith2Replicas() throws Exception {
    testRegionReplicaReplication(2);
  }

  @Test
  public void testRegionReplicaReplicationWith3Replicas() throws Exception {
    testRegionReplicaReplication(3);
  }

  @Test
  public void testRegionReplicaReplicationWith10Replicas() throws Exception {
    testRegionReplicaReplication(10);
  }

  @Test
  public void testRegionReplicaWithoutMemstoreReplication() throws Exception {
    int regionReplication = 3;
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = HTU.createTableDescriptor(tableName);
    htd.setRegionReplication(regionReplication);
    htd.setRegionMemstoreReplication(false);
    createOrEnableTableWithRetries(htd, true);

    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);
    try {
      // write data to the primary. The replicas should not receive the data
      final int STEP = 100;
      for (int i = 0; i < 3; ++i) {
        final int startRow = i * STEP;
        final int endRow = (i + 1) * STEP;
        LOG.info("Writing data from " + startRow + " to " + endRow);
        HTU.loadNumericRows(table, HBaseTestingUtility.fam1, startRow, endRow);
        verifyReplication(tableName, regionReplication, startRow, endRow, false);

        // Flush the table, now the data should show up in the replicas
        LOG.info("flushing table");
        HTU.flush(tableName);
        verifyReplication(tableName, regionReplication, 0, endRow, true);
      }
    } finally {
      table.close();
      connection.close();
    }
  }

  @Test
  public void testRegionReplicaReplicationForFlushAndCompaction() throws Exception {
    // Tests a table with region replication 3. Writes some data, and causes flushes and
    // compactions. Verifies that the data is readable from the replicas. Note that this
    // does not test whether the replicas actually pick up flushed files and apply compaction
    // to their stores
    int regionReplication = 3;
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = HTU.createTableDescriptor(tableName);
    htd.setRegionReplication(regionReplication);
    createOrEnableTableWithRetries(htd, true);


    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);
    try {
      // load the data to the table

      for (int i = 0; i < 6000; i += 1000) {
        LOG.info("Writing data from " + i + " to " + (i+1000));
        HTU.loadNumericRows(table, HBaseTestingUtility.fam1, i, i+1000);
        LOG.info("flushing table");
        HTU.flush(tableName);
        LOG.info("compacting table");
        HTU.compact(tableName, false);
      }

      verifyReplication(tableName, regionReplication, 0, 1000);
    } finally {
      table.close();
      connection.close();
    }
  }

  @Test
  public void testRegionReplicaReplicationIgnoresDisabledTables() throws Exception {
    testRegionReplicaReplicationIgnores(false, false);
  }

  @Test
  public void testRegionReplicaReplicationIgnoresDroppedTables() throws Exception {
    testRegionReplicaReplicationIgnores(true, false);
  }

  @Test
  public void testRegionReplicaReplicationIgnoresNonReplicatedTables() throws Exception {
    testRegionReplicaReplicationIgnores(false, true);
  }

  public void testRegionReplicaReplicationIgnores(boolean dropTable, boolean disableReplication)
      throws Exception {

    // tests having edits from a disabled or dropped table is handled correctly by skipping those
    // entries and further edits after the edits from dropped/disabled table can be replicated
    // without problems.
    final TableName tableName = TableName.valueOf(
      name.getMethodName() + "_drop_" + dropTable + "_disabledReplication_" + disableReplication);
    HTableDescriptor htd = HTU.createTableDescriptor(tableName);
    int regionReplication = 3;
    htd.setRegionReplication(regionReplication);
    HTU.deleteTableIfAny(tableName);

    createOrEnableTableWithRetries(htd, true);
    TableName toBeDisabledTable = TableName.valueOf(
      dropTable ? "droppedTable" : (disableReplication ? "disableReplication" : "disabledTable"));
    HTU.deleteTableIfAny(toBeDisabledTable);
    htd = HTU.createTableDescriptor(toBeDisabledTable.toString());
    htd.setRegionReplication(regionReplication);
    createOrEnableTableWithRetries(htd, true);

    // both tables are created, now pause replication
    HTU.getAdmin().disableReplicationPeer(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER);

    // now that the replication is disabled, write to the table to be dropped, then drop the table.

    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);
    Table tableToBeDisabled = connection.getTable(toBeDisabledTable);

    HTU.loadNumericRows(tableToBeDisabled, HBaseTestingUtility.fam1, 6000, 7000);

    AtomicLong skippedEdits = new AtomicLong();
    RegionReplicaReplicationEndpoint.RegionReplicaOutputSink sink =
        mock(RegionReplicaReplicationEndpoint.RegionReplicaOutputSink.class);
    when(sink.getSkippedEditsCounter()).thenReturn(skippedEdits);
    FSTableDescriptors fstd =
        new FSTableDescriptors(FileSystem.get(HTU.getConfiguration()), HTU.getDefaultRootDirPath());
    RegionReplicaReplicationEndpoint.RegionReplicaSinkWriter sinkWriter =
        new RegionReplicaReplicationEndpoint.RegionReplicaSinkWriter(sink,
            (ClusterConnection) connection, Executors.newSingleThreadExecutor(), Integer.MAX_VALUE,
            fstd);
    RegionLocator rl = connection.getRegionLocator(toBeDisabledTable);
    HRegionLocation hrl = rl.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY);
    byte[] encodedRegionName = hrl.getRegionInfo().getEncodedNameAsBytes();

    Cell cell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes("A"))
        .setFamily(HTU.fam1).setValue(Bytes.toBytes("VAL")).setType(Type.Put).build();
    Entry entry = new Entry(
      new WALKeyImpl(encodedRegionName, toBeDisabledTable, 1),
        new WALEdit()
            .add(cell));

    HTU.getAdmin().disableTable(toBeDisabledTable); // disable the table
    if (dropTable) {
      HTU.getAdmin().deleteTable(toBeDisabledTable);
    } else if (disableReplication) {
      htd.setRegionReplication(regionReplication - 2);
      HTU.getAdmin().modifyTable(toBeDisabledTable, htd);
      createOrEnableTableWithRetries(htd, false);
    }
    sinkWriter.append(toBeDisabledTable, encodedRegionName,
      HConstants.EMPTY_BYTE_ARRAY, Lists.newArrayList(entry, entry));

    assertEquals(2, skippedEdits.get());

    HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(0);
    MetricsSource metrics = mock(MetricsSource.class);
    ReplicationEndpoint.Context ctx =
      new ReplicationEndpoint.Context(HTU.getConfiguration(), HTU.getConfiguration(),
        HTU.getTestFileSystem(), ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER,
        UUID.fromString(rs.getClusterId()), rs.getReplicationSourceService().
        getReplicationManager().getReplicationPeers()
          .getPeer(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER),
        metrics, rs.getTableDescriptors(), rs);
    RegionReplicaReplicationEndpoint rrpe = new RegionReplicaReplicationEndpoint();
    rrpe.init(ctx);
    rrpe.start();
    ReplicationEndpoint.ReplicateContext repCtx = new ReplicationEndpoint.ReplicateContext();
    repCtx.setEntries(Lists.newArrayList(entry, entry));
    assertTrue(rrpe.replicate(repCtx));
    /* Come back here. There is a difference on how counting is done here and in master branch.
       St.Ack
    Mockito.verify(metrics, Mockito.times(1)).
      incrLogEditsFiltered(Mockito.eq(2L));
     */
    rrpe.stop();
    if (disableReplication) {
      // enable replication again so that we can verify replication
      HTU.getAdmin().disableTable(toBeDisabledTable); // disable the table
      htd.setRegionReplication(regionReplication);
      HTU.getAdmin().modifyTable(toBeDisabledTable, htd);
      createOrEnableTableWithRetries(htd, false);
    }

    try {
      // load some data to the to-be-dropped table

      // load the data to the table
      HTU.loadNumericRows(table, HBaseTestingUtility.fam1, 0, 1000);

      // now enable the replication
      HTU.getAdmin().enableReplicationPeer(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER);

      verifyReplication(tableName, regionReplication, 0, 1000);

    } finally {
      table.close();
      rl.close();
      tableToBeDisabled.close();
      HTU.deleteTableIfAny(toBeDisabledTable);
      connection.close();
    }
  }

  private void createOrEnableTableWithRetries(TableDescriptor htd, boolean createTableOperation) {
    // Helper function to run create/enable table operations with a retry feature
    boolean continueToRetry = true;
    int tries = 0;
    while (continueToRetry && tries < 50) {
      try {
        continueToRetry = false;
        if (createTableOperation) {
          HTU.getAdmin().createTable(htd);
        } else {
          HTU.getAdmin().enableTable(htd.getTableName());
        }
      } catch (IOException e) {
        if (e.getCause() instanceof ReplicationException) {
          continueToRetry = true;
          tries++;
          Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      }
    }
  }
}
