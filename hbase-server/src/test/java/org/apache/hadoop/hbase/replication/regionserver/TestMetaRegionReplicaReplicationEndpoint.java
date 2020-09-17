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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests RegionReplicaReplicationEndpoint class for hbase:meta by setting up region replicas and
 * verifying async wal replication replays the edits to the secondary region in various scenarios.
 * @see TestRegionReplicaReplicationEndpoint
 */
@Category({FlakeyTests.class, LargeTests.class})
public class TestMetaRegionReplicaReplicationEndpoint {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaRegionReplicaReplicationEndpoint.class);
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMetaRegionReplicaReplicationEndpoint.class);
  private static final int NB_SERVERS = 3;
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
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5); // less number of retries needed
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);
    // Enable hbase:meta replication.
    conf.setBoolean(ServerRegionReplicaUtil.META_REGION_REPLICA_REPLICATION_CONF_KEY, true);
    // Set hbase:meta replicas to be 3.
    conf.setInt(HConstants.META_REPLICAS_NUM, NB_SERVERS);
    HTU.startMiniCluster(NB_SERVERS);
    HTU.waitFor(30000,
      () -> HTU.getMiniHBaseCluster().getRegions(TableName.META_TABLE_NAME).size() >= NB_SERVERS);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testSpecialMetaReplicationPeerCreated() throws IOException, InterruptedException {
    MiniHBaseCluster cluster = HTU.getMiniHBaseCluster();
    HRegionServer hrs = cluster.getRegionServer(cluster.getServerHoldingMeta());
    assertTrue(isMetaRegionReplicaReplicationSource(hrs));
    // Now move the hbase:meta and make sure the peer is disabled on original server and enabled on
    // the new server.
    HRegionServer hrsOther = null;
    for (int i = 0; i < cluster.getNumLiveRegionServers(); i++) {
      hrsOther = cluster.getRegionServer(i);
      if (hrsOther.getServerName().equals(hrs.getServerName())) {
        hrsOther = null;
        continue;
      }
      break;
    }
    assertNotNull(hrsOther);
    Region meta = null;
    for (Region region: hrs.getOnlineRegionsLocalContext()) {
      if (region.getRegionInfo().isMetaRegion()) {
        meta = region;
        break;
      }
    }
    assertNotNull(meta);
    HTU.moveRegionAndWait(meta.getRegionInfo(), hrsOther.getServerName());
    assertFalse(isMetaRegionReplicaReplicationSource(hrs));
    assertTrue(isMetaRegionReplicaReplicationSource(hrsOther));
  }

  /**
   * @return Whether the special meta region replica peer is enabled on <code>hrs</code>
   */
  private boolean isMetaRegionReplicaReplicationSource(HRegionServer hrs) {
    boolean on = false;
    for (ReplicationSourceInterface rsi:
      hrs.getReplicationSourceService().getReplicationManager().getSources()) {
      on |= ReplicationSourceManager.META_REGION_REPLICA_REPLICATION_SOURCE.equals(rsi.getPeerId());
    }
    return on;
  }

  /**
   * Test meta region replica replication. Create some tables and see if replicas pick up the
   * additions.
   */
  @Test
  public void testHBaseMetaReplicates() throws Exception {
    HTU.createTable(TableName.valueOf(this.name.getMethodName() + "_0"), HConstants.CATALOG_FAMILY,
      Arrays.copyOfRange(HBaseTestingUtility.KEYS, 1, HBaseTestingUtility.KEYS.length));
    verifyReplication(TableName.META_TABLE_NAME, NB_SERVERS, HTU.KEYS);
  }

  /**
   * Test meta region replica replication. Create some tables and see if replicas pick up the
   * additions.
   */
  @Test
  public void testHBaseMetaReplicatesOneRow() throws Exception {
    HTU.createTable(TableName.valueOf(this.name.getMethodName()), HConstants.CATALOG_FAMILY);
    verifyReplication(TableName.META_TABLE_NAME, NB_SERVERS,
      new byte [][] {HTU.getMetaTableRows().get(0)});
  }

  private void verifyReplication(TableName tableName, int regionReplication, final byte [][] rows) {
    final Region[] regions = new Region[regionReplication];
    for (int i = 0; i < NB_SERVERS; i++) {
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
          LOG.info("Verifying replication for region replica {}", region.getRegionInfo());
          int count = 0;
          try (RegionScanner rs = region.getScanner(new Scan())) {
            List<Cell> cells = new ArrayList<>();
            while (rs.next(cells)) {
              cells.clear();
              count++;
            }
            return count == rows.length;
          } catch(Throwable ex) {
            LOG.warn("Verification from secondary region is not complete yet", ex);
            // still wait
            return false;
          }
        }
      });
    }
  }

//  @Test
//  public void testRegionReplicaReplicationWith2Replicas() throws Exception {
//    testRegionReplicaReplication(2);
//  }
//
//  @Test
//  public void testRegionReplicaReplicationWith3Replicas() throws Exception {
//    testRegionReplicaReplication(3);
//  }
//
//  @Test
//  public void testRegionReplicaReplicationWith10Replicas() throws Exception {
//    testRegionReplicaReplication(10);
//  }
//
//  @Test
//  public void testRegionReplicaReplicationForFlushAndCompaction() throws Exception {
//    // Tests a table with region replication 3. Writes some data, and causes flushes and
//    // compactions. Verifies that the data is readable from the replicas. Note that this
//    // does not test whether the replicas actually pick up flushed files and apply compaction
//    // to their stores
//    int regionReplication = 3;
//    final TableName tableName = TableName.valueOf(name.getMethodName());
//    HTableDescriptor htd = HTU.createTableDescriptor(tableName);
//    htd.setRegionReplication(regionReplication);
//    HTU.getAdmin().createTable(htd);
//
//
//    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
//    Table table = connection.getTable(tableName);
//    try {
//      // load the data to the table
//
//      for (int i = 0; i < 6000; i += 1000) {
//        LOG.info("Writing data from " + i + " to " + (i+1000));
//        HTU.loadNumericRows(table, HBaseTestingUtility.fam1, i, i+1000);
//        LOG.info("flushing table");
//        HTU.flush(tableName);
//        LOG.info("compacting table");
//        HTU.compact(tableName, false);
//      }
//
//      verifyReplication(tableName, regionReplication, 0, 1000);
//    } finally {
//      table.close();
//      connection.close();
//    }
//  }
//
//  @Test
//  public void testRegionReplicaReplicationIgnoresDisabledTables() throws Exception {
//    testRegionReplicaReplicationIgnores(false, false);
//  }
//
//  @Test
//  public void testRegionReplicaReplicationIgnoresDroppedTables() throws Exception {
//    testRegionReplicaReplicationIgnores(true, false);
//  }
//
//  @Test
//  public void testRegionReplicaReplicationIgnoresNonReplicatedTables() throws Exception {
//    testRegionReplicaReplicationIgnores(false, true);
//  }
//
//  public void testRegionReplicaReplicationIgnores(boolean dropTable, boolean disableReplication)
//      throws Exception {
//
//    // tests having edits from a disabled or dropped table is handled correctly by skipping those
//    // entries and further edits after the edits from dropped/disabled table can be replicated
//    // without problems.
//    final TableName tableName = TableName.valueOf(
//      name.getMethodName() + "_drop_" + dropTable + "_disabledReplication_" + disableReplication);
//    HTableDescriptor htd = HTU.createTableDescriptor(tableName);
//    int regionReplication = 3;
//    htd.setRegionReplication(regionReplication);
//    HTU.deleteTableIfAny(tableName);
//
//    HTU.getAdmin().createTable(htd);
//    TableName toBeDisabledTable = TableName.valueOf(
//      dropTable ? "droppedTable" : (disableReplication ? "disableReplication" : "disabledTable"));
//    HTU.deleteTableIfAny(toBeDisabledTable);
//    htd = HTU.createTableDescriptor(toBeDisabledTable.toString());
//    htd.setRegionReplication(regionReplication);
//    HTU.getAdmin().createTable(htd);
//
//    // both tables are created, now pause replication
//    ReplicationAdmin admin = new ReplicationAdmin(HTU.getConfiguration());
//    admin.disablePeer(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER);
//
//    // now that the replication is disabled, write to the table to be dropped, then drop the table.
//
//    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
//    Table table = connection.getTable(tableName);
//    Table tableToBeDisabled = connection.getTable(toBeDisabledTable);
//
//    HTU.loadNumericRows(tableToBeDisabled, HBaseTestingUtility.fam1, 6000, 7000);
//
//    AtomicLong skippedEdits = new AtomicLong();
//    RegionReplicaReplicationEndpoint.RegionReplicaOutputSink sink =
//        mock(RegionReplicaReplicationEndpoint.RegionReplicaOutputSink.class);
//    when(sink.getSkippedEditsCounter()).thenReturn(skippedEdits);
//    FSTableDescriptors fstd =
//        new FSTableDescriptors(FileSystem.get(HTU.getConfiguration()), HTU.getDefaultRootDirPath());
//    RegionReplicaReplicationEndpoint.RegionReplicaSinkWriter sinkWriter =
//        new RegionReplicaReplicationEndpoint.RegionReplicaSinkWriter(sink,
//            (ClusterConnection) connection, Executors.newSingleThreadExecutor(), Integer.MAX_VALUE,
//            fstd);
//    RegionLocator rl = connection.getRegionLocator(toBeDisabledTable);
//    HRegionLocation hrl = rl.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY);
//    byte[] encodedRegionName = hrl.getRegionInfo().getEncodedNameAsBytes();
//
//    Cell cell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes("A"))
//        .setFamily(HTU.fam1).setValue(Bytes.toBytes("VAL")).setType(Type.Put).build();
//    Entry entry = new Entry(
//      new WALKeyImpl(encodedRegionName, toBeDisabledTable, 1),
//        new WALEdit()
//            .add(cell));
//
//    HTU.getAdmin().disableTable(toBeDisabledTable); // disable the table
//    if (dropTable) {
//      HTU.getAdmin().deleteTable(toBeDisabledTable);
//    } else if (disableReplication) {
//      htd.setRegionReplication(regionReplication - 2);
//      HTU.getAdmin().modifyTable(toBeDisabledTable, htd);
//      HTU.getAdmin().enableTable(toBeDisabledTable);
//    }
//    sinkWriter.append(toBeDisabledTable, encodedRegionName,
//      HConstants.EMPTY_BYTE_ARRAY, Lists.newArrayList(entry, entry));
//
//    assertEquals(2, skippedEdits.get());
//
//    if (disableReplication) {
//      // enable replication again so that we can verify replication
//      HTU.getAdmin().disableTable(toBeDisabledTable); // disable the table
//      htd.setRegionReplication(regionReplication);
//      HTU.getAdmin().modifyTable(toBeDisabledTable, htd);
//      HTU.getAdmin().enableTable(toBeDisabledTable);
//    }
//
//    try {
//      // load some data to the to-be-dropped table
//
//      // load the data to the table
//      HTU.loadNumericRows(table, HBaseTestingUtility.fam1, 0, 1000);
//
//      // now enable the replication
//      admin.enablePeer(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER);
//
//      verifyReplication(tableName, regionReplication, 0, 1000);
//
//    } finally {
//      admin.close();
//      table.close();
//      rl.close();
//      tableToBeDisabled.close();
//      HTU.deleteTableIfAny(toBeDisabledTable);
//      connection.close();
//    }
//  }
}
