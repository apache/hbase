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

import static org.apache.hadoop.hbase.client.RegionLocator.LOCATOR_META_REPLICAS_MODE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.After;
import org.junit.Before;
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
 *
 * @see TestRegionReplicaReplicationEndpoint
 */
@Category({LargeTests.class})
public class TestMetaRegionReplicaReplicationEndpoint {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaRegionReplicaReplicationEndpoint.class);
  private static final Logger LOG =
    LoggerFactory.getLogger(TestMetaRegionReplicaReplicationEndpoint.class);
  private static final int NB_SERVERS = 4;
  private final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private int numOfMetaReplica = NB_SERVERS - 1;
  private static byte[] VALUE = Bytes.toBytes("value");

  @Rule
  public TestName name = new TestName();

  @Before
  public void before() throws Exception {
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
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);
    // Enable hbase:meta replication.
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CATALOG_CONF_KEY,
      true);
    // Set hbase:meta replicas to be 3.
    // conf.setInt(HConstants.META_REPLICAS_NUM, numOfMetaReplica);
    HTU.startMiniCluster(NB_SERVERS);
    // Enable hbase:meta replication.
    HBaseTestingUtility.setReplicas(HTU.getAdmin(), TableName.META_TABLE_NAME, numOfMetaReplica);

    HTU.waitFor(30000,
      () -> HTU.getMiniHBaseCluster().getRegions(TableName.META_TABLE_NAME).size()
      >= numOfMetaReplica);
  }

  @After
  public void after() throws Exception {
    HTU.shutdownMiniCluster();
  }

  /**
   * Assert that the ReplicationSource for hbase:meta gets created when hbase:meta is opened.
   */
  @Test
  public void testHBaseMetaReplicationSourceCreatedOnOpen() throws Exception {
    MiniHBaseCluster cluster = HTU.getMiniHBaseCluster();
    HRegionServer hrs = cluster.getRegionServer(cluster.getServerHoldingMeta());
    // Replicate a row to prove all working.
    testHBaseMetaReplicatesOneRow(0);
    assertTrue(isMetaRegionReplicaReplicationSource(hrs));
    // Now move the hbase:meta and make sure the ReplicationSource is in both places.
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
    assertFalse(isMetaRegionReplicaReplicationSource(hrsOther));
    Region meta = null;
    for (Region region : hrs.getOnlineRegionsLocalContext()) {
      if (region.getRegionInfo().isMetaRegion()) {
        meta = region;
        break;
      }
    }
    assertNotNull(meta);
    HTU.moveRegionAndWait(meta.getRegionInfo(), hrsOther.getServerName());
    // Assert that there is a ReplicationSource in both places now.
    assertTrue(isMetaRegionReplicaReplicationSource(hrs));
    assertTrue(isMetaRegionReplicaReplicationSource(hrsOther));
    // Replicate to show stuff still works.
    testHBaseMetaReplicatesOneRow(1);
    // Now pretend a few hours have gone by... roll the meta WAL in original location... Move the
    // meta back and retry replication. See if it works.
    hrs.getWAL(meta.getRegionInfo()).rollWriter(true);
    testHBaseMetaReplicatesOneRow(2);
    hrs.getWAL(meta.getRegionInfo()).rollWriter(true);
    testHBaseMetaReplicatesOneRow(3);
  }

  /**
   * Test meta region replica replication. Create some tables and see if replicas pick up the
   * additions.
   */
  private void testHBaseMetaReplicatesOneRow(int i) throws Exception {
    waitForMetaReplicasToOnline();
    try (Table table = HTU.createTable(TableName.valueOf(this.name.getMethodName() + "_" + i),
      HConstants.CATALOG_FAMILY)) {
      verifyReplication(TableName.META_TABLE_NAME, numOfMetaReplica, getMetaCells(table.getName()));
    }
  }

  /**
   * @return Whether the special meta region replica peer is enabled on <code>hrs</code>
   */
  private boolean isMetaRegionReplicaReplicationSource(HRegionServer hrs) {
    return hrs.getReplicationSourceService().getReplicationManager().
      catalogReplicationSource.get() != null;
  }

  /**
   * Test meta region replica replication. Create some tables and see if replicas pick up the
   * additions.
   */
  @Test
  public void testHBaseMetaReplicates() throws Exception {
    try (Table table = HTU
      .createTable(TableName.valueOf(this.name.getMethodName() + "_0"), HConstants.CATALOG_FAMILY,
        Arrays.copyOfRange(HBaseTestingUtility.KEYS, 1, HBaseTestingUtility.KEYS.length))) {
      verifyReplication(TableName.META_TABLE_NAME, numOfMetaReplica, getMetaCells(table.getName()));
    }
    try (Table table = HTU
      .createTable(TableName.valueOf(this.name.getMethodName() + "_1"), HConstants.CATALOG_FAMILY,
        Arrays.copyOfRange(HBaseTestingUtility.KEYS, 1, HBaseTestingUtility.KEYS.length))) {
      verifyReplication(TableName.META_TABLE_NAME, numOfMetaReplica, getMetaCells(table.getName()));
      // Try delete.
      HTU.deleteTableIfAny(table.getName());
      verifyDeletedReplication(TableName.META_TABLE_NAME, numOfMetaReplica, table.getName());
    }
  }

  @Test
  public void testCatalogReplicaReplicationWithFlushAndCompaction() throws Exception {
    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    TableName tableName = TableName.valueOf("hbase:meta");
    Table table = connection.getTable(tableName);
    try {
      // load the data to the table
      for (int i = 0; i < 5; i++) {
        LOG.info("Writing data from " + i * 1000 + " to " + (i * 1000 + 1000));
        HTU.loadNumericRows(table, HConstants.CATALOG_FAMILY, i * 1000, i * 1000 + 1000);
        LOG.info("flushing table");
        HTU.flush(tableName);
        LOG.info("compacting table");
        if (i < 4) {
          HTU.compact(tableName, false);
        }
      }

      verifyReplication(tableName, numOfMetaReplica, 0, 5000, HConstants.CATALOG_FAMILY);
    } finally {
      table.close();
      connection.close();
    }
  }

  @Test
  public void testCatalogReplicaReplicationWithReplicaMoved() throws Exception {
    MiniHBaseCluster cluster = HTU.getMiniHBaseCluster();
    HRegionServer hrs = cluster.getRegionServer(cluster.getServerHoldingMeta());

    HRegionServer hrsMetaReplica = null;
    HRegionServer hrsNoMetaReplica = null;
    HRegionServer server = null;
    Region metaReplica = null;
    boolean hostingMeta;

    for (int i = 0; i < cluster.getNumLiveRegionServers(); i++) {
      server = cluster.getRegionServer(i);
      hostingMeta = false;
      if (server == hrs) {
        continue;
      }
      for (Region region : server.getOnlineRegionsLocalContext()) {
        if (region.getRegionInfo().isMetaRegion()) {
          if (metaReplica == null) {
            metaReplica = region;
          }
          hostingMeta = true;
          break;
        }
      }
      if (!hostingMeta) {
        hrsNoMetaReplica = server;
      }
    }

    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    TableName tableName = TableName.valueOf("hbase:meta");
    Table table = connection.getTable(tableName);
    try {
      // load the data to the table
      for (int i = 0; i < 5; i++) {
        LOG.info("Writing data from " + i * 1000 + " to " + (i * 1000 + 1000));
        HTU.loadNumericRows(table, HConstants.CATALOG_FAMILY, i * 1000, i * 1000 + 1000);
        if (i == 0) {
          HTU.moveRegionAndWait(metaReplica.getRegionInfo(), hrsNoMetaReplica.getServerName());
        }
      }

      verifyReplication(tableName, numOfMetaReplica, 0, 5000, HConstants.CATALOG_FAMILY);
    } finally {
      table.close();
      connection.close();
    }
  }

  protected void verifyReplication(TableName tableName, int regionReplication, final int startRow,
    final int endRow, final byte[] family) throws Exception {
    verifyReplication(tableName, regionReplication, startRow, endRow, family, true);
  }

  private void verifyReplication(TableName tableName, int regionReplication, final int startRow,
    final int endRow, final byte[] family, final boolean present) throws Exception {
    // find the regions
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
          LOG.info("verifying replication for region replica:" + region.getRegionInfo());
          try {
            HTU.verifyNumericRows(region, family, startRow, endRow, present);
          } catch (Throwable ex) {
            LOG.warn("Verification from secondary region is not complete yet", ex);
            // still wait
            return false;
          }
          return true;
        }
      });
    }
  }

  /**
   * Replicas come online after primary.
   */
  private void waitForMetaReplicasToOnline() throws IOException {
    final RegionLocator regionLocator =
      HTU.getConnection().getRegionLocator(TableName.META_TABLE_NAME);
    HTU.waitFor(10000,
      // getRegionLocations returns an entry for each replica but if unassigned, entry is null.
      // Pass reload to force us to skip cache else it just keeps returning default.
      () -> regionLocator.getRegionLocations(HConstants.EMPTY_START_ROW, true).stream().
        filter(Objects::nonNull).count() >= numOfMetaReplica);
    List<HRegionLocation> locations = regionLocator.getRegionLocations(HConstants.EMPTY_START_ROW);
    LOG.info("Found locations {}", locations);
    assertEquals(numOfMetaReplica, locations.size());
  }

  /**
   * Scan hbase:meta for <code>tableName</code> content.
   */
  private List<Result> getMetaCells(TableName tableName) throws IOException {
    final List<Result> results = new ArrayList<>();
    MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
      @Override public boolean visit(Result r) throws IOException {
        results.add(r);
        return true;
      }
    };
    MetaTableAccessor.scanMetaForTableRegions(HTU.getConnection(), visitor, tableName);
    return results;
  }

  /**
   * @return All Regions for tableName including Replicas.
   */
  private Region[] getAllRegions(TableName tableName, int replication) {
    final Region[] regions = new Region[replication];
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
    return regions;
  }

  private Region getOneRegion(TableName tableName) {
    for (int i = 0; i < NB_SERVERS; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getRegions(tableName);
      if (onlineRegions.size() > 1) {
        return onlineRegions.get(0);
      }
    }
    return null;
  }

  /**
   * Verify when a Table is deleted from primary, then there are no references in replicas
   * (because they get the delete of the table rows too).
   */
  private void verifyDeletedReplication(TableName tableName, int regionReplication,
    final TableName deletedTableName) {
    final Region[] regions = getAllRegions(tableName, regionReplication);

    // Start count at '1' so we skip default, primary replica and only look at secondaries.
    for (int i = 1; i < regionReplication; i++) {
      final Region region = regions[i];
      // wait until all the data is replicated to all secondary regions
      Waiter.waitFor(HTU.getConfiguration(), 30000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          LOG.info("Verifying replication for region replica {}", region.getRegionInfo());
          try (RegionScanner rs = region.getScanner(new Scan())) {
            List<Cell> cells = new ArrayList<>();
            while (rs.next(cells)) {
              continue;
            }
            return doesNotContain(cells, deletedTableName);
          } catch (Throwable ex) {
            LOG.warn("Verification from secondary region is not complete yet", ex);
            // still wait
            return false;
          }
        }
      });
    }
  }

  /**
   * Cells are from hbase:meta replica so will start w/ 'tableName,'; i.e. the tablename followed
   * by HConstants.DELIMITER. Make sure the deleted table is no longer present in passed
   * <code>cells</code>.
   */
  private boolean doesNotContain(List<Cell> cells, TableName tableName) {
    for (Cell cell : cells) {
      String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
      if (row.startsWith(tableName.toString() + HConstants.DELIMITER)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Verify Replicas have results (exactly).
   */
  private void verifyReplication(TableName tableName, int regionReplication,
    List<Result> contains) {
    final Region[] regions = getAllRegions(tableName, regionReplication);

    // Start count at '1' so we skip default, primary replica and only look at secondaries.
    for (int i = 1; i < regionReplication; i++) {
      final Region region = regions[i];
      // wait until all the data is replicated to all secondary regions
      Waiter.waitFor(HTU.getConfiguration(), 30000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          LOG.info("Verifying replication for region replica {}", region.getRegionInfo());
          try (RegionScanner rs = region.getScanner(new Scan())) {
            List<Cell> cells = new ArrayList<>();
            while (rs.next(cells)) {
              continue;
            }
            return contains(contains, cells);
          } catch (Throwable ex) {
            LOG.warn("Verification from secondary region is not complete yet", ex);
            // still wait
            return false;
          }
        }
      });
    }
  }

  /**
   * Presumes sorted Cells. Verify that <code>cells</code> has <code>contains</code> at least.
   */
  static boolean contains(List<Result> contains, List<Cell> cells) throws IOException {
    CellScanner containsScanner = CellUtil.createCellScanner(contains);
    CellScanner cellsScanner = CellUtil.createCellScanner(cells);
    int matches = 0;
    int count = 0;
    while (containsScanner.advance()) {
      while (cellsScanner.advance()) {
        count++;
        LOG.info("{} {}", containsScanner.current(), cellsScanner.current());
        if (containsScanner.current().equals(cellsScanner.current())) {
          matches++;
          break;
        }
      }
    }
    return !containsScanner.advance() && matches >= 1 && count >= matches && count == cells.size();
  }

  private void doNGets(final Table table, final byte[][] keys) throws Exception {
    for (byte[] key : keys) {
      Result r = table.get(new Get(key));
      assertArrayEquals(VALUE, r.getValue(HConstants.CATALOG_FAMILY, HConstants.CATALOG_FAMILY));
    }
  }

  private void primaryNoChangeReplicaIncrease(final long[] before, final long[] after) {
    assertEquals(before[RegionInfo.DEFAULT_REPLICA_ID],
      after[RegionInfo.DEFAULT_REPLICA_ID]);

    for (int i = 1; i < after.length; i ++) {
      assertTrue(after[i] > before[i]);
    }
  }

  private void primaryIncreaseReplicaNoChange(final long[] before, final long[] after) {
    // There are read requests increase for primary meta replica.
    assertTrue(after[RegionInfo.DEFAULT_REPLICA_ID] >
      before[RegionInfo.DEFAULT_REPLICA_ID]);

    // No change for replica regions
    for (int i = 1; i < after.length; i ++) {
      assertEquals(before[i], after[i]);
    }
  }

  private void primaryMayIncreaseReplicaNoChange(final long[] before, final long[] after) {
    // For primary meta replica, scan request may increase. No change for replica meta regions.
    assertTrue(after[RegionInfo.DEFAULT_REPLICA_ID] >=
      before[RegionInfo.DEFAULT_REPLICA_ID]);

    // No change for replica regions
    for (int i = 1; i < after.length; i ++) {
      assertEquals(before[i], after[i]);
    }
  }

  private void primaryIncreaseReplicaIncrease(final long[] before, final long[] after) {
    // There are read requests increase for primary meta replica.
    assertTrue(after[RegionInfo.DEFAULT_REPLICA_ID] > before[RegionInfo.DEFAULT_REPLICA_ID]);

    // There are read requests incrase for meta replica regions.
    for (int i = 1; i < after.length; i++) {
      assertTrue(after[i] > before[i]);
    }
  }

  private void getMetaReplicaReadRequests(final Region[] metaRegions, final long[] counters) {
    int i = 0;
    for (Region r : metaRegions) {
      LOG.info("read request for region {} is {}", r, r.getReadRequestsCount());
      counters[i] = r.getReadRequestsCount();
      i ++;
    }
  }

  @Test
  public void testHBaseMetaReplicaGets() throws Exception {

    TableName tn = TableName.valueOf(this.name.getMethodName());
    final Region[] metaRegions = getAllRegions(TableName.META_TABLE_NAME, numOfMetaReplica);
    long[] readReqsForMetaReplicas = new long[numOfMetaReplica];
    long[] readReqsForMetaReplicasAfterGet = new long[numOfMetaReplica];
    long[] readReqsForMetaReplicasAfterMove = new long[numOfMetaReplica];
    long[] readReqsForMetaReplicasAfterSecondMove = new long[numOfMetaReplica];
    long[] readReqsForMetaReplicasAfterThirdGet = new long[numOfMetaReplica];
    Region userRegion = null;
    HRegionServer srcRs = null;
    HRegionServer destRs = null;

    try (Table table = HTU.createTable(tn, HConstants.CATALOG_FAMILY,
      Arrays.copyOfRange(HBaseTestingUtility.KEYS, 1, HBaseTestingUtility.KEYS.length))) {
      verifyReplication(TableName.META_TABLE_NAME, numOfMetaReplica, getMetaCells(table.getName()));
      // load different values
      HTU.loadTable(table, new byte[][] { HConstants.CATALOG_FAMILY }, VALUE);
      for (int i = 0; i < NB_SERVERS; i++) {
        HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
        List<HRegion> onlineRegions = rs.getRegions(tn);
        if (onlineRegions.size() > 0) {
          userRegion = onlineRegions.get(0);
          srcRs = rs;
          if (i > 0) {
            destRs = HTU.getMiniHBaseCluster().getRegionServer(0);
          } else {
            destRs = HTU.getMiniHBaseCluster().getRegionServer(1);
          }
        }
      }

      getMetaReplicaReadRequests(metaRegions, readReqsForMetaReplicas);

      Configuration c = new Configuration(HTU.getConfiguration());
      c.set(LOCATOR_META_REPLICAS_MODE, "LoadBalance");
      Connection connection = ConnectionFactory.createConnection(c);
      Table tableForGet = connection.getTable(tn);
      byte[][] getRows = new byte[HBaseTestingUtility.KEYS.length][];

      int i = 0;
      for (byte[] key : HBaseTestingUtility.KEYS) {
        getRows[i] = key;
        i++;
      }
      getRows[0] = Bytes.toBytes("aaa");
      doNGets(tableForGet, getRows);

      getMetaReplicaReadRequests(metaRegions, readReqsForMetaReplicasAfterGet);

      // There are more reads against all meta replica regions, including the primary region.
      primaryIncreaseReplicaIncrease(readReqsForMetaReplicas, readReqsForMetaReplicasAfterGet);

      // move one of regions so it meta cache may be invalid.
      HTU.moveRegionAndWait(userRegion.getRegionInfo(), destRs.getServerName());

      doNGets(tableForGet, getRows);

      getMetaReplicaReadRequests(metaRegions, readReqsForMetaReplicasAfterMove);

      // There are read requests increase for primary meta replica.
      // For rest of meta replicas, there is no change as regionMove will tell the new location
      primaryIncreaseReplicaNoChange(readReqsForMetaReplicasAfterGet,
        readReqsForMetaReplicasAfterMove);
      // Move region again.
      HTU.moveRegionAndWait(userRegion.getRegionInfo(), srcRs.getServerName());

      // Wait until moveRegion cache timeout.
      while (destRs.getMovedRegion(userRegion.getRegionInfo().getEncodedName()) != null) {
        Thread.sleep(1000);
      }

      getMetaReplicaReadRequests(metaRegions, readReqsForMetaReplicasAfterSecondMove);

      // There may be read requests increase for primary meta replica.
      // For rest of meta replicas, there is no change.
      primaryMayIncreaseReplicaNoChange(readReqsForMetaReplicasAfterMove,
        readReqsForMetaReplicasAfterSecondMove);

      doNGets(tableForGet, getRows);

      getMetaReplicaReadRequests(metaRegions, readReqsForMetaReplicasAfterThirdGet);

      // Since it gets RegionNotServedException, it will go to primary for the next lookup.
      // There are read requests increase for primary meta replica.
      // For rest of meta replicas, there is no change.
      primaryIncreaseReplicaNoChange(readReqsForMetaReplicasAfterSecondMove,
        readReqsForMetaReplicasAfterThirdGet);
    }
  }
}
