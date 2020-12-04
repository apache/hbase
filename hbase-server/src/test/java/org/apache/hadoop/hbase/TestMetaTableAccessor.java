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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.DelegatingRpcScheduler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test {@link org.apache.hadoop.hbase.MetaTableAccessor}.
 */
@Category(MediumTests.class)
public class TestMetaTableAccessor {
  private static final Log LOG = LogFactory.getLog(TestMetaTableAccessor.class);
  private static final  HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Connection connection;
  private Random random = new Random();

  @BeforeClass public static void beforeClass() throws Exception {
    UTIL.startMiniCluster(3);

    Configuration c = new Configuration(UTIL.getConfiguration());
    // Tests to 4 retries every 5 seconds. Make it try every 1 second so more
    // responsive.  1 second is default as is ten retries.
    c.setLong("hbase.client.pause", 1000);
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    connection = ConnectionFactory.createConnection(c);
  }

  @AfterClass public static void afterClass() throws Exception {
    connection.close();
    UTIL.shutdownMiniCluster();
  }

  /**
   * Does {@link MetaTableAccessor#getRegion(Connection, byte[])} and a write
   * against hbase:meta while its hosted server is restarted to prove our retrying
   * works.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test public void testRetrying()
  throws IOException, InterruptedException {
    final TableName name =
        TableName.valueOf("testRetrying");
    LOG.info("Started " + name);
    HTable t = UTIL.createMultiRegionTable(name, HConstants.CATALOG_FAMILY);
    int regionCount = -1;
    try (RegionLocator r = t.getRegionLocator()) {
      regionCount = r.getStartKeys().length;
    }
    // Test it works getting a region from just made user table.
    final List<HRegionInfo> regions =
      testGettingTableRegions(connection, name, regionCount);
    MetaTask reader = new MetaTask(connection, "reader") {
      @Override
      void metaTask() throws Throwable {
        testGetRegion(connection, regions.get(0));
        LOG.info("Read " + regions.get(0).getEncodedName());
      }
    };
    MetaTask writer = new MetaTask(connection, "writer") {
      @Override
      void metaTask() throws Throwable {
        MetaTableAccessor.addRegionToMeta(connection, regions.get(0));
        LOG.info("Wrote " + regions.get(0).getEncodedName());
      }
    };
    reader.start();
    writer.start();

    // We're gonna check how it takes. If it takes too long, we will consider
    //  it as a fail. We can't put that in the @Test tag as we want to close
    //  the threads nicely
    final long timeOut = 180000;
    long startTime = System.currentTimeMillis();

    try {
      // Make sure reader and writer are working.
      assertTrue(reader.isProgressing());
      assertTrue(writer.isProgressing());

      // Kill server hosting meta -- twice  . See if our reader/writer ride over the
      // meta moves.  They'll need to retry.
      for (int i = 0; i < 2; i++) {
        LOG.info("Restart=" + i);
        UTIL.ensureSomeRegionServersAvailable(2);
        int index = -1;
        do {
          index = UTIL.getMiniHBaseCluster().getServerWithMeta();
        } while (index == -1 &&
          startTime + timeOut < System.currentTimeMillis());

        if (index != -1){
          UTIL.getMiniHBaseCluster().abortRegionServer(index);
          UTIL.getMiniHBaseCluster().waitOnRegionServer(index);
        }
      }

      assertTrue("reader: " + reader.toString(), reader.isProgressing());
      assertTrue("writer: " + writer.toString(), writer.isProgressing());
    } catch (IOException e) {
      throw e;
    } finally {
      reader.stop = true;
      writer.stop = true;
      reader.join();
      writer.join();
      t.close();
    }
    long exeTime = System.currentTimeMillis() - startTime;
    assertTrue("Timeout: test took " + exeTime / 1000 + " sec", exeTime < timeOut);
  }

  /**
   * Thread that runs a MetaTableAccessor task until asked stop.
   */
  abstract static class MetaTask extends Thread {
    boolean stop = false;
    int count = 0;
    Throwable t = null;
    final Connection connection;

    MetaTask(final Connection connection, final String name) {
      super(name);
      this.connection = connection;
    }

    @Override
    public void run() {
      try {
        while(!this.stop) {
          LOG.info("Before " + this.getName()+ ", count=" + this.count);
          metaTask();
          this.count += 1;
          LOG.info("After " + this.getName() + ", count=" + this.count);
          Thread.sleep(100);
        }
      } catch (Throwable t) {
        LOG.info(this.getName() + " failed", t);
        this.t = t;
      }
    }

    boolean isProgressing() throws InterruptedException {
      int currentCount = this.count;
      while(currentCount == this.count) {
        if (!isAlive()) return false;
        if (this.t != null) return false;
        Thread.sleep(10);
      }
      return true;
    }

    @Override
    public String toString() {
      return "count=" + this.count + ", t=" +
        (this.t == null? "null": this.t.toString());
    }

    abstract void metaTask() throws Throwable;
  }

  @Test public void testGetRegionsCatalogTables()
  throws IOException, InterruptedException {
    List<HRegionInfo> regions =
      MetaTableAccessor.getTableRegions(UTIL.getZooKeeperWatcher(),
        connection, TableName.META_TABLE_NAME);
    assertTrue(regions.size() >= 1);
    assertTrue(MetaTableAccessor.getTableRegionsAndLocations(UTIL.getZooKeeperWatcher(),
      connection,TableName.META_TABLE_NAME).size() >= 1);
  }

  @Test public void testTableExists() throws IOException {
    final TableName name =
        TableName.valueOf("testTableExists");
    assertFalse(MetaTableAccessor.tableExists(connection, name));
    UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    assertTrue(MetaTableAccessor.tableExists(connection, name));
    Admin admin = UTIL.getHBaseAdmin();
    admin.disableTable(name);
    admin.deleteTable(name);
    assertFalse(MetaTableAccessor.tableExists(connection, name));
    assertTrue(MetaTableAccessor.tableExists(connection,
      TableName.META_TABLE_NAME));
  }

  @Test public void testGetRegion() throws IOException, InterruptedException {
    final String name = "testGetRegion";
    LOG.info("Started " + name);
    // Test get on non-existent region.
    Pair<HRegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, Bytes.toBytes("nonexistent-region"));
    assertNull(pair);
    LOG.info("Finished " + name);
  }

  // Test for the optimization made in HBASE-3650
  @Test public void testScanMetaForTable()
  throws IOException, InterruptedException {
    final TableName name =
        TableName.valueOf("testScanMetaForTable");
    LOG.info("Started " + name);

    /** Create 2 tables
     - testScanMetaForTable
     - testScanMetaForTablf
    **/

    UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    // name that is +1 greater than the first one (e+1=f)
    TableName greaterName =
        TableName.valueOf("testScanMetaForTablf");
    UTIL.createTable(greaterName, HConstants.CATALOG_FAMILY);

    // Now make sure we only get the regions from 1 of the tables at a time

    assertEquals(1, MetaTableAccessor.getTableRegions(UTIL.getZooKeeperWatcher(),
      connection, name).size());
    assertEquals(1, MetaTableAccessor.getTableRegions(UTIL.getZooKeeperWatcher(),
      connection, greaterName).size());
  }

  private static List<HRegionInfo> testGettingTableRegions(final Connection connection,
      final TableName name, final int regionCount)
  throws IOException, InterruptedException {
    List<HRegionInfo> regions = MetaTableAccessor.getTableRegions(UTIL.getZooKeeperWatcher(),
      connection, name);
    assertEquals(regionCount, regions.size());
    Pair<HRegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, regions.get(0).getRegionName());
    assertEquals(regions.get(0).getEncodedName(),
      pair.getFirst().getEncodedName());
    return regions;
  }

  private static void testGetRegion(final Connection connection,
      final HRegionInfo region)
  throws IOException, InterruptedException {
    Pair<HRegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, region.getRegionName());
    assertEquals(region.getEncodedName(),
      pair.getFirst().getEncodedName());
  }

  @Test
  public void testParseReplicaIdFromServerColumn() {
    String column1 = HConstants.SERVER_QUALIFIER_STR;
    assertEquals(0, MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column1)));
    String column2 = column1 + MetaTableAccessor.META_REPLICA_ID_DELIMITER;
    assertEquals(-1, MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column2)));
    String column3 = column2 + "00";
    assertEquals(-1, MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column3)));
    String column4 = column3 + "2A";
    assertEquals(42, MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column4)));
    String column5 = column4 + "2A";
    assertEquals(-1, MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column5)));
    String column6 = HConstants.STARTCODE_QUALIFIER_STR;
    assertEquals(-1, MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column6)));
  }

  @Test
  public void testMetaReaderGetColumnMethods() {
    Assert.assertArrayEquals(HConstants.SERVER_QUALIFIER, MetaTableAccessor.getServerColumn(0));
    Assert.assertArrayEquals(Bytes.toBytes(HConstants.SERVER_QUALIFIER_STR
      + MetaTableAccessor.META_REPLICA_ID_DELIMITER + "002A"),
      MetaTableAccessor.getServerColumn(42));

    Assert.assertArrayEquals(HConstants.STARTCODE_QUALIFIER,
      MetaTableAccessor.getStartCodeColumn(0));
    Assert.assertArrayEquals(Bytes.toBytes(HConstants.STARTCODE_QUALIFIER_STR
      + MetaTableAccessor.META_REPLICA_ID_DELIMITER + "002A"),
      MetaTableAccessor.getStartCodeColumn(42));

    Assert.assertArrayEquals(HConstants.SEQNUM_QUALIFIER,
      MetaTableAccessor.getSeqNumColumn(0));
    Assert.assertArrayEquals(Bytes.toBytes(HConstants.SEQNUM_QUALIFIER_STR
      + MetaTableAccessor.META_REPLICA_ID_DELIMITER + "002A"),
      MetaTableAccessor.getSeqNumColumn(42));
  }

  @Test
  public void testMetaLocationsForRegionReplicas() throws IOException {
    ServerName serverName0 = ServerName.valueOf("foo", 60010, random.nextLong());
    ServerName serverName1 = ServerName.valueOf("bar", 60010, random.nextLong());
    ServerName serverName100 = ServerName.valueOf("baz", 60010, random.nextLong());

    long regionId = System.currentTimeMillis();
    HRegionInfo primary = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, regionId, 0);
    HRegionInfo replica1 = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, regionId, 1);
    HRegionInfo replica100 = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, regionId, 100);

    long seqNum0 = random.nextLong();
    long seqNum1 = random.nextLong();
    long seqNum100 = random.nextLong();


    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      MetaTableAccessor.updateRegionLocation(connection, primary, serverName0, seqNum0, -1);

      // assert that the server, startcode and seqNum columns are there for the primary region
      assertMetaLocation(meta, primary.getRegionName(), serverName0, seqNum0, 0, true);

      // add replica = 1
      MetaTableAccessor.updateRegionLocation(connection, replica1, serverName1, seqNum1, -1);
      // check whether the primary is still there
      assertMetaLocation(meta, primary.getRegionName(), serverName0, seqNum0, 0, true);
      // now check for replica 1
      assertMetaLocation(meta, primary.getRegionName(), serverName1, seqNum1, 1, true);

      // add replica = 1
      MetaTableAccessor.updateRegionLocation(connection, replica100, serverName100, seqNum100, -1);
      // check whether the primary is still there
      assertMetaLocation(meta, primary.getRegionName(), serverName0, seqNum0, 0, true);
      // check whether the replica 1 is still there
      assertMetaLocation(meta, primary.getRegionName(), serverName1, seqNum1, 1, true);
      // now check for replica 1
      assertMetaLocation(meta, primary.getRegionName(), serverName100, seqNum100, 100, true);
    } finally {
      meta.close();
    }
  }

  public static void assertMetaLocation(Table meta, byte[] row, ServerName serverName,
      long seqNum, int replicaId, boolean checkSeqNum) throws IOException {
    Get get = new Get(row);
    Result result = meta.get(get);
    assertTrue(Bytes.equals(
      result.getValue(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(replicaId)),
      Bytes.toBytes(serverName.getHostAndPort())));
    assertTrue(Bytes.equals(
      result.getValue(HConstants.CATALOG_FAMILY, MetaTableAccessor.getStartCodeColumn(replicaId)),
      Bytes.toBytes(serverName.getStartcode())));
    if (checkSeqNum) {
      assertTrue(Bytes.equals(
        result.getValue(HConstants.CATALOG_FAMILY, MetaTableAccessor.getSeqNumColumn(replicaId)),
        Bytes.toBytes(seqNum)));
    }
  }

  public static void assertEmptyMetaLocation(Table meta, byte[] row, int replicaId)
      throws IOException {
    Get get = new Get(row);
    Result result = meta.get(get);
    Cell serverCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
      MetaTableAccessor.getServerColumn(replicaId));
    Cell startCodeCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
      MetaTableAccessor.getStartCodeColumn(replicaId));
    assertNotNull(serverCell);
    assertNotNull(startCodeCell);
    assertEquals(0, serverCell.getValueLength());
    assertEquals(0, startCodeCell.getValueLength());
  }

  @Test
  public void testMetaLocationForRegionReplicasIsAddedAtTableCreation() throws IOException {
    long regionId = System.currentTimeMillis();
    HRegionInfo primary = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, regionId, 0);

    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      List<HRegionInfo> regionInfos = Lists.newArrayList(primary);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 3);

      assertEmptyMetaLocation(meta, primary.getRegionName(), 1);
      assertEmptyMetaLocation(meta, primary.getRegionName(), 2);
    } finally {
      meta.close();
    }
  }

  @Test
  public void testMetaLocationForRegionReplicasIsAddedAtRegionSplit() throws IOException {
    long regionId = System.currentTimeMillis();
    ServerName serverName0 = ServerName.valueOf("foo", 60010, random.nextLong());
    HRegionInfo parent = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, regionId, 0);
    HRegionInfo splitA = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, Bytes.toBytes("a"), false, regionId+1, 0);
    HRegionInfo splitB = new HRegionInfo(TableName.valueOf("table_foo"),
      Bytes.toBytes("a"), HConstants.EMPTY_END_ROW, false, regionId+1, 0);


    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      List<HRegionInfo> regionInfos = Lists.newArrayList(parent);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 3);

      MetaTableAccessor.splitRegion(connection, parent, splitA, splitB, serverName0, 3);

      assertEmptyMetaLocation(meta, splitA.getRegionName(), 1);
      assertEmptyMetaLocation(meta, splitA.getRegionName(), 2);
      assertEmptyMetaLocation(meta, splitB.getRegionName(), 1);
      assertEmptyMetaLocation(meta, splitB.getRegionName(), 2);
    } finally {
      meta.close();
    }
  }

  @Test
  public void testMetaLocationForRegionReplicasIsAddedAtRegionMerge() throws IOException {
    long regionId = System.currentTimeMillis();
    ServerName serverName0 = ServerName.valueOf("foo", 60010, random.nextLong());

    HRegionInfo parentA = new HRegionInfo(TableName.valueOf("table_foo"),
      Bytes.toBytes("a"), HConstants.EMPTY_END_ROW, false, regionId, 0);
    HRegionInfo parentB = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, Bytes.toBytes("a"), false, regionId, 0);
    HRegionInfo merged = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, regionId+1, 0);

    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      List<HRegionInfo> regionInfos = Lists.newArrayList(parentA, parentB);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 3);

      MetaTableAccessor.mergeRegions(connection, merged, parentA, parentB, serverName0, 3,
          HConstants.LATEST_TIMESTAMP);

      assertEmptyMetaLocation(meta, merged.getRegionName(), 1);
      assertEmptyMetaLocation(meta, merged.getRegionName(), 2);
    } finally {
      meta.close();
    }
  }

  /**
   * Tests whether maximum of masters system time versus RSs local system time is used
   */
  @Test
  public void testMastersSystemTimeIsUsedInUpdateLocations() throws IOException {
    long regionId = System.currentTimeMillis();
    HRegionInfo regionInfo = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, regionId, 0);

    ServerName sn = ServerName.valueOf("bar", 0, 0);
    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      List<HRegionInfo> regionInfos = Lists.newArrayList(regionInfo);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 1);

      long masterSystemTime = EnvironmentEdgeManager.currentTime() + 123456789;
      MetaTableAccessor.updateRegionLocation(connection, regionInfo, sn, 1, masterSystemTime);

      Get get = new Get(regionInfo.getRegionName());
      Result result = meta.get(get);
      Cell serverCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getServerColumn(0));
      Cell startCodeCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        MetaTableAccessor.getStartCodeColumn(0));
      Cell seqNumCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        MetaTableAccessor.getSeqNumColumn(0));
      assertNotNull(serverCell);
      assertNotNull(startCodeCell);
      assertNotNull(seqNumCell);
      assertTrue(serverCell.getValueLength() > 0);
      assertTrue(startCodeCell.getValueLength() > 0);
      assertTrue(seqNumCell.getValueLength() > 0);
      assertEquals(masterSystemTime, serverCell.getTimestamp());
      assertEquals(masterSystemTime, startCodeCell.getTimestamp());
      assertEquals(masterSystemTime, seqNumCell.getTimestamp());
    } finally {
      meta.close();
    }
  }

  @Test
  public void testMastersSystemTimeIsUsedInMergeRegions() throws IOException {
    long regionId = System.currentTimeMillis();
    HRegionInfo regionInfoA = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, new byte[] {'a'}, false, regionId, 0);
    HRegionInfo regionInfoB = new HRegionInfo(TableName.valueOf("table_foo"),
      new byte[] {'a'}, HConstants.EMPTY_END_ROW, false, regionId, 0);
    HRegionInfo mergedRegionInfo = new HRegionInfo(TableName.valueOf("table_foo"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, regionId, 0);

    ServerName sn = ServerName.valueOf("bar", 0, 0);
    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      List<HRegionInfo> regionInfos = Lists.newArrayList(regionInfoA, regionInfoB);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 1);

      // write the serverName column with a big current time, but set the masters time as even
      // bigger. When region merge deletes the rows for regionA and regionB, the serverName columns
      // should not be seen by the following get
      long serverNameTime = EnvironmentEdgeManager.currentTime()   + 100000000;
      long masterSystemTime = EnvironmentEdgeManager.currentTime() + 123456789;

      // write the serverName columns
      MetaTableAccessor.updateRegionLocation(connection, regionInfoA, sn, 1, serverNameTime);

      // assert that we have the serverName column with expected ts
      Get get = new Get(mergedRegionInfo.getRegionName());
      Result result = meta.get(get);
      Cell serverCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getServerColumn(0));
      assertNotNull(serverCell);
      assertEquals(serverNameTime, serverCell.getTimestamp());

      // now merge the regions, effectively deleting the rows for region a and b.
      MetaTableAccessor.mergeRegions(connection, mergedRegionInfo,
        regionInfoA, regionInfoB, sn, 1, masterSystemTime);

      result = meta.get(get);
      serverCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
          MetaTableAccessor.getServerColumn(0));
      Cell startCodeCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        MetaTableAccessor.getStartCodeColumn(0));
      Cell seqNumCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        MetaTableAccessor.getSeqNumColumn(0));
      assertNull(serverCell);
      assertNull(startCodeCell);
      assertNull(seqNumCell);
    } finally {
      meta.close();
    }
  }

  public static class SpyingRpcSchedulerFactory extends SimpleRpcSchedulerFactory {
    @Override
    public RpcScheduler create(Configuration conf, PriorityFunction priority, Abortable server) {
      final RpcScheduler delegate = super.create(conf, priority, server);
      return new SpyingRpcScheduler(delegate);
    }
  }

  public static class SpyingRpcScheduler extends DelegatingRpcScheduler {
    long numPriorityCalls = 0;

    public SpyingRpcScheduler(RpcScheduler delegate) {
      super(delegate);
    }

    @Override
    public boolean dispatch(CallRunner task) throws IOException, InterruptedException {
      int priority = task.getCall().getPriority();

      if (priority > HConstants.QOS_THRESHOLD) {
        numPriorityCalls++;
      }
      return super.dispatch(task);
    }
  }

  @Test
  public void testMetaUpdatesGoToPriorityQueue() throws Exception {
    // This test has to be end-to-end, and do the verification from the server side
    Configuration c = UTIL.getConfiguration();

    c.set(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      SpyingRpcSchedulerFactory.class.getName());

    // restart so that new config takes place
    afterClass();
    beforeClass();

    TableName tableName = TableName.valueOf("foo");
    try (Admin admin = connection.getAdmin();
        RegionLocator rl = connection.getRegionLocator(tableName)) {

      // create a table and prepare for a manual split
      UTIL.createTable(tableName, "cf1");

      HRegionLocation loc = rl.getAllRegionLocations().get(0);
      HRegionInfo parent = loc.getRegionInfo();
      long rid = 1000;
      byte[] splitKey = Bytes.toBytes("a");
      HRegionInfo splitA = new HRegionInfo(parent.getTable(), parent.getStartKey(),
        splitKey, false, rid);
      HRegionInfo splitB = new HRegionInfo(parent.getTable(), splitKey,
        parent.getEndKey(), false, rid);

      // find the meta server
      MiniHBaseCluster cluster = UTIL.getMiniHBaseCluster();
      int rsIndex = cluster.getServerWithMeta();
      HRegionServer rs;
      if (rsIndex >= 0) {
        rs = cluster.getRegionServer(rsIndex);
      } else {
        // it is in master
        rs = cluster.getMaster();
      }
      SpyingRpcScheduler scheduler = (SpyingRpcScheduler) rs.getRpcServer().getScheduler();
      long prevCalls = scheduler.numPriorityCalls;
      MetaTableAccessor.splitRegion(connection, parent, splitA, splitB, loc.getServerName(), 1);

      assertTrue(prevCalls < scheduler.numPriorityCalls);
    }
  }

  @Test
  public void testEmptyMetaDaughterLocationDuringSplit() throws IOException {
    long regionId = System.currentTimeMillis();
    ServerName serverName0 = ServerName.valueOf("foo", 60010, random.nextLong());
    HRegionInfo parent = new HRegionInfo(TableName.valueOf("table_foo"), HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW, false, regionId, 0);
    HRegionInfo splitA = new HRegionInfo(TableName.valueOf("table_foo"), HConstants.EMPTY_START_ROW,
        Bytes.toBytes("a"), false, regionId + 1, 0);
    HRegionInfo splitB = new HRegionInfo(TableName.valueOf("table_foo"), Bytes.toBytes("a"),
        HConstants.EMPTY_END_ROW, false, regionId + 1, 0);

    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      List<HRegionInfo> regionInfos = Lists.newArrayList(parent);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 3);

      MetaTableAccessor.splitRegion(connection, parent, splitA, splitB, serverName0, 3);
      Get get1 = new Get(splitA.getRegionName());
      Result resultA = meta.get(get1);
      Cell serverCellA = resultA.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        MetaTableAccessor.getServerColumn(splitA.getReplicaId()));
      Cell startCodeCellA = resultA.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        MetaTableAccessor.getStartCodeColumn(splitA.getReplicaId()));
      assertNull(serverCellA);
      assertNull(startCodeCellA);

      Get get2 = new Get(splitA.getRegionName());
      Result resultB = meta.get(get2);
      Cell serverCellB = resultB.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        MetaTableAccessor.getServerColumn(splitB.getReplicaId()));
      Cell startCodeCellB = resultB.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        MetaTableAccessor.getStartCodeColumn(splitB.getReplicaId()));
      assertNull(serverCellB);
      assertNull(startCodeCellB);
    } finally {
      if (meta != null) {
        meta.close();
      }
    }
  }
}

