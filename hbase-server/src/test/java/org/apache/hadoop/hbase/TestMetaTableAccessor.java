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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.DelegatingRpcScheduler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test {@link org.apache.hadoop.hbase.MetaTableAccessor}.
 */
@Category({MiscTests.class, MediumTests.class})
@SuppressWarnings("deprecation")
public class TestMetaTableAccessor {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaTableAccessor.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMetaTableAccessor.class);
  private static final  HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Connection connection;

  @Rule
  public TestName name = new TestName();

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
   * Test for HBASE-23044.
   */
  @Test
  public void testGetMergeRegions() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    UTIL.createMultiRegionTable(tn, Bytes.toBytes("CF"), 4);
    UTIL.waitTableAvailable(tn);
    try (Admin admin = UTIL.getAdmin()) {
      List<RegionInfo> regions = admin.getRegions(tn);
      assertEquals(4, regions.size());
      admin.mergeRegionsAsync(regions.get(0).getRegionName(), regions.get(1).getRegionName(), false)
          .get(60, TimeUnit.SECONDS);
      admin.mergeRegionsAsync(regions.get(2).getRegionName(), regions.get(3).getRegionName(), false)
          .get(60, TimeUnit.SECONDS);

      List<RegionInfo> mergedRegions = admin.getRegions(tn);
      assertEquals(2, mergedRegions.size());
      RegionInfo mergedRegion0 = mergedRegions.get(0);
      RegionInfo mergedRegion1 = mergedRegions.get(1);

      List<RegionInfo> mergeParents =
          MetaTableAccessor.getMergeRegions(connection, mergedRegion0.getRegionName());
      assertTrue(mergeParents.contains(regions.get(0)));
      assertTrue(mergeParents.contains(regions.get(1)));
      mergeParents = MetaTableAccessor.getMergeRegions(connection, mergedRegion1.getRegionName());
      assertTrue(mergeParents.contains(regions.get(2)));
      assertTrue(mergeParents.contains(regions.get(3)));

      // Delete merge qualifiers for mergedRegion0, then cannot getMergeRegions again
      MetaTableAccessor.deleteMergeQualifiers(connection, mergedRegion0);
      mergeParents = MetaTableAccessor.getMergeRegions(connection, mergedRegion0.getRegionName());
      assertNull(mergeParents);

      mergeParents = MetaTableAccessor.getMergeRegions(connection, mergedRegion1.getRegionName());
      assertTrue(mergeParents.contains(regions.get(2)));
      assertTrue(mergeParents.contains(regions.get(3)));
    }
    UTIL.deleteTable(tn);
  }

  @Test
  public void testAddMergeRegions() throws IOException {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    Put put = new Put(Bytes.toBytes(this.name.getMethodName()));
    List<RegionInfo> ris = new ArrayList<>();
    int limit = 10;
    byte [] previous = HConstants.EMPTY_START_ROW;
    for (int i = 0; i < limit; i++) {
      RegionInfo ri = RegionInfoBuilder.newBuilder(tn).
          setStartKey(previous).setEndKey(Bytes.toBytes(i)).build();
      ris.add(ri);
    }
    put = MetaTableAccessor.addMergeRegions(put, ris);
    List<Cell> cells = put.getFamilyCellMap().get(HConstants.CATALOG_FAMILY);
    String previousQualifier = null;
    assertEquals(limit, cells.size());
    for (Cell cell: cells) {
      LOG.info(cell.toString());
      String qualifier = Bytes.toString(cell.getQualifierArray());
      assertTrue(qualifier.startsWith(HConstants.MERGE_QUALIFIER_PREFIX_STR));
      assertNotEquals(qualifier, previousQualifier);
      previousQualifier = qualifier;
    }
  }

  @Test
  public void testIsMetaWhenAllHealthy() throws InterruptedException {
    HMaster m = UTIL.getMiniHBaseCluster().getMaster();
    assertTrue(m.waitForMetaOnline());
  }

  @Test
  public void testIsMetaWhenMetaGoesOffline() throws InterruptedException {
    HMaster m = UTIL.getMiniHBaseCluster().getMaster();
    int index = UTIL.getMiniHBaseCluster().getServerWithMeta();
    HRegionServer rsWithMeta = UTIL.getMiniHBaseCluster().getRegionServer(index);
    rsWithMeta.abort("TESTING");
    assertTrue(m.waitForMetaOnline());
  }

  /**
   * Does {@link MetaTableAccessor#getRegion(Connection, byte[])} and a write
   * against hbase:meta while its hosted server is restarted to prove our retrying
   * works.
   */
  @Test public void testRetrying()
  throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    LOG.info("Started " + tableName);
    Table t = UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY);
    int regionCount = -1;
    try (RegionLocator r = UTIL.getConnection().getRegionLocator(tableName)) {
      regionCount = r.getStartKeys().length;
    }
    // Test it works getting a region from just made user table.
    final List<RegionInfo> regions =
      testGettingTableRegions(connection, tableName, regionCount);
    MetaTask reader = new MetaTask(connection, "reader") {
      @Override
      void metaTask() throws Throwable {
        testGetRegion(connection, regions.get(0));
        LOG.info("Read " + regions.get(0).getEncodedName());
      }
    };
    MetaTask writer = new MetaTask(connection, "writer") {

      @Override
      void metaTask() throws IOException {
        MetaTableAccessor.addRegionsToMeta(connection, Collections.singletonList(regions.get(0)),
          1);
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

  @Test
  public void testGetRegionsFromMetaTable() throws IOException, InterruptedException {
    List<RegionInfo> regions = MetaTableLocator.getMetaRegions(UTIL.getZooKeeperWatcher());
    assertTrue(regions.size() >= 1);
    assertTrue(
        MetaTableLocator.getMetaRegionsAndLocations(UTIL.getZooKeeperWatcher()).size() >= 1);
  }

  @Test
  public void testGetRegion() throws IOException, InterruptedException {
    final String name = this.name.getMethodName();
    LOG.info("Started " + name);
    // Test get on non-existent region.
    Pair<RegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, Bytes.toBytes("nonexistent-region"));
    assertNull(pair);
    LOG.info("Finished " + name);
  }

  // Test for the optimization made in HBASE-3650
  @Test public void testScanMetaForTable()
  throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    LOG.info("Started " + tableName);

    /** Create 2 tables
     - testScanMetaForTable
     - testScanMetaForTablf
    **/

    UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    // name that is +1 greater than the first one (e+1=f)
    TableName greaterName =
        TableName.valueOf("testScanMetaForTablf");
    UTIL.createTable(greaterName, HConstants.CATALOG_FAMILY);

    // Now make sure we only get the regions from 1 of the tables at a time

    assertEquals(1, MetaTableAccessor.getTableRegions(connection, tableName).size());
    assertEquals(1, MetaTableAccessor.getTableRegions(connection, greaterName).size());
  }

  private static List<RegionInfo> testGettingTableRegions(final Connection connection,
      final TableName name, final int regionCount)
  throws IOException, InterruptedException {
    List<RegionInfo> regions = MetaTableAccessor.getTableRegions(connection, name);
    assertEquals(regionCount, regions.size());
    Pair<RegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, regions.get(0).getRegionName());
    assertEquals(regions.get(0).getEncodedName(),
      pair.getFirst().getEncodedName());
    return regions;
  }

  private static void testGetRegion(final Connection connection,
      final RegionInfo region)
  throws IOException, InterruptedException {
    Pair<RegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, region.getRegionName());
    assertEquals(region.getEncodedName(),
      pair.getFirst().getEncodedName());
  }

  @Test
  public void testParseReplicaIdFromServerColumn() {
    String column1 = HConstants.SERVER_QUALIFIER_STR;
    assertEquals(0,
        MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column1)));
    String column2 = column1 + MetaTableAccessor.META_REPLICA_ID_DELIMITER;
    assertEquals(-1,
        MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column2)));
    String column3 = column2 + "00";
    assertEquals(-1,
        MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column3)));
    String column4 = column3 + "2A";
    assertEquals(42,
        MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column4)));
    String column5 = column4 + "2A";
    assertEquals(-1,
        MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column5)));
    String column6 = HConstants.STARTCODE_QUALIFIER_STR;
    assertEquals(-1,
        MetaTableAccessor.parseReplicaIdFromServerColumn(Bytes.toBytes(column6)));
  }

  /**
   * The info we can get from the regionName is: table name, start key, regionId, replicaId.
   */
  @Test
  public void testParseRegionInfoFromRegionName() throws IOException  {
    RegionInfo originalRegionInfo = RegionInfoBuilder.newBuilder(
      TableName.valueOf(name.getMethodName())).setRegionId(999999L)
      .setStartKey(Bytes.toBytes("2")).setEndKey(Bytes.toBytes("3"))
      .setReplicaId(1).build();
    RegionInfo newParsedRegionInfo = MetaTableAccessor
      .parseRegionInfoFromRegionName(originalRegionInfo.getRegionName());
    assertEquals("Parse TableName error", originalRegionInfo.getTable(),
      newParsedRegionInfo.getTable());
    assertEquals("Parse regionId error", originalRegionInfo.getRegionId(),
      newParsedRegionInfo.getRegionId());
    assertTrue("Parse startKey error", Bytes.equals(originalRegionInfo.getStartKey(),
      newParsedRegionInfo.getStartKey()));
    assertEquals("Parse replicaId error", originalRegionInfo.getReplicaId(),
      newParsedRegionInfo.getReplicaId());
    assertTrue("We can't parse endKey from regionName only",
      Bytes.equals(HConstants.EMPTY_END_ROW, newParsedRegionInfo.getEndKey()));
  }

  @Test
  public void testMetaReaderGetColumnMethods() {
    assertArrayEquals(HConstants.SERVER_QUALIFIER, MetaTableAccessor.getServerColumn(0));
    assertArrayEquals(Bytes.toBytes(HConstants.SERVER_QUALIFIER_STR
      + MetaTableAccessor.META_REPLICA_ID_DELIMITER + "002A"),
      MetaTableAccessor.getServerColumn(42));

    assertArrayEquals(HConstants.STARTCODE_QUALIFIER,
      MetaTableAccessor.getStartCodeColumn(0));
    assertArrayEquals(Bytes.toBytes(HConstants.STARTCODE_QUALIFIER_STR
      + MetaTableAccessor.META_REPLICA_ID_DELIMITER + "002A"),
      MetaTableAccessor.getStartCodeColumn(42));

    assertArrayEquals(HConstants.SEQNUM_QUALIFIER,
      MetaTableAccessor.getSeqNumColumn(0));
    assertArrayEquals(Bytes.toBytes(HConstants.SEQNUM_QUALIFIER_STR
      + MetaTableAccessor.META_REPLICA_ID_DELIMITER + "002A"),
      MetaTableAccessor.getSeqNumColumn(42));
  }

  @Test
  public void testMetaLocationsForRegionReplicas() throws IOException {
    Random rand = ThreadLocalRandom.current();

    ServerName serverName0 = ServerName.valueOf("foo", 60010, rand.nextLong());
    ServerName serverName1 = ServerName.valueOf("bar", 60010, rand.nextLong());
    ServerName serverName100 = ServerName.valueOf("baz", 60010, rand.nextLong());

    long regionId = System.currentTimeMillis();
    RegionInfo primary = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();
    RegionInfo replica1 = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(1)
        .build();
    RegionInfo replica100 = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(100)
        .build();

    long seqNum0 = rand.nextLong();
    long seqNum1 = rand.nextLong();
    long seqNum100 = rand.nextLong();

    try (Table meta = MetaTableAccessor.getMetaHTable(connection)) {
      MetaTableAccessor.updateRegionLocation(connection, primary, serverName0, seqNum0,
        EnvironmentEdgeManager.currentTime());

      // assert that the server, startcode and seqNum columns are there for the primary region
      assertMetaLocation(meta, primary.getRegionName(), serverName0, seqNum0, 0, true);

      // add replica = 1
      MetaTableAccessor.updateRegionLocation(connection, replica1, serverName1, seqNum1,
        EnvironmentEdgeManager.currentTime());
      // check whether the primary is still there
      assertMetaLocation(meta, primary.getRegionName(), serverName0, seqNum0, 0, true);
      // now check for replica 1
      assertMetaLocation(meta, primary.getRegionName(), serverName1, seqNum1, 1, true);

      // add replica = 1
      MetaTableAccessor.updateRegionLocation(connection, replica100, serverName100, seqNum100,
        EnvironmentEdgeManager.currentTime());
      // check whether the primary is still there
      assertMetaLocation(meta, primary.getRegionName(), serverName0, seqNum0, 0, true);
      // check whether the replica 1 is still there
      assertMetaLocation(meta, primary.getRegionName(), serverName1, seqNum1, 1, true);
      // now check for replica 1
      assertMetaLocation(meta, primary.getRegionName(), serverName100, seqNum100, 100, true);
    }
  }

  public static void assertMetaLocation(Table meta, byte[] row, ServerName serverName,
      long seqNum, int replicaId, boolean checkSeqNum) throws IOException {
    Get get = new Get(row);
    Result result = meta.get(get);
    assertTrue(Bytes.equals(
      result.getValue(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(replicaId)),
      Bytes.toBytes(serverName.getAddress().toString())));
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
    RegionInfo primary = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();

    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      List<RegionInfo> regionInfos = Lists.newArrayList(primary);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 3);

      assertEmptyMetaLocation(meta, primary.getRegionName(), 1);
      assertEmptyMetaLocation(meta, primary.getRegionName(), 2);
    } finally {
      meta.close();
    }
  }

  @Test
  public void testMetaLocationForRegionReplicasIsAddedAtRegionSplit() throws IOException {
    long regionId = EnvironmentEdgeManager.currentTime();
    ServerName serverName0 = ServerName.valueOf("foo", 60010,
      ThreadLocalRandom.current().nextLong());
    RegionInfo parent = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();

    RegionInfo splitA = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(Bytes.toBytes("a"))
        .setSplit(false)
        .setRegionId(regionId + 1)
        .setReplicaId(0)
        .build();
    RegionInfo splitB = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(Bytes.toBytes("a"))
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId + 1)
        .setReplicaId(0)
        .build();

    try (Table meta = MetaTableAccessor.getMetaHTable(connection)) {
      List<RegionInfo> regionInfos = Lists.newArrayList(parent);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 3);

      MetaTableAccessor.splitRegion(connection, parent, -1L, splitA, splitB, serverName0, 3);

      assertEmptyMetaLocation(meta, splitA.getRegionName(), 1);
      assertEmptyMetaLocation(meta, splitA.getRegionName(), 2);
      assertEmptyMetaLocation(meta, splitB.getRegionName(), 1);
      assertEmptyMetaLocation(meta, splitB.getRegionName(), 2);
    }
  }

  @Test
  public void testMetaLocationForRegionReplicasIsAddedAtRegionMerge() throws IOException {
    long regionId = EnvironmentEdgeManager.currentTime();
    ServerName serverName0 = ServerName.valueOf("foo", 60010,
      ThreadLocalRandom.current().nextLong());

    RegionInfo parentA = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(Bytes.toBytes("a"))
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();

    RegionInfo parentB = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(Bytes.toBytes("a"))
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();
    RegionInfo merged = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId + 1)
        .setReplicaId(0)
        .build();

    try (Table meta = MetaTableAccessor.getMetaHTable(connection)) {
      List<RegionInfo> regionInfos = Lists.newArrayList(parentA, parentB);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 3);
      MetaTableAccessor.mergeRegions(connection, merged, getMapOfRegionsToSeqNum(parentA, parentB),
          serverName0, 3);
      assertEmptyMetaLocation(meta, merged.getRegionName(), 1);
      assertEmptyMetaLocation(meta, merged.getRegionName(), 2);
    }
  }

  private Map<RegionInfo, Long> getMapOfRegionsToSeqNum(RegionInfo ... regions) {
    Map<RegionInfo, Long> mids = new HashMap<>(regions.length);
    for (RegionInfo region: regions) {
      mids.put(region, -1L);
    }
    return mids;
  }

  @Test
  public void testMetaScanner() throws Exception {
    LOG.info("Starting " + name.getMethodName());

    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[][] SPLIT_KEYS =
        new byte[][] { Bytes.toBytes("region_a"), Bytes.toBytes("region_b") };

    UTIL.createTable(tableName, FAMILY, SPLIT_KEYS);
    Table table = connection.getTable(tableName);
    // Make sure all the regions are deployed
    UTIL.countRows(table);

    MetaTableAccessor.Visitor visitor =
        mock(MetaTableAccessor.Visitor.class);
    doReturn(true).when(visitor).visit((Result) anyObject());

    // Scanning the entire table should give us three rows
    MetaTableAccessor.scanMetaForTableRegions(connection, visitor, tableName);
    verify(visitor, times(3)).visit((Result) anyObject());

    // Scanning the table with a specified empty start row should also
    // give us three hbase:meta rows
    reset(visitor);
    doReturn(true).when(visitor).visit((Result) anyObject());
    MetaTableAccessor.scanMeta(connection, visitor, tableName, null, 1000);
    verify(visitor, times(3)).visit((Result) anyObject());

    // Scanning the table starting in the middle should give us two rows:
    // region_a and region_b
    reset(visitor);
    doReturn(true).when(visitor).visit((Result) anyObject());
    MetaTableAccessor.scanMeta(connection, visitor, tableName, Bytes.toBytes("region_ac"), 1000);
    verify(visitor, times(2)).visit((Result) anyObject());

    // Scanning with a limit of 1 should only give us one row
    reset(visitor);
    doReturn(true).when(visitor).visit((Result) anyObject());
    MetaTableAccessor.scanMeta(connection, visitor, tableName, Bytes.toBytes("region_ac"), 1);
    verify(visitor, times(1)).visit((Result) anyObject());
    table.close();
  }

  /**
   * Tests whether maximum of masters system time versus RSs local system time is used
   */
  @Test
  public void testMastersSystemTimeIsUsedInUpdateLocations() throws IOException {
    long regionId = System.currentTimeMillis();
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();

    ServerName sn = ServerName.valueOf("bar", 0, 0);
    try (Table meta = MetaTableAccessor.getMetaHTable(connection)) {
      List<RegionInfo> regionInfos = Lists.newArrayList(regionInfo);
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
    }
  }

  @Test
  public void testMastersSystemTimeIsUsedInMergeRegions() throws IOException {
    long regionId = System.currentTimeMillis();

    RegionInfo regionInfoA = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(new byte[] {'a'})
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();

    RegionInfo regionInfoB = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(new byte[] {'a'})
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();
    RegionInfo mergedRegionInfo = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();

    ServerName sn = ServerName.valueOf("bar", 0, 0);
    try (Table meta = MetaTableAccessor.getMetaHTable(connection)) {
      List<RegionInfo> regionInfos = Lists.newArrayList(regionInfoA, regionInfoB);
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

      ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
      edge.setValue(masterSystemTime);
      EnvironmentEdgeManager.injectEdge(edge);
      try {
        // now merge the regions, effectively deleting the rows for region a and b.
        MetaTableAccessor.mergeRegions(connection, mergedRegionInfo,
            getMapOfRegionsToSeqNum(regionInfoA, regionInfoB), sn, 1);
      } finally {
        EnvironmentEdgeManager.reset();
      }


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
      int priority = task.getRpcCall().getPriority();

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

    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Admin admin = connection.getAdmin();
        RegionLocator rl = connection.getRegionLocator(tableName)) {

      // create a table and prepare for a manual split
      UTIL.createTable(tableName, "cf1");

      HRegionLocation loc = rl.getAllRegionLocations().get(0);
      RegionInfo parent = loc.getRegionInfo();
      long rid = 1000;
      byte[] splitKey = Bytes.toBytes("a");
      RegionInfo splitA = RegionInfoBuilder.newBuilder(parent.getTable())
          .setStartKey(parent.getStartKey())
          .setEndKey(splitKey)
          .setSplit(false)
          .setRegionId(rid)
          .build();
      RegionInfo splitB = RegionInfoBuilder.newBuilder(parent.getTable())
          .setStartKey(splitKey)
          .setEndKey(parent.getEndKey())
          .setSplit(false)
          .setRegionId(rid)
          .build();

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
      MetaTableAccessor.splitRegion(connection, parent, -1L, splitA, splitB, loc.getServerName(),
        1);

      assertTrue(prevCalls < scheduler.numPriorityCalls);
    }
  }

  @Test
  public void testEmptyMetaDaughterLocationDuringSplit() throws IOException {
    long regionId = EnvironmentEdgeManager.currentTime();
    ServerName serverName0 = ServerName.valueOf("foo", 60010,
      ThreadLocalRandom.current().nextLong());
    RegionInfo parent = RegionInfoBuilder.newBuilder(TableName.valueOf("table_foo"))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId)
        .setReplicaId(0)
        .build();
    RegionInfo splitA = RegionInfoBuilder.newBuilder(TableName.valueOf("table_foo"))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(Bytes.toBytes("a"))
        .setSplit(false)
        .setRegionId(regionId + 1)
        .setReplicaId(0)
        .build();
    RegionInfo splitB = RegionInfoBuilder.newBuilder(TableName.valueOf("table_foo"))
        .setStartKey(Bytes.toBytes("a"))
        .setEndKey(HConstants.EMPTY_END_ROW)
        .setSplit(false)
        .setRegionId(regionId + 1)
        .setReplicaId(0)
        .build();

    Table meta = MetaTableAccessor.getMetaHTable(connection);
    try {
      List<RegionInfo> regionInfos = Lists.newArrayList(parent);
      MetaTableAccessor.addRegionsToMeta(connection, regionInfos, 3);

      MetaTableAccessor.splitRegion(connection, parent, -1L, splitA, splitB,
        serverName0, 3);
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

  @Test
  public void testScanByRegionEncodedNameExistingRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testScanByRegionEncodedNameExistingRegion");
    UTIL.createTable(tableName, "cf");
    final List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
    final String encodedName = regions.get(0).getRegionInfo().getEncodedName();
    final Result result = MetaTableAccessor.scanByRegionEncodedName(UTIL.getConnection(),
      encodedName);
    assertNotNull(result);
    assertTrue(result.advance());
    final String resultingRowKey = CellUtil.getCellKeyAsString(result.current());
    assertTrue(resultingRowKey.contains(encodedName));
    UTIL.deleteTable(tableName);
  }

  @Test
  public void testScanByRegionEncodedNameNonExistingRegion() throws Exception {
    final String encodedName = "nonexistingregion";
    final Result result = MetaTableAccessor.scanByRegionEncodedName(UTIL.getConnection(),
      encodedName);
    assertNull(result);
  }
}

