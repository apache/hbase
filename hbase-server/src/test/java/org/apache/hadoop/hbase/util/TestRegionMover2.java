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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for Region Mover Load/Unload functionality with and without ack mode and also to test
 * exclude functionality useful for rack decommissioning
 */
@Category({ MiscTests.class, LargeTests.class })
public class TestRegionMover2 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionMover2.class);
  private static final String CF = "fam1";

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMover2.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    createTable(name.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.getAdmin().disableTable(tableName);
    TEST_UTIL.getAdmin().deleteTable(tableName);
  }

  private TableName createTable(String name) throws IOException {
    final TableName tableName = TableName.valueOf(name);
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();
    int startKey = 0;
    int endKey = 80000;
    TEST_UTIL.getAdmin().createTable(tableDesc, Bytes.toBytes(startKey), Bytes.toBytes(endKey), 9);
    return tableName;
  }

  @Test
  public void testWithMergedRegions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = createPuts(10000);
    table.put(puts);
    admin.flush(tableName);
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getAddress().toString();
    int numRegions = regionServer.getNumberOfOnlineRegions();
    List<HRegion> hRegions = regionServer.getRegions().stream()
      .filter(hRegion -> hRegion.getRegionInfo().getTable().equals(tableName))
      .collect(Collectors.toList());
    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(true)
        .maxthreads(8);
    try (RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {}", regionServer.getServerName());
      rm.unload();
      Assert.assertEquals(0, regionServer.getNumberOfOnlineRegions());
      LOG.debug("Successfully Unloaded, now Loading");
      admin.mergeRegionsAsync(new byte[][] { hRegions.get(0).getRegionInfo().getRegionName(),
        hRegions.get(1).getRegionInfo().getRegionName() }, true).get(5, TimeUnit.SECONDS);
      Assert.assertTrue(rm.load());
      Assert.assertEquals(numRegions - 2, regionServer.getNumberOfOnlineRegions());
    }
  }

  @Test
  public void testWithSplitRegions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = createPuts(50000);
    table.put(puts);
    admin.flush(tableName);
    admin.compact(tableName);
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getAddress().toString();
    int numRegions = regionServer.getNumberOfOnlineRegions();
    List<HRegion> hRegions = regionServer.getRegions().stream()
      .filter(hRegion -> hRegion.getRegionInfo().getTable().equals(tableName))
      .collect(Collectors.toList());

    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(true)
        .maxthreads(8);
    try (RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {}", regionServer.getServerName());
      rm.unload();
      Assert.assertEquals(0, regionServer.getNumberOfOnlineRegions());
      LOG.debug("Successfully Unloaded, now Loading");
      HRegion hRegion = hRegions.get(1);
      if (hRegion.getRegionInfo().getStartKey().length == 0) {
        hRegion = hRegions.get(0);
      }
      int startKey = 0;
      int endKey = Integer.MAX_VALUE;
      if (hRegion.getRegionInfo().getStartKey().length > 0) {
        startKey = Bytes.toInt(hRegion.getRegionInfo().getStartKey());
      }
      if (hRegion.getRegionInfo().getEndKey().length > 0) {
        endKey = Bytes.toInt(hRegion.getRegionInfo().getEndKey());
      }
      int midKey = startKey + (endKey - startKey) / 2;
      admin.splitRegionAsync(hRegion.getRegionInfo().getRegionName(), Bytes.toBytes(midKey)).get(5,
        TimeUnit.SECONDS);
      Assert.assertTrue(rm.load());
      Assert.assertEquals(numRegions - 1, regionServer.getNumberOfOnlineRegions());
    }
  }

  @Test
  public void testFailedRegionMove() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = createPuts(1000);
    table.put(puts);
    admin.flush(tableName);
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getAddress().toString();
    List<HRegion> hRegions = regionServer.getRegions().stream()
      .filter(hRegion -> hRegion.getRegionInfo().getTable().equals(tableName))
      .collect(Collectors.toList());
    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(true)
        .maxthreads(8);
    try (RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {}", regionServer.getServerName());
      rm.unload();
      Assert.assertEquals(0, regionServer.getNumberOfOnlineRegions());
      LOG.debug("Successfully Unloaded, now Loading");
      admin.offline(hRegions.get(0).getRegionInfo().getRegionName());
      // loading regions will fail because of offline region
      Assert.assertFalse(rm.load());
    }
  }

  @Test
  public void testDeletedTable() throws Exception {
    TableName tableNameToDelete = createTable(name.getMethodName() + "ToDelete");
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getAddress().toString();
    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(true)
        .maxthreads(8);
    try (Admin admin = TEST_UTIL.getAdmin(); RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {}", regionServer.getServerName());
      rm.unload();
      Assert.assertEquals(0, regionServer.getNumberOfOnlineRegions());
      LOG.debug("Successfully Unloaded, now delete table");
      admin.disableTable(tableNameToDelete);
      admin.deleteTable(tableNameToDelete);
      Assert.assertTrue(rm.load());
    }
  }

  public void loadDummyDataInTable(TableName tableName) throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = createPuts(1000);
    table.put(puts);
    admin.flush(tableName);
  }

  @Test
  public void testIsolateSingleRegionOnTheSameServer() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    loadDummyDataInTable(tableName);
    ServerName sourceServerName = findSourceServerName(tableName);
    // Isolating 1 region on the same region server.
    regionIsolationOperation(sourceServerName, sourceServerName, 1, false);
  }

  @Test
  public void testIsolateSingleRegionOnTheDifferentServer() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    loadDummyDataInTable(tableName);
    ServerName sourceServerName = findSourceServerName(tableName);
    ServerName destinationServerName = findDestinationServerName(sourceServerName);
    // Isolating 1 region on the different region server.
    regionIsolationOperation(sourceServerName, destinationServerName, 1, false);
  }

  @Test
  public void testIsolateMultipleRegionsOnTheSameServer() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    loadDummyDataInTable(tableName);
    ServerName sourceServerName = findSourceServerName(tableName);
    // Isolating 2 regions on the same region server.
    regionIsolationOperation(sourceServerName, sourceServerName, 2, false);
  }

  @Test
  public void testIsolateMultipleRegionsOnTheDifferentServer() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    loadDummyDataInTable(tableName);
    // Isolating 2 regions on the different region server.
    ServerName sourceServerName = findSourceServerName(tableName);
    ServerName destinationServerName = findDestinationServerName(sourceServerName);
    regionIsolationOperation(sourceServerName, destinationServerName, 2, false);
  }

  @Test
  public void testIsolateMetaOnTheSameSever() throws Exception {
    ServerName metaServerSource = findMetaRSLocation();
    regionIsolationOperation(metaServerSource, metaServerSource, 1, true);
  }

  @Test
  public void testIsolateMetaOnTheDifferentServer() throws Exception {
    ServerName metaServerSource = findMetaRSLocation();
    ServerName metaServerDestination = findDestinationServerName(metaServerSource);
    regionIsolationOperation(metaServerSource, metaServerDestination, 1, true);
  }

  @Test
  public void testIsolateMetaAndRandomRegionOnTheMetaServer() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    loadDummyDataInTable(tableName);
    ServerName metaServerSource = findMetaRSLocation();
    ServerName randomSeverRegion = findSourceServerName(tableName);
    regionIsolationOperation(randomSeverRegion, metaServerSource, 2, true);
  }

  @Test
  public void testIsolateMetaAndRandomRegionOnTheRandomServer() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    loadDummyDataInTable(tableName);
    ServerName randomSeverRegion = findSourceServerName(tableName);
    regionIsolationOperation(randomSeverRegion, randomSeverRegion, 2, true);
  }

  private List<Put> createPuts(int count) {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      puts.add(new Put(Bytes.toBytes("rowkey_" + i)).addColumn(Bytes.toBytes(CF),
        Bytes.toBytes("q1"), Bytes.toBytes("val_" + i)));
    }
    return puts;
  }

  public ServerName findMetaRSLocation() throws Exception {
    ZKWatcher zkWatcher = new ZKWatcher(TEST_UTIL.getConfiguration(), null, null);
    List<HRegionLocation> result = new ArrayList<>();
    for (String znode : zkWatcher.getMetaReplicaNodes()) {
      String path = ZNodePaths.joinZNode(zkWatcher.getZNodePaths().baseZNode, znode);
      int replicaId = zkWatcher.getZNodePaths().getMetaReplicaIdFromPath(path);
      RegionState state = MetaTableLocator.getMetaRegionState(zkWatcher, replicaId);
      result.add(new HRegionLocation(state.getRegion(), state.getServerName()));
    }
    return result.get(0).getServerName();
  }

  public ServerName findSourceServerName(TableName tableName) throws Exception {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    int numOfRS = cluster.getNumLiveRegionServers();
    ServerName sourceServer = null;
    for (int i = 0; i < numOfRS; i++) {
      HRegionServer regionServer = cluster.getRegionServer(i);
      List<HRegion> hRegions = regionServer.getRegions().stream()
        .filter(hRegion -> hRegion.getRegionInfo().getTable().equals(tableName))
        .collect(Collectors.toList());
      if (hRegions.size() >= 2) {
        sourceServer = regionServer.getServerName();
        break;
      }
    }
    if (sourceServer == null) {
      throw new Exception(
        "This shouldn't happen, No RS found with more than 2 regions of table : " + tableName);
    }
    return sourceServer;
  }

  public ServerName findDestinationServerName(ServerName sourceServerName) throws Exception {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    ServerName destinationServerName = null;
    int numOfRS = cluster.getNumLiveRegionServers();
    for (int i = 0; i < numOfRS; i++) {
      destinationServerName = cluster.getRegionServer(i).getServerName();
      if (!destinationServerName.equals(sourceServerName)) {
        break;
      }
    }
    if (destinationServerName == null) {
      throw new Exception("This shouldn't happen, No RS found which is different than source RS");
    }
    return destinationServerName;
  }

  public void regionIsolationOperation(ServerName sourceServerName,
    ServerName destinationServerName, int numRegionsToIsolate, boolean isolateMetaAlso)
    throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    HRegionServer sourceRS = cluster.getRegionServer(sourceServerName);
    List<HRegion> hRegions = sourceRS.getRegions().stream()
      .filter(hRegion -> hRegion.getRegionInfo().getTable().equals(tableName))
      .collect(Collectors.toList());
    List<String> listOfRegionIDsToIsolate = new ArrayList<>();
    for (int i = 0; i < numRegionsToIsolate; i++) {
      listOfRegionIDsToIsolate.add(hRegions.get(i).getRegionInfo().getEncodedName());
    }

    if (isolateMetaAlso) {
      listOfRegionIDsToIsolate.remove(0);
      listOfRegionIDsToIsolate.add(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedName());
    }

    HRegionServer destinationRS = cluster.getRegionServer(destinationServerName);
    String destinationRSName = destinationRS.getServerName().getAddress().toString();
    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(destinationRSName, TEST_UTIL.getConfiguration()).ack(true)
        .maxthreads(8).isolateRegionIdArray(listOfRegionIDsToIsolate);
    try (RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {} except regions: {}", destinationRS.getServerName(),
        listOfRegionIDsToIsolate);
      rm.isolateRegions();
      Assert.assertEquals(numRegionsToIsolate, destinationRS.getNumberOfOnlineRegions());
      List<HRegion> onlineRegions = destinationRS.getRegions();
      for (int i = 0; i < numRegionsToIsolate; i++) {
        Assert.assertTrue(
          listOfRegionIDsToIsolate.contains(onlineRegions.get(i).getRegionInfo().getEncodedName()));
      }
      LOG.debug("Successfully Isolated {} regions: {} on {}", listOfRegionIDsToIsolate.size(),
        listOfRegionIDsToIsolate, destinationRS.getServerName());
    } finally {
      admin.recommissionRegionServer(destinationRS.getServerName(), null);
    }
  }
}
