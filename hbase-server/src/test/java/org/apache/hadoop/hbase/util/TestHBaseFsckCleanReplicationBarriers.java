/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestHBaseFsckCleanReplicationBarriers {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseFsckCleanReplicationBarriers.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static String PEER_1 = "1", PEER_2 = "2";

  private static ReplicationQueueStorage QUEUE_STORAGE;

  private static String WAL_FILE_NAME = "test.wal";

  private static String TABLE_NAME = "test";

  private static String COLUMN_FAMILY = "info";

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    QUEUE_STORAGE = ReplicationStorageFactory.getReplicationQueueStorage(UTIL.getZooKeeperWatcher(),
      UTIL.getConfiguration());
    createPeer();
    QUEUE_STORAGE.addWAL(UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName(), PEER_1,
      WAL_FILE_NAME);
    QUEUE_STORAGE.addWAL(UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName(), PEER_2,
      WAL_FILE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCleanReplicationBarrierWithNonExistTable()
      throws ClassNotFoundException, IOException {
    TableName tableName = TableName.valueOf(TABLE_NAME + "_non");
    boolean cleaned = HbckTestingUtil.cleanReplicationBarrier(UTIL.getConfiguration(), tableName);
    assertFalse(cleaned);
  }

  @Test
  public void testCleanReplicationBarrierWithDeletedTable() throws Exception {
    TableName tableName = TableName.valueOf(TABLE_NAME + "_deleted");
    List<RegionInfo> regionInfos = new ArrayList<>();
    // only write some barriers into meta table

    for (int i = 0; i < 110; i++) {
      RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes(i))
          .setEndKey(Bytes.toBytes(i + 1)).build();
      regionInfos.add(regionInfo);
      addStateAndBarrier(regionInfo, RegionState.State.OPEN, 10, 100);
      updatePushedSeqId(regionInfo, 10);
      assertEquals("check if there is lastPushedId", 10,
        QUEUE_STORAGE.getLastSequenceId(regionInfo.getEncodedName(), PEER_1));
      assertEquals("check if there is lastPushedId", 10,
        QUEUE_STORAGE.getLastSequenceId(regionInfo.getEncodedName(), PEER_2));
    }
    Scan barrierScan = new Scan();
    barrierScan.setCaching(100);
    barrierScan.addFamily(HConstants.REPLICATION_BARRIER_FAMILY);
    barrierScan
        .withStartRow(
          MetaTableAccessor.getTableStartRowForMeta(tableName, MetaTableAccessor.QueryType.REGION))
        .withStopRow(
          MetaTableAccessor.getTableStopRowForMeta(tableName, MetaTableAccessor.QueryType.REGION));
    Result result;
    try (ResultScanner scanner =
        MetaTableAccessor.getMetaHTable(UTIL.getConnection()).getScanner(barrierScan)) {
      while ((result = scanner.next()) != null) {
        assertTrue(MetaTableAccessor.getReplicationBarriers(result).length > 0);
      }
    }
    boolean cleaned = HbckTestingUtil.cleanReplicationBarrier(UTIL.getConfiguration(), tableName);
    assertTrue(cleaned);
    for (RegionInfo regionInfo : regionInfos) {
      assertEquals("check if there is lastPushedId", -1,
        QUEUE_STORAGE.getLastSequenceId(regionInfo.getEncodedName(), PEER_1));
      assertEquals("check if there is lastPushedId", -1,
        QUEUE_STORAGE.getLastSequenceId(regionInfo.getEncodedName(), PEER_2));
    }
    cleaned = HbckTestingUtil.cleanReplicationBarrier(UTIL.getConfiguration(), tableName);
    assertFalse(cleaned);
    for (RegionInfo region : regionInfos) {
      assertEquals(0, MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(),
        region.getRegionName()).length);
    }
  }

  @Test
  public void testCleanReplicationBarrierWithExistTable() throws Exception {
    TableName tableName = TableName.valueOf(TABLE_NAME);
    String cf = COLUMN_FAMILY;
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build())
        .setReplicationScope(HConstants.REPLICATION_SCOPE_LOCAL).build();
    UTIL.createTable(tableDescriptor, Bytes.split(Bytes.toBytes(1), Bytes.toBytes(256), 123));
    assertTrue(UTIL.getAdmin().getRegions(tableName).size() > 0);
    for (RegionInfo region : UTIL.getAdmin().getRegions(tableName)) {
      addStateAndBarrier(region, RegionState.State.OFFLINE, 10, 100);
      updatePushedSeqId(region, 10);
      assertEquals("check if there is lastPushedId", 10,
        QUEUE_STORAGE.getLastSequenceId(region.getEncodedName(), PEER_1));
      assertEquals("check if there is lastPushedId", 10,
        QUEUE_STORAGE.getLastSequenceId(region.getEncodedName(), PEER_2));
    }
    boolean cleaned = HbckTestingUtil.cleanReplicationBarrier(UTIL.getConfiguration(), tableName);
    assertTrue(cleaned);
    for (RegionInfo region : UTIL.getAdmin().getRegions(tableName)) {
      assertEquals("check if there is lastPushedId", -1,
        QUEUE_STORAGE.getLastSequenceId(region.getEncodedName(), PEER_1));
      assertEquals("check if there is lastPushedId", -1,
        QUEUE_STORAGE.getLastSequenceId(region.getEncodedName(), PEER_2));
    }
    cleaned = HbckTestingUtil.cleanReplicationBarrier(UTIL.getConfiguration(), tableName);
    assertFalse(cleaned);
    for (RegionInfo region : UTIL.getAdmin().getRegions(tableName)) {
      assertEquals(0, MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(),
        region.getRegionName()).length);
    }
  }

  public static void createPeer() throws IOException {
    ReplicationPeerConfig rpc =
      ReplicationPeerConfig.newBuilder().setClusterKey(UTIL.getClusterKey() + "-test")
        .setSerial(true).build();
    UTIL.getAdmin().addReplicationPeer(PEER_1, rpc);
    UTIL.getAdmin().addReplicationPeer(PEER_2, rpc);
  }

  private void addStateAndBarrier(RegionInfo region, RegionState.State state, long... barriers)
      throws IOException {
    Put put = new Put(region.getRegionName(), EnvironmentEdgeManager.currentTime());
    if (state != null) {
      put.addColumn(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER,
        Bytes.toBytes(state.name()));
    }
    for (int i = 0; i < barriers.length; i++) {
      put.addColumn(HConstants.REPLICATION_BARRIER_FAMILY, HConstants.SEQNUM_QUALIFIER,
        put.getTimeStamp() - barriers.length + i, Bytes.toBytes(barriers[i]));
    }
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      table.put(put);
    }
  }

  private void updatePushedSeqId(RegionInfo region, long seqId) throws ReplicationException {
    QUEUE_STORAGE.setWALPosition(UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName(),
      PEER_1, WAL_FILE_NAME, 10, ImmutableMap.of(region.getEncodedName(), seqId));
    QUEUE_STORAGE.setWALPosition(UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName(),
      PEER_2, WAL_FILE_NAME, 10, ImmutableMap.of(region.getEncodedName(), seqId));
  }
}
