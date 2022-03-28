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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Testcase for HBASE-20147.
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestAddToSerialReplicationPeer extends SerialReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAddToSerialReplicationPeer.class);

  @Before
  public void setUp() throws IOException, StreamLacksCapabilityException {
    setupWALWriter();
  }

  // make sure that we will start replication for the sequence id after move, that's what we want to
  // test here.
  private void moveRegionAndArchiveOldWals(RegionInfo region, HRegionServer rs) throws Exception {
    moveRegion(region, rs);
    rollAllWALs();
  }

  private void waitUntilReplicatedToTheCurrentWALFile(HRegionServer rs, final String oldWalName)
    throws Exception {
    Path path = ((AbstractFSWAL<?>) rs.getWAL(null)).getCurrentFileName();
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(path.getName());
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        ReplicationSourceManager manager =
          ((Replication) rs.getReplicationSourceService()).getReplicationManager();
        // Make sure replication moves to the new file.
        return (manager.getWALs().get(PEER_ID).get(logPrefix).size() == 1) &&
          !oldWalName.equals(manager.getWALs().get(PEER_ID).get(logPrefix).first());
      }

      @Override
      public String explainFailure() throws Exception {
        return "Still not replicated to the current WAL file yet";
      }
    });
  }

  @Test
  public void testAddPeer() throws Exception {
    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    RegionInfo region = UTIL.getAdmin().getRegions(tableName).get(0);
    HRegionServer rs = UTIL.getOtherRegionServer(UTIL.getRSForFirstRegionInTable(tableName));
    moveRegionAndArchiveOldWals(region, rs);
    addPeer(true);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    waitUntilReplicationDone(100);
    checkOrder(100);
  }

  @Test
  public void testChangeToSerial() throws Exception {
    ReplicationPeerConfig peerConfig =
      ReplicationPeerConfig.newBuilder().setClusterKey("127.0.0.1:2181:/hbase")
        .setReplicationEndpointImpl(LocalReplicationEndpoint.class.getName()).build();
    UTIL.getAdmin().addReplicationPeer(PEER_ID, peerConfig, true);

    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }

    RegionInfo region = UTIL.getAdmin().getRegions(tableName).get(0);
    HRegionServer srcRs = UTIL.getRSForFirstRegionInTable(tableName);
    // Get the current wal file name
    String walFileNameBeforeRollover =
      ((AbstractFSWAL<?>) srcRs.getWAL(null)).getCurrentFileName().getName();

    HRegionServer rs = UTIL.getOtherRegionServer(srcRs);
    moveRegionAndArchiveOldWals(region, rs);
    waitUntilReplicationDone(100);
    waitUntilReplicatedToTheCurrentWALFile(srcRs, walFileNameBeforeRollover);

    UTIL.getAdmin().disableReplicationPeer(PEER_ID);
    UTIL.getAdmin().updateReplicationPeerConfig(PEER_ID,
      ReplicationPeerConfig.newBuilder(peerConfig).setSerial(true).build());
    UTIL.getAdmin().enableReplicationPeer(PEER_ID);

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    waitUntilReplicationDone(200);
    checkOrder(200);
  }

  @Test
  public void testAddToSerialPeer() throws Exception {
    ReplicationPeerConfig peerConfig =
      ReplicationPeerConfig.newBuilder().setClusterKey("127.0.0.1:2181:/hbase")
        .setReplicationEndpointImpl(LocalReplicationEndpoint.class.getName())
        .setReplicateAllUserTables(false).setSerial(true).build();
    UTIL.getAdmin().addReplicationPeer(PEER_ID, peerConfig, true);

    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    RegionInfo region = UTIL.getAdmin().getRegions(tableName).get(0);
    HRegionServer srcRs = UTIL.getRSForFirstRegionInTable(tableName);
    HRegionServer rs = UTIL.getOtherRegionServer(srcRs);

    // Get the current wal file name
    String walFileNameBeforeRollover =
      ((AbstractFSWAL<?>) srcRs.getWAL(null)).getCurrentFileName().getName();

    moveRegionAndArchiveOldWals(region, rs);

    // Make sure that the replication done for the oldWal at source rs.
    waitUntilReplicatedToTheCurrentWALFile(srcRs, walFileNameBeforeRollover);

    UTIL.getAdmin().disableReplicationPeer(PEER_ID);
    UTIL.getAdmin().updateReplicationPeerConfig(PEER_ID,
      ReplicationPeerConfig.newBuilder(peerConfig)
        .setTableCFsMap(ImmutableMap.of(tableName, Collections.emptyList())).build());
    UTIL.getAdmin().enableReplicationPeer(PEER_ID);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    waitUntilReplicationDone(100);
    checkOrder(100);
  }

  @Test
  public void testDisabledTable() throws Exception {
    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    UTIL.getAdmin().disableTable(tableName);
    rollAllWALs();
    addPeer(true);
    UTIL.getAdmin().enableTable(tableName);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    waitUntilReplicationDone(100);
    checkOrder(100);
  }

  @Test
  public void testDisablingTable() throws Exception {
    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    UTIL.getAdmin().disableTable(tableName);
    rollAllWALs();
    TableStateManager tsm = UTIL.getMiniHBaseCluster().getMaster().getTableStateManager();
    tsm.setTableState(tableName, TableState.State.DISABLING);
    Thread t = new Thread(() -> {
      try {
        addPeer(true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    Thread.sleep(5000);
    // we will wait on the disabling table so the thread should still be alive.
    assertTrue(t.isAlive());
    tsm.setTableState(tableName, TableState.State.DISABLED);
    t.join();
    UTIL.getAdmin().enableTable(tableName);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    waitUntilReplicationDone(100);
    checkOrder(100);
  }

  @Test
  public void testEnablingTable() throws Exception {
    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    RegionInfo region = UTIL.getAdmin().getRegions(tableName).get(0);
    HRegionServer rs = UTIL.getOtherRegionServer(UTIL.getRSForFirstRegionInTable(tableName));
    moveRegionAndArchiveOldWals(region, rs);
    TableStateManager tsm = UTIL.getMiniHBaseCluster().getMaster().getTableStateManager();
    tsm.setTableState(tableName, TableState.State.ENABLING);
    Thread t = new Thread(() -> {
      try {
        addPeer(true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    Thread.sleep(5000);
    // we will wait on the disabling table so the thread should still be alive.
    assertTrue(t.isAlive());
    tsm.setTableState(tableName, TableState.State.ENABLED);
    t.join();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    waitUntilReplicationDone(100);
    checkOrder(100);
  }
}
