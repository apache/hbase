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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestSerialReplicationChecker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSerialReplicationChecker.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static String PEER_ID = "1";

  private static ReplicationQueueStorage QUEUE_STORAGE;

  private static String WAL_FILE_NAME = "test.wal";

  private Connection conn;

  private SerialReplicationChecker checker;

  @Rule
  public final TestName name = new TestName();

  private TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
    QUEUE_STORAGE = ReplicationStorageFactory.getReplicationQueueStorage(UTIL.getZooKeeperWatcher(),
      UTIL.getConfiguration());
    QUEUE_STORAGE.addWAL(UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName(), PEER_ID,
      WAL_FILE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    ReplicationSource source = mock(ReplicationSource.class);
    when(source.getPeerId()).thenReturn(PEER_ID);
    when(source.getReplicationQueueStorage()).thenReturn(QUEUE_STORAGE);
    conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    doAnswer(new Answer<Table>() {

      @Override
      public Table answer(InvocationOnMock invocation) throws Throwable {
        return UTIL.getConnection().getTable((TableName) invocation.getArgument(0));
      }

    }).when(conn).getTable(any(TableName.class));
    Server server = mock(Server.class);
    when(server.getConnection()).thenReturn(conn);
    when(source.getServer()).thenReturn(server);
    checker = new SerialReplicationChecker(UTIL.getConfiguration(), source);
    tableName = TableName.valueOf(name.getMethodName());
  }

  private Entry createEntry(RegionInfo region, long seqId) {
    WALKeyImpl key = mock(WALKeyImpl.class);
    when(key.getTableName()).thenReturn(tableName);
    when(key.getEncodedRegionName()).thenReturn(region.getEncodedNameAsBytes());
    when(key.getSequenceId()).thenReturn(seqId);
    Entry entry = mock(Entry.class);
    when(entry.getKey()).thenReturn(key);
    return entry;
  }

  private Cell createCell(RegionInfo region) {
    return CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(region.getStartKey())
      .setType(Type.Put).build();
  }

  @Test
  public void testNoBarrierCanPush() throws IOException {
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    assertTrue(checker.canPush(createEntry(region, 100), createCell(region)));
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

  private void setState(RegionInfo region, RegionState.State state) throws IOException {
    Put put = new Put(region.getRegionName(), EnvironmentEdgeManager.currentTime());
    put.addColumn(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER,
      Bytes.toBytes(state.name()));
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      table.put(put);
    }
  }

  private void updatePushedSeqId(RegionInfo region, long seqId) throws ReplicationException {
    QUEUE_STORAGE.setWALPosition(UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName(),
      PEER_ID, WAL_FILE_NAME, 10, ImmutableMap.of(region.getEncodedName(), seqId));
  }

  private void addParents(RegionInfo region, List<RegionInfo> parents) throws IOException {
    Put put = new Put(region.getRegionName(), EnvironmentEdgeManager.currentTime());
    put.addColumn(HConstants.REPLICATION_BARRIER_FAMILY,
      MetaTableAccessor.REPLICATION_PARENT_QUALIFIER, MetaTableAccessor.getParentsBytes(parents));
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      table.put(put);
    }
  }

  @Test
  public void testLastRegionAndOpeningCanNotPush() throws IOException, ReplicationException {
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    addStateAndBarrier(region, RegionState.State.OPEN, 10);
    Cell cell = createCell(region);
    // can push since we are in the first range
    assertTrue(checker.canPush(createEntry(region, 100), cell));
    setState(region, RegionState.State.OPENING);
    // can not push since we are in the last range and the state is OPENING
    assertFalse(checker.canPush(createEntry(region, 102), cell));
    addStateAndBarrier(region, RegionState.State.OPEN, 50);
    // can not push since the previous range has not been finished yet
    assertFalse(checker.canPush(createEntry(region, 102), cell));
    updatePushedSeqId(region, 49);
    // can push since the previous range has been finished
    assertTrue(checker.canPush(createEntry(region, 102), cell));
    setState(region, RegionState.State.OPENING);
    assertFalse(checker.canPush(createEntry(region, 104), cell));
  }

  @Test
  public void testCanPushUnder() throws IOException, ReplicationException {
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    addStateAndBarrier(region, RegionState.State.OPEN, 10, 100);
    updatePushedSeqId(region, 9);
    Cell cell = createCell(region);
    assertTrue(checker.canPush(createEntry(region, 20), cell));
    verify(conn, times(1)).getTable(any(TableName.class));
    // not continuous
    for (int i = 22; i < 100; i += 2) {
      assertTrue(checker.canPush(createEntry(region, i), cell));
    }
    // verify that we do not go to meta table
    verify(conn, times(1)).getTable(any(TableName.class));
  }

  @Test
  public void testCanPushIfContinuous() throws IOException, ReplicationException {
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    addStateAndBarrier(region, RegionState.State.OPEN, 10);
    updatePushedSeqId(region, 9);
    Cell cell = createCell(region);
    assertTrue(checker.canPush(createEntry(region, 20), cell));
    verify(conn, times(1)).getTable(any(TableName.class));
    // continuous
    for (int i = 21; i < 100; i++) {
      assertTrue(checker.canPush(createEntry(region, i), cell));
    }
    // verify that we do not go to meta table
    verify(conn, times(1)).getTable(any(TableName.class));
  }

  @Test
  public void testCanPushAfterMerge() throws IOException, ReplicationException {
    // 0xFF is the escape byte when storing region name so let's make sure it can work.
    byte[] endKey = new byte[] { (byte) 0xFF, 0x00, (byte) 0xFF, (byte) 0xFF, 0x01 };
    RegionInfo regionA =
      RegionInfoBuilder.newBuilder(tableName).setEndKey(endKey).setRegionId(1).build();
    RegionInfo regionB =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(endKey).setRegionId(2).build();
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).setRegionId(3).build();
    addStateAndBarrier(regionA, null, 10, 100);
    addStateAndBarrier(regionB, null, 20, 200);
    addStateAndBarrier(region, RegionState.State.OPEN, 200);
    addParents(region, Arrays.asList(regionA, regionB));
    Cell cell = createCell(region);
    // can not push since both parents have not been finished yet
    assertFalse(checker.canPush(createEntry(region, 300), cell));
    updatePushedSeqId(regionB, 199);
    // can not push since regionA has not been finished yet
    assertFalse(checker.canPush(createEntry(region, 300), cell));
    updatePushedSeqId(regionA, 99);
    // can push since all parents have been finished
    assertTrue(checker.canPush(createEntry(region, 300), cell));
  }

  @Test
  public void testCanPushAfterSplit() throws IOException, ReplicationException {
    // 0xFF is the escape byte when storing region name so let's make sure it can work.
    byte[] endKey = new byte[] { (byte) 0xFF, 0x00, (byte) 0xFF, (byte) 0xFF, 0x01 };
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).setRegionId(1).build();
    RegionInfo regionA =
      RegionInfoBuilder.newBuilder(tableName).setEndKey(endKey).setRegionId(2).build();
    RegionInfo regionB =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(endKey).setRegionId(3).build();
    addStateAndBarrier(region, null, 10, 100);
    addStateAndBarrier(regionA, RegionState.State.OPEN, 100, 200);
    addStateAndBarrier(regionB, RegionState.State.OPEN, 100, 300);
    addParents(regionA, Arrays.asList(region));
    addParents(regionB, Arrays.asList(region));
    Cell cellA = createCell(regionA);
    Cell cellB = createCell(regionB);
    // can not push since parent has not been finished yet
    assertFalse(checker.canPush(createEntry(regionA, 150), cellA));
    assertFalse(checker.canPush(createEntry(regionB, 200), cellB));
    updatePushedSeqId(region, 99);
    // can push since parent has been finished
    assertTrue(checker.canPush(createEntry(regionA, 150), cellA));
    assertTrue(checker.canPush(createEntry(regionB, 200), cellB));
  }

  @Test
  public void testCanPushEqualsToBarrier() throws IOException, ReplicationException {
    // For binary search, equals to an element will result to a positive value, let's test whether
    // it works.
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    addStateAndBarrier(region, RegionState.State.OPEN, 10, 100);
    Cell cell = createCell(region);
    assertTrue(checker.canPush(createEntry(region, 10), cell));
    assertFalse(checker.canPush(createEntry(region, 100), cell));
    updatePushedSeqId(region, 99);
    assertTrue(checker.canPush(createEntry(region, 100), cell));
  }
}
