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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
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

@Category({ MasterTests.class, MediumTests.class })
public class TestReplicationBarrierCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationBarrierCleaner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileCleaner.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Rule
  public final TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws IOException {
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
      ResultScanner scanner = table.getScanner(new Scan().addFamily(HConstants.CATALOG_FAMILY)
        .addFamily(HConstants.REPLICATION_BARRIER_FAMILY).setFilter(new FirstKeyOnlyFilter()))) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        TableName tableName = RegionInfo.getTable(result.getRow());
        if (!tableName.isSystemTable()) {
          table.delete(new Delete(result.getRow()));
        }
      }
    }
  }

  private ReplicationPeerManager create(ReplicationQueueStorage queueStorage,
      List<String> firstPeerIds, @SuppressWarnings("unchecked") List<String>... peerIds) {
    ReplicationPeerManager peerManager = mock(ReplicationPeerManager.class);
    if (queueStorage != null) {
      when(peerManager.getQueueStorage()).thenReturn(queueStorage);
    }
    if (peerIds.length == 0) {
      when(peerManager.getSerialPeerIdsBelongsTo(any(TableName.class))).thenReturn(firstPeerIds);
    } else {
      when(peerManager.getSerialPeerIdsBelongsTo(any(TableName.class))).thenReturn(firstPeerIds,
        peerIds);
    }
    return peerManager;
  }

  private ReplicationQueueStorage create(Long lastPushedSeqId, Long... lastPushedSeqIds)
      throws ReplicationException {
    ReplicationQueueStorage queueStorage = mock(ReplicationQueueStorage.class);
    if (lastPushedSeqIds.length == 0) {
      when(queueStorage.getLastSequenceId(anyString(), anyString())).thenReturn(lastPushedSeqId);
    } else {
      when(queueStorage.getLastSequenceId(anyString(), anyString())).thenReturn(lastPushedSeqId,
        lastPushedSeqIds);
    }
    return queueStorage;
  }

  private ReplicationBarrierCleaner create(ReplicationPeerManager peerManager) throws IOException {
    return new ReplicationBarrierCleaner(UTIL.getConfiguration(), new WarnOnlyStoppable(),
      UTIL.getConnection(), peerManager);
  }

  private void addBarrier(RegionInfo region, long... barriers) throws IOException {
    Put put = new Put(region.getRegionName(), EnvironmentEdgeManager.currentTime());
    for (int i = 0; i < barriers.length; i++) {
      put.addColumn(HConstants.REPLICATION_BARRIER_FAMILY, HConstants.SEQNUM_QUALIFIER,
        put.getTimestamp() - barriers.length + i, Bytes.toBytes(barriers[i]));
    }
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      table.put(put);
    }
  }

  private void fillCatalogFamily(RegionInfo region) throws IOException {
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      table.put(new Put(region.getRegionName()).addColumn(HConstants.CATALOG_FAMILY,
        Bytes.toBytes("whatever"), Bytes.toBytes("whatever")));
    }
  }

  private void clearCatalogFamily(RegionInfo region) throws IOException {
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      table.delete(new Delete(region.getRegionName()).addFamily(HConstants.CATALOG_FAMILY));
    }
  }

  @Test
  public void testNothing() throws IOException {
    ReplicationPeerManager peerManager = mock(ReplicationPeerManager.class);
    ReplicationBarrierCleaner cleaner = create(peerManager);
    cleaner.chore();
    verify(peerManager, never()).getSerialPeerIdsBelongsTo(any(TableName.class));
    verify(peerManager, never()).getQueueStorage();
  }

  @Test
  public void testCleanNoPeers() throws IOException {
    TableName tableName1 = TableName.valueOf(name.getMethodName() + "_1");
    RegionInfo region11 =
      RegionInfoBuilder.newBuilder(tableName1).setEndKey(Bytes.toBytes(1)).build();
    addBarrier(region11, 10, 20, 30, 40, 50, 60);
    fillCatalogFamily(region11);
    RegionInfo region12 =
      RegionInfoBuilder.newBuilder(tableName1).setStartKey(Bytes.toBytes(1)).build();
    addBarrier(region12, 20, 30, 40, 50, 60, 70);
    fillCatalogFamily(region12);

    TableName tableName2 = TableName.valueOf(name.getMethodName() + "_2");
    RegionInfo region21 =
      RegionInfoBuilder.newBuilder(tableName2).setEndKey(Bytes.toBytes(1)).build();
    addBarrier(region21, 100, 200, 300, 400);
    fillCatalogFamily(region21);
    RegionInfo region22 =
      RegionInfoBuilder.newBuilder(tableName2).setStartKey(Bytes.toBytes(1)).build();
    addBarrier(region22, 200, 300, 400, 500, 600);
    fillCatalogFamily(region22);

    @SuppressWarnings("unchecked")
    ReplicationPeerManager peerManager =
      create(null, Collections.emptyList(), Collections.emptyList());
    ReplicationBarrierCleaner cleaner = create(peerManager);
    cleaner.chore();

    // should never call this method
    verify(peerManager, never()).getQueueStorage();
    // should only be called twice although we have 4 regions to clean
    verify(peerManager, times(2)).getSerialPeerIdsBelongsTo(any(TableName.class));

    assertArrayEquals(new long[] { 60 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region11.getRegionName()));
    assertArrayEquals(new long[] { 70 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region12.getRegionName()));

    assertArrayEquals(new long[] { 400 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region21.getRegionName()));
    assertArrayEquals(new long[] { 600 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region22.getRegionName()));
  }

  @Test
  public void testDeleteBarriers() throws IOException, ReplicationException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    addBarrier(region, 10, 20, 30, 40, 50, 60);
    // two peers
    ReplicationQueueStorage queueStorage = create(-1L, 2L, 15L, 25L, 20L, 25L, 65L, 55L, 70L, 70L);
    List<String> peerIds = Lists.newArrayList("1", "2");

    @SuppressWarnings("unchecked")
    ReplicationPeerManager peerManager =
      create(queueStorage, peerIds, peerIds, peerIds, peerIds, peerIds);
    ReplicationBarrierCleaner cleaner = create(peerManager);

    // beyond the first barrier, no deletion
    cleaner.chore();
    assertArrayEquals(new long[] { 10, 20, 30, 40, 50, 60 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region.getRegionName()));

    // in the first range, still no deletion
    cleaner.chore();
    assertArrayEquals(new long[] { 10, 20, 30, 40, 50, 60 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region.getRegionName()));

    // in the second range, 10 is deleted
    cleaner.chore();
    assertArrayEquals(new long[] { 20, 30, 40, 50, 60 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region.getRegionName()));

    // between 50 and 60, so the barriers before 50 will be deleted
    cleaner.chore();
    assertArrayEquals(new long[] { 50, 60 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region.getRegionName()));

    // in the last open range, 50 is deleted
    cleaner.chore();
    assertArrayEquals(new long[] { 60 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region.getRegionName()));
  }

  @Test
  public void testDeleteRowForDeletedRegion() throws IOException, ReplicationException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    addBarrier(region, 40, 50, 60);
    fillCatalogFamily(region);

    String peerId = "1";
    ReplicationQueueStorage queueStorage = create(59L);
    @SuppressWarnings("unchecked")
    ReplicationPeerManager peerManager = create(queueStorage, Lists.newArrayList(peerId));
    ReplicationBarrierCleaner cleaner = create(peerManager);

    // we have something in catalog family, so only delete 40
    cleaner.chore();
    assertArrayEquals(new long[] { 50, 60 },
      MetaTableAccessor.getReplicationBarrier(UTIL.getConnection(), region.getRegionName()));
    verify(queueStorage, never()).removeLastSequenceIds(anyString(), anyList());

    // No catalog family, then we should remove the whole row
    clearCatalogFamily(region);
    cleaner.chore();
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      assertFalse(table
        .exists(new Get(region.getRegionName()).addFamily(HConstants.REPLICATION_BARRIER_FAMILY)));
    }
    verify(queueStorage, times(1)).removeLastSequenceIds(peerId,
      Arrays.asList(region.getEncodedName()));
  }

  @Test
  public void testDeleteRowForDeletedRegionNoPeers() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    addBarrier(region, 40, 50, 60);

    ReplicationPeerManager peerManager = mock(ReplicationPeerManager.class);
    ReplicationBarrierCleaner cleaner = create(peerManager);
    cleaner.chore();

    verify(peerManager, times(1)).getSerialPeerIdsBelongsTo(tableName);
    // There are no peers, and no catalog family for this region either, so we should remove the
    // barriers. And since there is no catalog family, after we delete the barrier family, the whole
    // row is deleted.
    try (Table table = UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      assertFalse(table.exists(new Get(region.getRegionName())));
    }
  }

  private static class WarnOnlyStoppable implements Stoppable {
    @Override
    public void stop(String why) {
      LOG.warn("TestReplicationBarrierCleaner received stop, ignoring. Reason: " + why);
    }

    @Override
    public boolean isStopped() {
      return false;
    }
  }
}
