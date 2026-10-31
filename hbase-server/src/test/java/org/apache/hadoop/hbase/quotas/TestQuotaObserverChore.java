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
package org.apache.hadoop.hbase.quotas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Non-HBase cluster unit tests for {@link QuotaObserverChore}.
 */
@Tag(SmallTests.TAG)
public class TestQuotaObserverChore {

  private Connection conn;
  private QuotaObserverChore chore;

  @BeforeEach
  public void setup() throws Exception {
    conn = mock(Connection.class);
    chore = mock(QuotaObserverChore.class);
  }

  @Test
  public void testNumRegionsForTable() {
    TableName tn1 = TableName.valueOf("t1");
    TableName tn2 = TableName.valueOf("t2");
    TableName tn3 = TableName.valueOf("t3");

    final int numTable1Regions = 10;
    final int numTable2Regions = 15;
    final int numTable3Regions = 8;
    Map<RegionInfo, Long> regionReports = new HashMap<>();
    for (int i = 0; i < numTable1Regions; i++) {
      regionReports.put(RegionInfoBuilder.newBuilder(tn1).setStartKey(Bytes.toBytes(i))
        .setEndKey(Bytes.toBytes(i + 1)).build(), 0L);
    }

    for (int i = 0; i < numTable2Regions; i++) {
      regionReports.put(RegionInfoBuilder.newBuilder(tn2).setStartKey(Bytes.toBytes(i))
        .setEndKey(Bytes.toBytes(i + 1)).build(), 0L);
    }

    for (int i = 0; i < numTable3Regions; i++) {
      regionReports.put(RegionInfoBuilder.newBuilder(tn3).setStartKey(Bytes.toBytes(i))
        .setEndKey(Bytes.toBytes(i + 1)).build(), 0L);
    }

    TableQuotaSnapshotStore store = new TableQuotaSnapshotStore(conn, chore, regionReports);
    when(chore.getTableSnapshotStore()).thenReturn(store);

    assertEquals(numTable1Regions, Iterables.size(store.filterBySubject(tn1)));
    assertEquals(numTable2Regions, Iterables.size(store.filterBySubject(tn2)));
    assertEquals(numTable3Regions, Iterables.size(store.filterBySubject(tn3)));
  }

  @Test
  public void testRetryTableEnableOnFailure() throws Exception {
    Admin admin = mock(Admin.class);
    when(conn.getAdmin()).thenReturn(admin);
    SpaceQuotaSnapshotNotifier notifier = mock(SpaceQuotaSnapshotNotifier.class);
    QuotaObserverChore realChore = new QuotaObserverChore(conn, HBaseConfiguration.create(),
      notifier, mock(MasterQuotaManager.class), mock(Stoppable.class), null);
    realChore.initializeSnapshotStores(Collections.emptyMap());

    TableName table = TableName.valueOf("retryEnable");
    SpaceQuotaSnapshot violatingSnapshot =
      new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.DISABLE), 2048L, 1024L);
    SpaceQuotaSnapshot targetSnapshot =
      new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 512L, 1024L);
    realChore.setTableQuotaSnapshot(table, violatingSnapshot);

    doThrow(new IOException("injected enable failure")).doNothing().when(admin).enableTable(table);

    // The first update fails to enable the table, so the in-memory snapshot must not advance.
    assertThrows(IOException.class,
      () -> realChore.updateTableQuota(table, violatingSnapshot, targetSnapshot));
    assertEquals(violatingSnapshot, realChore.getTableQuotaSnapshot(table));
    verify(admin, times(1)).enableTable(table);

    // The next chore run recomputes the same transition and must retry the enable.
    realChore.updateTableQuota(table, violatingSnapshot, targetSnapshot);
    verify(admin, times(2)).enableTable(table);
    assertEquals(targetSnapshot, realChore.getTableQuotaSnapshot(table));
  }

  @Test
  public void testRetryTableDisableOnFailure() throws Exception {
    Admin admin = mock(Admin.class);
    when(conn.getAdmin()).thenReturn(admin);
    SpaceQuotaSnapshotNotifier notifier = mock(SpaceQuotaSnapshotNotifier.class);
    QuotaObserverChore realChore = new QuotaObserverChore(conn, HBaseConfiguration.create(),
      notifier, mock(MasterQuotaManager.class), mock(Stoppable.class), null);
    realChore.initializeSnapshotStores(Collections.emptyMap());

    TableName table = TableName.valueOf("retryDisable");
    SpaceQuotaSnapshot observantSnapshot =
      new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 512L, 1024L);
    SpaceQuotaSnapshot targetSnapshot =
      new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.DISABLE), 2048L, 1024L);
    realChore.setTableQuotaSnapshot(table, observantSnapshot);

    doThrow(new IOException("injected disable failure")).doNothing().when(admin)
      .disableTable(table);

    // The first update fails to disable the table, so the in-memory snapshot must not advance.
    assertThrows(IOException.class,
      () -> realChore.updateTableQuota(table, observantSnapshot, targetSnapshot));
    assertEquals(observantSnapshot, realChore.getTableQuotaSnapshot(table));
    verify(admin, times(1)).disableTable(table);

    // The next chore run recomputes the same transition and must retry the disable.
    realChore.updateTableQuota(table, observantSnapshot, targetSnapshot);
    verify(admin, times(2)).disableTable(table);
    assertEquals(targetSnapshot, realChore.getTableQuotaSnapshot(table));
  }
}
