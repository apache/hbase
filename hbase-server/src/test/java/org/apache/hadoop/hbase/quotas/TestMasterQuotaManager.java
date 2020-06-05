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
package org.apache.hadoop.hbase.quotas;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMasterQuotaManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterQuotaManager.class);

  @Test
  public void testUninitializedQuotaManangerDoesNotFail() {
    MasterServices masterServices = mock(MasterServices.class);
    MasterQuotaManager manager = new MasterQuotaManager(masterServices);
    manager.addRegionSize(null, 0, 0);
    assertNotNull(manager.snapshotRegionSizes());
  }

  private RegionInfo createRegionInfo(TableName tableName, byte[] startKey, byte[] endKey) {
    return RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey).build();
  }

  @Test
  public void testOldEntriesRemoved() {
    MasterServices masterServices = mock(MasterServices.class);
    MasterQuotaManager manager = new MasterQuotaManager(masterServices);
    manager.initializeRegionSizes();
    // Mock out some regions
    TableName tableName = TableName.valueOf("foo");
    RegionInfo region1 = createRegionInfo(tableName, null, toBytes("a"));
    RegionInfo region2 = createRegionInfo(tableName, toBytes("a"), toBytes("b"));
    RegionInfo region3 = createRegionInfo(tableName, toBytes("b"), toBytes("c"));
    RegionInfo region4 = createRegionInfo(tableName, toBytes("c"), toBytes("d"));
    RegionInfo region5 = createRegionInfo(tableName, toBytes("d"), null);

    final long size = 0;
    long time1 = 10;
    manager.addRegionSize(region1, size, time1);
    manager.addRegionSize(region2, size, time1);

    long time2 = 20;
    manager.addRegionSize(region3, size, time2);
    manager.addRegionSize(region4, size, time2);

    long time3 = 30;
    manager.addRegionSize(region5, size, time3);

    assertEquals(5, manager.snapshotRegionSizes().size());

    QuotaObserverChore chore = mock(QuotaObserverChore.class);
    // Prune nothing
    assertEquals(0, manager.pruneEntriesOlderThan(0, chore));
    assertEquals(5, manager.snapshotRegionSizes().size());
    assertEquals(0, manager.pruneEntriesOlderThan(10, chore));
    assertEquals(5, manager.snapshotRegionSizes().size());

    // Prune the elements at time1
    assertEquals(2, manager.pruneEntriesOlderThan(15, chore));
    assertEquals(3, manager.snapshotRegionSizes().size());

    // Prune the elements at time2
    assertEquals(2, manager.pruneEntriesOlderThan(30, chore));
    assertEquals(1, manager.snapshotRegionSizes().size());
  }
}
