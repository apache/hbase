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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocateType;
import org.apache.hadoop.hbase.master.MetaLocationCache.CacheHolder;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestBackupMasterSyncRoot {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupMasterSyncRoot.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(MetaLocationCache.SYNC_INTERVAL_SECONDS, 1);
    StartMiniClusterOption option =
      StartMiniClusterOption.builder().numMasters(2).numRegionServers(3).build();
    UTIL.startMiniCluster(option);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSync() throws Exception {
    HMaster active = UTIL.getHBaseCluster().getMaster();
    AssignmentManager activeAM = active.getAssignmentManager();
    RegionInfo meta =
      activeAM.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME).get(0);
    ServerName expected = activeAM.getRegionStates().getRegionStateNode(meta).getRegionLocation();
    HMaster backup = UTIL.getHBaseCluster().getMasterThreads().stream().map(t -> t.getMaster())
      .filter(h -> h != active).findFirst().get();
    MetaLocationCache cache = backup.getMetaLocationCache();
    UTIL.waitFor(10000, () -> {
      RegionLocations loc = cache.locateMeta(HConstants.EMPTY_START_ROW, RegionLocateType.CURRENT);
      return loc != null && loc.getRegionLocation().getServerName().equals(expected);
    });
    CacheHolder currentHolder = cache.holder.get();
    assertNotNull(currentHolder);
    long lastSyncSeqId = currentHolder.lastSyncSeqId;
    long currentMVCC = active.masterRegion.getReadPoint();
    assertTrue(lastSyncSeqId <= currentMVCC);
    TableName table = TableName.valueOf("test");
    UTIL.createTable(table, Bytes.toBytes("f"));
    UTIL.waitTableAvailable(table);
    long newMVCC = active.masterRegion.getReadPoint();
    // we have created several new procedures so the read point should be advanced
    assertTrue(newMVCC > currentMVCC);
    Thread.sleep(3000);
    // should not change since the root family is not changed
    assertSame(currentHolder, cache.holder.get());

    ServerName newExpected =
      UTIL.getAdmin().getRegionServers().stream().filter(s -> !s.equals(expected)).findAny().get();
    active.getAssignmentManager().moveAsync(new RegionPlan(meta, expected, newExpected)).get();
    assertEquals(newExpected,
      activeAM.getRegionStates().getRegionStateNode(meta).getRegionLocation());
    UTIL.waitFor(10000, () -> {
      RegionLocations loc = cache.locateMeta(HConstants.EMPTY_START_ROW, RegionLocateType.CURRENT);
      return loc != null && loc.getRegionLocation().getServerName().equals(newExpected);
    });
    CacheHolder newHolder = cache.holder.get();
    // this time the cache should be changed
    assertNotSame(currentHolder, newHolder);
    assertTrue(newHolder.lastSyncSeqId > newMVCC);
  }
}
