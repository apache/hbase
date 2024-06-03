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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterMetricsWrapper {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterMetricsWrapper.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterMetricsWrapper.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final int NUM_RS = 4;

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster(NUM_RS);
  }

  @AfterClass
  public static void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testInfo() throws IOException {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MetricsMasterWrapperImpl info = new MetricsMasterWrapperImpl(master);
    assertEquals(master.getRegionNormalizerManager().getSplitPlanCount(), info.getSplitPlanCount(),
      0);
    assertEquals(master.getRegionNormalizerManager().getMergePlanCount(), info.getMergePlanCount(),
      0);
    assertEquals(master.getAverageLoad(), info.getAverageLoad(), 0);
    assertEquals(master.getClusterId(), info.getClusterId());
    assertEquals(master.getMasterActiveTime(), info.getActiveTime());
    assertEquals(master.getMasterStartTime(), info.getStartTime());
    assertEquals(master.getMasterCoprocessors().length, info.getCoprocessors().length);
    assertEquals(master.getServerManager().getOnlineServersList().size(),
      info.getNumRegionServers());
    assertEquals(master.getMasterWalManager().getOldWALsDirSize(), info.getOldWALsDirSize());
    int regionServerCount = NUM_RS;
    assertEquals(regionServerCount, info.getNumRegionServers());

    String zkServers = info.getZookeeperQuorum();
    assertEquals(zkServers.split(",").length, TEST_UTIL.getZkCluster().getZooKeeperServerNum());

    final int index = 3;
    LOG.info("Stopping " + TEST_UTIL.getMiniHBaseCluster().getRegionServer(index));
    TEST_UTIL.getMiniHBaseCluster().stopRegionServer(index, false);
    TEST_UTIL.getMiniHBaseCluster().waitOnRegionServer(index);
    // We stopped the regionserver but could take a while for the master to notice it so hang here
    // until it does... then move forward to see if metrics wrapper notices.
    while (
      TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServers().size()
          == regionServerCount
    ) {
      Threads.sleep(10);
    }
    assertEquals(regionServerCount - 1, info.getNumRegionServers());
    assertEquals(1, info.getNumDeadRegionServers());
    // now we do not expose this information as WALProcedureStore is not the only ProcedureStore
    // implementation any more.
    assertEquals(0, info.getNumWALFiles());
    // We decommission the first online region server and verify the metrics.
    TEST_UTIL.getMiniHBaseCluster().getMaster().decommissionRegionServers(
      master.getServerManager().getOnlineServersList().subList(0, 1), false);
    assertEquals(1, info.getNumDrainingRegionServers());
    assertEquals(master.getServerManager().getOnlineServersList().get(0).toString(),
      info.getDrainingRegionServers());
  }

  @Test
  public void testQuotaSnapshotConversion() {
    MetricsMasterWrapperImpl info =
      new MetricsMasterWrapperImpl(TEST_UTIL.getHBaseCluster().getMaster());
    assertEquals(new SimpleImmutableEntry<Long, Long>(1024L, 2048L), info
      .convertSnapshot(new SpaceQuotaSnapshot(SpaceQuotaStatus.notInViolation(), 1024L, 2048L)));
    assertEquals(new SimpleImmutableEntry<Long, Long>(4096L, 2048L), info.convertSnapshot(
      new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_INSERTS), 4096L, 2048L)));
  }

  /**
   * tests online and offline region number
   */
  @Test
  public void testOfflineRegion() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MetricsMasterWrapperImpl info = new MetricsMasterWrapperImpl(master);
    TableName table = TableName.valueOf("testRegionNumber");
    try {
      RegionInfo hri;
      byte[] FAMILY = Bytes.toBytes("FAMILY");
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
      TEST_UTIL.getAdmin().createTable(tableDescriptor, Bytes.toBytes("A"), Bytes.toBytes("Z"), 5);

      // wait till the table is assigned
      long timeoutTime = EnvironmentEdgeManager.currentTime() + 1000;
      while (true) {
        List<RegionInfo> regions =
          master.getAssignmentManager().getRegionStates().getRegionsOfTable(table);
        if (regions.size() > 3) {
          hri = regions.get(2);
          break;
        }
        long now = EnvironmentEdgeManager.currentTime();
        if (now > timeoutTime) {
          fail("Could not find an online region");
        }
        Thread.sleep(10);
      }

      PairOfSameType<Integer> regionNumberPair = info.getRegionCounts();
      assertEquals(5, regionNumberPair.getFirst().intValue());
      assertEquals(0, regionNumberPair.getSecond().intValue());

      TEST_UTIL.getAdmin().offline(hri.getRegionName());

      timeoutTime = EnvironmentEdgeManager.currentTime() + 800;
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      while (true) {
        if (
          regionStates.getRegionByStateOfTable(table).get(RegionState.State.OFFLINE).contains(hri)
        ) {
          break;
        }
        long now = EnvironmentEdgeManager.currentTime();
        if (now > timeoutTime) {
          fail("Failed to offline the region in time");
          break;
        }
        Thread.sleep(10);
      }
      regionNumberPair = info.getRegionCounts();
      assertEquals(4, regionNumberPair.getFirst().intValue());
      assertEquals(1, regionNumberPair.getSecond().intValue());
    } finally {
      TEST_UTIL.deleteTable(table);
    }
  }
}
