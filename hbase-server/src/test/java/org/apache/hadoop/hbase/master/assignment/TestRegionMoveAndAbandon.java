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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Testcase for HBASE-20792.
 */
@Category({ MediumTests.class, MasterTests.class })
public class TestRegionMoveAndAbandon {
  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMoveAndAbandon.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionMoveAndAbandon.class);

  @Rule
  public TestName name = new TestName();

  private HBaseTestingUtility UTIL;
  private MiniHBaseCluster cluster;
  private MiniZooKeeperCluster zkCluster;
  private HRegionServer rs1;
  private HRegionServer rs2;
  private RegionInfo regionInfo;

  @Before
  public void setup() throws Exception {
    UTIL = new HBaseTestingUtility();
    zkCluster = UTIL.startMiniZKCluster();
    StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(2).build();
    cluster = UTIL.startMiniHBaseCluster(option);
    rs1 = cluster.getRegionServer(0);
    rs2 = cluster.getRegionServer(1);
    assertEquals(2, cluster.getRegionServerThreads().size());
    // We'll use hbase:namespace for our testing
    UTIL.waitTableAvailable(TableName.NAMESPACE_TABLE_NAME, 30_000);
    regionInfo =
      Iterables.getOnlyElement(cluster.getRegions(TableName.NAMESPACE_TABLE_NAME)).getRegionInfo();
  }

  @After
  public void teardown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    if (zkCluster != null) {
      zkCluster.shutdown();
      zkCluster = null;
    }
  }

  @Test
  public void test() throws Exception {
    LOG.info("Moving {} to {}", regionInfo, rs2.getServerName());
    // Move to RS2
    UTIL.moveRegionAndWait(regionInfo, rs2.getServerName());
    LOG.info("Moving {} to {}", regionInfo, rs1.getServerName());
    // Move to RS1
    UTIL.moveRegionAndWait(regionInfo, rs1.getServerName());
    LOG.info("Killing RS {}", rs1.getServerName());
    // Stop RS1
    cluster.killRegionServer(rs1.getServerName());
    UTIL.waitFor(30_000, () -> rs1.isStopped() && !rs1.isAlive());
    // Region should get moved to RS2
    UTIL.waitTableAvailable(TableName.NAMESPACE_TABLE_NAME, 60_000);
    // Restart the master
    LOG.info("Killing master {}", cluster.getMaster().getServerName());
    cluster.killMaster(cluster.getMaster().getServerName());
    // Stop RS2
    LOG.info("Killing RS {}", rs2.getServerName());
    cluster.killRegionServer(rs2.getServerName());
    UTIL.waitFor(30_000, () -> rs2.isStopped() && !rs2.isAlive());
    UTIL.waitFor(30_000, () -> rs1.isStopped() && !rs1.isAlive());
    // make sure none of regionserver threads is alive.
    UTIL.waitFor(30_000, () ->
      UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().isEmpty());
    // Start up everything again
    LOG.info("Starting cluster");
    UTIL.getMiniHBaseCluster().startMaster();
    UTIL.invalidateConnection();
    UTIL.ensureSomeRegionServersAvailable(2);

    UTIL.waitFor(30_000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        try (Table nsTable = UTIL.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME)) {
          // Doesn't matter what we're getting. We just want to make sure we can access the region
          nsTable.get(new Get(Bytes.toBytes("a")));
          return true;
        }
      }
    });
  }
}
