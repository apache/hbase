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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicasAreDistributed {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicasAreDistributed.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionReplicasAreDistributed.class);

  private static final int NB_SERVERS = 3;
  private static Table table;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;
  Map<ServerName, Collection<RegionInfo>> serverVsOnlineRegions;
  Map<ServerName, Collection<RegionInfo>> serverVsOnlineRegions2;
  Map<ServerName, Collection<RegionInfo>> serverVsOnlineRegions3;
  Map<ServerName, Collection<RegionInfo>> serverVsOnlineRegions4;

  @BeforeClass
  public static void before() throws Exception {
    HTU.getConfiguration().setInt(">hbase.master.wait.on.regionservers.mintostart", 3);

    HTU.startMiniCluster(NB_SERVERS);
    Thread.sleep(3000);
    final TableName tableName =
        TableName.valueOf(TestRegionReplicasAreDistributed.class.getSimpleName());

    // Create table then get the single region for our new table.
    createTableDirectlyFromHTD(tableName);
  }

  private static void createTableDirectlyFromHTD(final TableName tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(3);
    // create a table with 3 replication

    table = HTU.createTable(htd, new byte[][] { f }, getSplits(20),
      new Configuration(HTU.getConfiguration()));
  }

  private static byte[][] getSplits(int numRegions) {
    RegionSplitter.UniformSplit split = new RegionSplitter.UniformSplit();
    split.setFirstRow(Bytes.toBytes(0L));
    split.setLastRow(Bytes.toBytes(Long.MAX_VALUE));
    return split.split(numRegions);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = false;
    table.close();
    HTU.shutdownMiniCluster();
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private HRegionServer getSecondaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(1);
  }

  private HRegionServer getTertiaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(2);
  }

  @Test
  public void testRegionReplicasCreatedAreDistributed() throws Exception {
    try {
      checkAndAssertRegionDistribution(false);
      // now diesbale and enable the table again. It should be truly distributed
      HTU.getAdmin().disableTable(table.getName());
      LOG.info("Disabled the table " + table.getName());
      LOG.info("enabling the table " + table.getName());
      HTU.getAdmin().enableTable(table.getName());
      LOG.info("Enabled the table " + table.getName());
      boolean res = checkAndAssertRegionDistribution(true);
      assertTrue("Region retainment not done ", res);
    } finally {
      HTU.getAdmin().disableTable(table.getName());
      HTU.getAdmin().deleteTable(table.getName());
    }
  }

  private boolean checkAndAssertRegionDistribution(boolean checkfourth) throws Exception {
    Collection<RegionInfo> onlineRegions =
        new ArrayList<RegionInfo>(getRS().getOnlineRegionsLocalContext().size());
    for (HRegion region : getRS().getOnlineRegionsLocalContext()) {
      onlineRegions.add(region.getRegionInfo());
    }
    if (this.serverVsOnlineRegions == null) {
      this.serverVsOnlineRegions = new HashMap<ServerName, Collection<RegionInfo>>();
      this.serverVsOnlineRegions.put(getRS().getServerName(), onlineRegions);
    } else {
      Collection<RegionInfo> existingRegions =
          new ArrayList<RegionInfo>(this.serverVsOnlineRegions.get(getRS().getServerName()));
      LOG.info("Count is " + existingRegions.size() + " " + onlineRegions.size());
      for (RegionInfo existingRegion : existingRegions) {
        if (!onlineRegions.contains(existingRegion)) {
          return false;
        }
      }
    }
    Collection<RegionInfo> onlineRegions2 =
        new ArrayList<RegionInfo>(getSecondaryRS().getOnlineRegionsLocalContext().size());
    for (HRegion region : getSecondaryRS().getOnlineRegionsLocalContext()) {
      onlineRegions2.add(region.getRegionInfo());
    }
    if (this.serverVsOnlineRegions2 == null) {
      this.serverVsOnlineRegions2 = new HashMap<ServerName, Collection<RegionInfo>>();
      this.serverVsOnlineRegions2.put(getSecondaryRS().getServerName(), onlineRegions2);
    } else {
      Collection<RegionInfo> existingRegions = new ArrayList<RegionInfo>(
          this.serverVsOnlineRegions2.get(getSecondaryRS().getServerName()));
      LOG.info("Count is " + existingRegions.size() + " " + onlineRegions2.size());
      for (RegionInfo existingRegion : existingRegions) {
        if (!onlineRegions2.contains(existingRegion)) {
          return false;
        }
      }
    }
    Collection<RegionInfo> onlineRegions3 =
        new ArrayList<RegionInfo>(getTertiaryRS().getOnlineRegionsLocalContext().size());
    for (HRegion region : getTertiaryRS().getOnlineRegionsLocalContext()) {
      onlineRegions3.add(region.getRegionInfo());
    }
    if (this.serverVsOnlineRegions3 == null) {
      this.serverVsOnlineRegions3 = new HashMap<ServerName, Collection<RegionInfo>>();
      this.serverVsOnlineRegions3.put(getTertiaryRS().getServerName(), onlineRegions3);
    } else {
      Collection<RegionInfo> existingRegions = new ArrayList<RegionInfo>(
          this.serverVsOnlineRegions3.get(getTertiaryRS().getServerName()));
      LOG.info("Count is " + existingRegions.size() + " " + onlineRegions3.size());
      for (RegionInfo existingRegion : existingRegions) {
        if (!onlineRegions3.contains(existingRegion)) {
          return false;
        }
      }
    }
    // META and namespace to be added
    return true;
  }
}
