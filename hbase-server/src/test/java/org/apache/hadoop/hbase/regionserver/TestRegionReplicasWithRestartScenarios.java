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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RegionServerTests.class, MediumTests.class})
public class TestRegionReplicasWithRestartScenarios {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicasWithRestartScenarios.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegionReplicasWithRestartScenarios.class);

  @Rule public TestName name = new TestName();

  private static final int NB_SERVERS = 3;
  private Table table;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Reduce the hdfs block size and prefetch to trigger the file-link reopen
    // when the file is moved to archive (e.g. compaction)
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 8192);
    HTU.getConfiguration().setInt(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 1);
    HTU.getConfiguration().setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 128 * 1024 * 1024);
    HTU.getConfiguration().setInt(">hbase.master.wait.on.regionservers.mintostart", 3);
    HTU.startMiniCluster(NB_SERVERS);
  }

  @Before
  public void before() throws IOException {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    // Create table then get the single region for our new table.
    this.table = createTableDirectlyFromHTD(tableName);
  }

  @After
  public void after() throws IOException {
    this.table.close();
  }

  private static Table createTableDirectlyFromHTD(final TableName tableName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setRegionReplication(3);
    return HTU.createTable(builder.build(), new byte[][] { f }, getSplits(20),
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
  public void testRegionReplicasCreated() throws Exception {
    Collection<HRegion> onlineRegions = getRS().getOnlineRegionsLocalContext();
    boolean res = checkDuplicates(onlineRegions);
    assertFalse(res);
    Collection<HRegion> onlineRegions2 = getSecondaryRS().getOnlineRegionsLocalContext();
    res = checkDuplicates(onlineRegions2);
    assertFalse(res);
    Collection<HRegion> onlineRegions3 = getTertiaryRS().getOnlineRegionsLocalContext();
    checkDuplicates(onlineRegions3);
    assertFalse(res);
    int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
    assertEquals(61, totalRegions);
  }

  private boolean checkDuplicates(Collection<HRegion> onlineRegions3) throws Exception {
    ArrayList<Region> copyOfRegion = new ArrayList<Region>(onlineRegions3);
    for (Region region : copyOfRegion) {
      RegionInfo regionInfo = region.getRegionInfo();
      RegionInfo regionInfoForReplica =
          RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo);
      int i = 0;
      for (Region actualRegion : onlineRegions3) {
        if (regionInfoForReplica.equals(
          RegionReplicaUtil.getRegionInfoForDefaultReplica(actualRegion.getRegionInfo()))) {
          i++;
          if (i > 1) {
            LOG.info("Duplicate found " + actualRegion.getRegionInfo() + " " +
                region.getRegionInfo());
            assertTrue(Bytes.equals(region.getRegionInfo().getStartKey(),
              actualRegion.getRegionInfo().getStartKey()));
            assertTrue(Bytes.equals(region.getRegionInfo().getEndKey(),
              actualRegion.getRegionInfo().getEndKey()));
            return true;
          }
        }
      }
    }
    return false;
  }
}
