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
package org.apache.hadoop.hbase.master.assignment;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
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
import static org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil.insertData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Category({ MasterTests.class, MediumTests.class})
public class TestRegionSplitAndSeparateChildren {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionSplitAndSeparateChildren.class);

  private static final Logger LOG = LoggerFactory.getLogger(
    TestRegionSplitAndSeparateChildren.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static String columnFamilyName = "cf";

  private static final int startRowNum = 11;
  private static final int rowCount = 60;

  @Rule
  public TestName name = new TestName();

  private static void setupConf(Configuration conf) {
    // enable automatically separate child regions
    conf.setBoolean(HConstants.HBASE_ENABLE_SEPARATE_CHILD_REGIONS, true);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    StartMiniClusterOption option =
      StartMiniClusterOption.builder().numMasters(1).numRegionServers(3).numDataNodes(3).build();
    UTIL.startMiniCluster(option);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Before
  public void setup() throws Exception {
    // Turn off the meta scanner so it don't remove parent on us.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
    // Disable compaction.
    for (int i = 0; i < UTIL.getHBaseCluster().getLiveRegionServerThreads().size(); i++) {
      UTIL.getHBaseCluster().getRegionServer(i).getCompactSplitThread().switchCompaction(
        false);
    }
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor htd : UTIL.getAdmin().listTableDescriptors()) {
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testSplitTableRegionAndSeparateChildRegions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo[] regions =
      MasterProcedureTestingUtility.createTable(procExec, tableName, null, columnFamilyName);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    assertTrue("not able to split table",
      UTIL.getHBaseCluster().getRegions(tableName).size() == 2);

    //disable table
    UTIL.getAdmin().disableTable(tableName);
    Thread.sleep(500);

    //stop master
    UTIL.getHBaseCluster().stopMaster(0);
    UTIL.getHBaseCluster().waitOnMaster(0);
    Thread.sleep(500);

    //restart master
    JVMClusterUtil.MasterThread t = UTIL.getHBaseCluster().startMaster();
    Thread.sleep(500);

    UTIL.invalidateConnection();
    // enable table
    UTIL.getAdmin().enableTable(tableName);
    Thread.sleep(500);

    List<HRegion> tableRegions = UTIL.getHBaseCluster().getRegions(tableName);
    assertEquals("Table region not correct.", 2, tableRegions.size());
    Map<RegionInfo, ServerName> regionInfoMap = UTIL.getHBaseCluster().getMaster()
      .getAssignmentManager().getRegionStates().getRegionAssignments();
    assertNotEquals(regionInfoMap.get(tableRegions.get(0).getRegionInfo()),
      regionInfoMap.get(tableRegions.get(1).getRegionInfo()));
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
