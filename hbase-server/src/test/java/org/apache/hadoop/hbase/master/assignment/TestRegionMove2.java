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

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Like TestRegionMove in regionserver package but in here in this package so I can get access to
 * Procedure internals to mess with the assignment to manufacture states seen out on clusters.
 */
@Category({LargeTests.class})
public class TestRegionMove2 {
  private final static Logger LOG = LoggerFactory.getLogger(TestRegionMove2.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionMove2.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void startCluster() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test that we make it through to the end if parent Region is offlined between start of this
   * Move and when we go to run the move UnassignProcedure.
   */
  @Test
  public void testMoveOfRegionOfflinedPostStart() throws Exception {
    TableName tableName = TableName.valueOf(this.name.getMethodName());
    // Create a table with more than one region
    byte [] cf = Bytes.toBytes("cf");
    Table t = TEST_UTIL.createMultiRegionTable(tableName, cf, 10);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    HRegionServer rs1 = null;
    HRegionServer rs2 = null;
    List<RegionInfo> regionsOnRS1ForTable = null;
    try (Admin admin = TEST_UTIL.getAdmin()) {
      // Write an update to each region
      for (RegionInfo regionInfo : admin.getRegions(tableName)) {
        byte[] startKey = regionInfo.getStartKey();
        // StartKey of first region is "empty", which would throw an error if we try to Put that.
        byte[] rowKey =
            org.apache.hbase.thirdparty.com.google.common.primitives.Bytes.concat(startKey,
                Bytes.toBytes("1"));
        Put p = new Put(rowKey);
        p.addColumn(cf, Bytes.toBytes("q1"), Bytes.toBytes("value"));
        t.put(p);
      }

      // Get a Region which is on the first RS
      rs1 = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      rs2 = TEST_UTIL.getOtherRegionServer(rs1);
      regionsOnRS1ForTable = admin.getRegions(rs1.getServerName()).stream().
          filter((regionInfo) -> regionInfo.getTable().equals(tableName)).
          collect(Collectors.toList());
    }
    assertTrue("Expected to find at least one region for " + tableName + " on " +
        rs1.getServerName() + ", but found none", !regionsOnRS1ForTable.isEmpty());
    final RegionInfo regionToMove = regionsOnRS1ForTable.get(0);
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();

    // Try to move the region. HackedMoveRegionProcedure should intercede and mess up the region
    // state setting it to SPLIT when we run the UnassignProcedure part of move region.
    // Then when we go to do the unassignprocedure, we should notice the region-to-move is not
    // online.... spew some log, and then fast-track to the end of the unassign. The assign under
    // move will also notice that the parent is not-online but SPLIT and will skip it... so the
    // move will "succeed" but we won't have moved the region!
    RegionPlan rp = new RegionPlan(regionToMove, rs1.getServerName(), rs2.getServerName());
    MasterProcedureEnv env = master.getMasterProcedureExecutor().getEnvironment();
    HackedMoveRegionProcedure p = new HackedMoveRegionProcedure(env, rp);
    master.getMasterProcedureExecutor().submitProcedure(p);
    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), p);
    // Split should have been called.
    assertTrue(p.split.get());
    // The region should not have been moved!
    assertTrue(rs1.getOnlineRegion(regionToMove.getRegionName()) != null);
  }

  /**
   * Class just so we can mess around with RegionStateNode state at a particular point in the
   * Procedure to try and mess it up.
   */
  public static class HackedMoveRegionProcedure extends MoveRegionProcedure {
    /**
     * Set to true after we hack this regions RSN to SPLIT
     */
    public static AtomicBoolean split = new AtomicBoolean(false);

    // Required by the Procedure framework to create the procedure on replay
    public HackedMoveRegionProcedure() {
      super();
    }

    public HackedMoveRegionProcedure(MasterProcedureEnv env, RegionPlan plan)
        throws HBaseIOException {
      super(env, plan, false);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env,
        MasterProcedureProtos.MoveRegionState state) throws InterruptedException {
      Flow flow = null;
      switch (state) {
        case MOVE_REGION_UNASSIGN:
          // Just before the unassign, flip the state to SPLIT. The unassign should exit!
          RegionStates.RegionStateNode rsn =
              env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(getRegion());
          rsn.setState(RegionState.State.SPLIT);
          LOG.info("HACKED RSN, setting it to SPLIT: {}", rsn);
          split.set(true);
        default:
          flow = super.executeFromState(env, state);
      }
      return flow;
    }
  }
}
