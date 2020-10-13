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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, MediumTests.class })
public class TestFailedMetaReplicaAssigment {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFailedMetaReplicaAssigment.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    // using our rigged master, to force a failed meta replica assignment when start up master
    // this test can be removed once we remove the HConstants.META_REPLICAS_NUM config.
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.META_REPLICAS_NUM, 3);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numAlwaysStandByMasters(1)
      .numMasters(1).numRegionServers(1).masterClass(BrokenMetaReplicaMaster.class).build();
    TEST_UTIL.startMiniCluster(option);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFailedReplicaAssignment() throws InterruptedException {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    // waiting for master to come up
    TEST_UTIL.waitFor(30000, () -> master.isInitialized());

    AssignmentManager am = master.getAssignmentManager();
    // showing one of the replicas got assigned
    RegionInfo metaReplicaHri =
      RegionReplicaUtil.getRegionInfoForReplica(RegionInfoBuilder.FIRST_META_REGIONINFO, 1);
    // we use assignAsync so we need to wait a bit
    TEST_UTIL.waitFor(30000, () -> {
      RegionStateNode metaReplicaRegionNode =
        am.getRegionStates().getOrCreateRegionStateNode(metaReplicaHri);
      return metaReplicaRegionNode.getRegionLocation() != null;
    });
    // showing one of the replicas failed to be assigned
    RegionInfo metaReplicaHri2 =
      RegionReplicaUtil.getRegionInfoForReplica(RegionInfoBuilder.FIRST_META_REGIONINFO, 2);
    RegionStateNode metaReplicaRegionNode2 =
      am.getRegionStates().getOrCreateRegionStateNode(metaReplicaHri2);
    // wait for several seconds to make sure that it is not assigned
    for (int i = 0; i < 3; i++) {
      Thread.sleep(2000);
      assertNull(metaReplicaRegionNode2.getRegionLocation());
    }

    // showing master is active and running
    assertFalse(master.isStopping());
    assertFalse(master.isStopped());
    assertTrue(master.isActiveMaster());
  }

  public static class BrokenTransitRegionStateProcedure extends TransitRegionStateProcedure {

    public BrokenTransitRegionStateProcedure() {
      super(null, null, null, false, TransitionType.ASSIGN);
    }

    public BrokenTransitRegionStateProcedure(MasterProcedureEnv env, RegionInfo hri) {
      super(env, hri, null, false, TransitionType.ASSIGN);
    }

    @Override
    protected Procedure[] execute(MasterProcedureEnv env)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
      throw new ProcedureSuspendedException("Never end procedure!");
    }
  }

  public static class BrokenMetaReplicaMaster extends HMaster {
    public BrokenMetaReplicaMaster(final Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public AssignmentManager createAssignmentManager(MasterServices master) {
      return new BrokenMasterMetaAssignmentManager(master);
    }
  }

  public static class BrokenMasterMetaAssignmentManager extends AssignmentManager {
    MasterServices master;

    public BrokenMasterMetaAssignmentManager(final MasterServices master) {
      super(master);
      this.master = master;
    }

    @Override
    public TransitRegionStateProcedure[] createAssignProcedures(List<RegionInfo> hris) {
      List<TransitRegionStateProcedure> procs = new ArrayList<>();
      for (RegionInfo hri : hris) {
        if (hri.isMetaRegion() && hri.getReplicaId() == 2) {
          RegionStateNode regionNode = getRegionStates().getOrCreateRegionStateNode(hri);
          regionNode.lock();
          try {
            procs.add(regionNode.setProcedure(new BrokenTransitRegionStateProcedure(
              master.getMasterProcedureExecutor().getEnvironment(), hri)));
          } finally {
            regionNode.unlock();
          }
        } else {
          procs.add(super.createAssignProcedures(Collections.singletonList(hri))[0]);
        }
      }
      return procs.toArray(new TransitRegionStateProcedure[0]);
    }
  }
}
