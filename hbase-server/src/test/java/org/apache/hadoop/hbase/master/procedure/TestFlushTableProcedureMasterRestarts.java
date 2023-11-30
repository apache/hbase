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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestFlushTableProcedureMasterRestarts extends TestFlushTableProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFlushTableProcedureMasterRestarts.class);

  @Test
  public void testMasterRestarts() throws IOException {
    assertTableMemStoreNotEmpty();

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();
    FlushTableProcedure proc = new FlushTableProcedure(env, TABLE_NAME);
    long procId = procExec.submitProcedure(proc);
    TEST_UTIL.waitFor(5000, 1000, () -> proc.getState().getNumber() > 1);

    TEST_UTIL.getHBaseCluster().killMaster(master.getServerName());
    TEST_UTIL.getHBaseCluster().waitForMasterToStop(master.getServerName(), 30000);
    TEST_UTIL.getHBaseCluster().startMaster();
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();

    master = TEST_UTIL.getHBaseCluster().getMaster();
    procExec = master.getMasterProcedureExecutor();
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertTableMemStoreEmpty();
  }

  @Test
  public void testSkipRIT() throws IOException {
    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).get(0);

    TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
      .getRegionStateNode(region.getRegionInfo())
      .setState(RegionState.State.CLOSING, RegionState.State.OPEN);

    FlushRegionProcedure proc = new FlushRegionProcedure(region.getRegionInfo());
    TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().submitProcedure(proc);

    // wait for a time which is shorter than RSProcedureDispatcher delays
    TEST_UTIL.waitFor(5000, () -> proc.isFinished());
  }
}
