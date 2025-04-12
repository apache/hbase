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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure.TransitionType;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Testcase for HBASE-29259
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestTRSPPersistUninitializedSubProc {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTRSPPersistUninitializedSubProc.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static byte[] CF = Bytes.toBytes("cf");

  private static TableName TN = TableName.valueOf("tn");

  public static class TRSPForTest extends TransitRegionStateProcedure {

    private boolean injected = false;

    public TRSPForTest() {
    }

    public TRSPForTest(MasterProcedureEnv env, RegionInfo hri, ServerName assignCandidate,
      boolean forceNewPlan, TransitionType type) {
      super(env, hri, assignCandidate, forceNewPlan, type);
    }

    @Override
    protected Procedure[] execute(MasterProcedureEnv env)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
      Procedure[] subProcs = super.execute(env);
      if (!injected && subProcs != null && subProcs[0] instanceof CloseRegionProcedure) {
        injected = true;
        ServerName sn = ((CloseRegionProcedure) subProcs[0]).targetServer;
        env.getMasterServices().getServerManager().expireServer(sn);
        try {
          UTIL.waitFor(15000, () -> env.getMasterServices().getProcedures().stream().anyMatch(
            p -> p instanceof ServerCrashProcedure && p.getState() != ProcedureState.INITIALIZING));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        // sleep 10 seconds to let the SCP interrupt the TRSP, where we will call TRSP.serverCrashed
        Thread.sleep(10000);
      }
      return subProcs;
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(2);
    UTIL.getAdmin().balancerSwitch(false, true);
    UTIL.createTable(TN, CF);
    UTIL.waitTableAvailable(TN);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testServerCrash() throws Exception {
    HMaster master = UTIL.getHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    RegionInfo region = UTIL.getAdmin().getRegions(TN).get(0);
    RegionStateNode rsn =
      master.getAssignmentManager().getRegionStates().getRegionStateNode(region);
    TRSPForTest trsp =
      new TRSPForTest(procExec.getEnvironment(), region, null, false, TransitionType.REOPEN);
    // attach it to RegionStateNode, to simulate normal reopen
    rsn.setProcedure(trsp);
    procExec.submitProcedure(trsp);
    ProcedureTestingUtility.waitProcedure(procExec, trsp);
    // make sure we do not store invalid procedure to procedure store
    ProcedureTestingUtility.restart(procExec);
  }
}
