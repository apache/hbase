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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-20634
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestServerCrashProcedureStuck {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestServerCrashProcedureStuck.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("test");

  private static byte[] CF = Bytes.toBytes("cf");

  private static CountDownLatch ARRIVE = new CountDownLatch(1);

  private static CountDownLatch RESUME = new CountDownLatch(1);

  public enum DummyState {
    STATE
  }

  public static final class DummyRegionProcedure
      extends AbstractStateMachineRegionProcedure<DummyState> {

    public DummyRegionProcedure() {
    }

    public DummyRegionProcedure(MasterProcedureEnv env, RegionInfo hri) {
      super(env, hri);
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.REGION_EDIT;
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, DummyState state)
        throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
      ARRIVE.countDown();
      RESUME.await();
      return Flow.NO_MORE_STATE;
    }

    @Override
    protected void rollbackState(MasterProcedureEnv env, DummyState state)
        throws IOException, InterruptedException {
    }

    @Override
    protected DummyState getState(int stateId) {
      return DummyState.STATE;
    }

    @Override
    protected int getStateId(DummyState state) {
      return 0;
    }

    @Override
    protected DummyState getInitialState() {
      return DummyState.STATE;
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    UTIL.getAdmin().balancerSwitch(false, true);
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    RegionServerThread rsThread = null;
    for (RegionServerThread t : UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      if (!t.getRegionServer().getRegions(TABLE_NAME).isEmpty()) {
        rsThread = t;
        break;
      }
    }
    HRegionServer rs = rsThread.getRegionServer();
    RegionInfo hri = rs.getRegions(TABLE_NAME).get(0).getRegionInfo();
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> executor = master.getMasterProcedureExecutor();
    DummyRegionProcedure proc = new DummyRegionProcedure(executor.getEnvironment(), hri);
    long procId = master.getMasterProcedureExecutor().submitProcedure(proc);
    ARRIVE.await();
    try (AsyncConnection conn =
      ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get()) {
      AsyncAdmin admin = conn.getAdmin();
      CompletableFuture<Void> future = admin.move(hri.getRegionName());
      rs.abort("For testing!");

      UTIL.waitFor(30000,
        () -> executor.getProcedures().stream().filter(p -> p instanceof AssignProcedure)
          .map(p -> (AssignProcedure) p)
          .anyMatch(p -> Bytes.equals(hri.getRegionName(), p.getRegionInfo().getRegionName())));
      RESUME.countDown();
      UTIL.waitFor(30000, () -> executor.isFinished(procId));
      // see whether the move region procedure can finish properly
      future.get(30, TimeUnit.SECONDS);
    }
  }
}
