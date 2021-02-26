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

import static org.junit.Assert.assertFalse;

import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Testcase for HBASE-21490.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestLoadProcedureError {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLoadProcedureError.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("Load");

  private static volatile CountDownLatch ARRIVE;

  private static volatile boolean FINISH_PROC;

  private static volatile boolean FAIL_LOAD;

  public static final class TestProcedure extends NoopProcedure<MasterProcedureEnv>
      implements TableProcedureInterface {

    @Override
    protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      if (ARRIVE != null) {
        ARRIVE.countDown();
        ARRIVE = null;
      }
      if (FINISH_PROC) {
        return null;
      }
      setTimeout(1000);
      setState(ProcedureState.WAITING_TIMEOUT);
      throw new ProcedureSuspendedException();
    }

    @Override
    protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
      setState(ProcedureState.RUNNABLE);
      env.getProcedureScheduler().addBack(this);
      return false;
    }

    @Override
    protected void afterReplay(MasterProcedureEnv env) {
      if (FAIL_LOAD) {
        throw new RuntimeException("Inject error");
      }
    }

    @Override
    public TableName getTableName() {
      return NAME;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.READ;
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void waitNoMaster() {
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getLiveMasterThreads().isEmpty());
  }

  @Test
  public void testLoadError() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    ARRIVE = new CountDownLatch(1);
    long procId = procExec.submitProcedure(new TestProcedure());
    ARRIVE.await();
    FAIL_LOAD = true;
    // do not persist the store tracker
    UTIL.getMiniHBaseCluster().getMaster().getProcedureStore().stop(true);
    UTIL.getMiniHBaseCluster().getMaster().abort("for testing");
    waitNoMaster();
    // restart twice, and should fail twice, as we will throw an exception in the afterReplay above
    // in order to reproduce the problem in HBASE-21490 stably, here we will wait until a master is
    // fully done, before starting the new master, otherwise the new master may start too early and
    // call recoverLease on the proc wal files and cause we fail to persist the store tracker when
    // shutting down
    UTIL.getMiniHBaseCluster().startMaster();
    waitNoMaster();
    UTIL.getMiniHBaseCluster().startMaster();
    waitNoMaster();
    FAIL_LOAD = false;
    HMaster master = UTIL.getMiniHBaseCluster().startMaster().getMaster();
    UTIL.waitFor(30000, () -> master.isActiveMaster() && master.isInitialized());
    // assert the procedure is still there and not finished yet
    TestProcedure proc = (TestProcedure) master.getMasterProcedureExecutor().getProcedure(procId);
    assertFalse(proc.isFinished());
    FINISH_PROC = true;
    UTIL.waitFor(30000, () -> proc.isFinished());
  }
}
