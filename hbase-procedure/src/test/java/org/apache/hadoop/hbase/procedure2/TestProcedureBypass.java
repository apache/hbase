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
package org.apache.hadoop.hbase.procedure2;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;


@Category({MasterTests.class, SmallTests.class})
public class TestProcedureBypass {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
      .forClass(TestProcedureBypass.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureBypass.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;

  private static TestProcEnv procEnv;
  private static ProcedureStore procStore;

  private static ProcedureExecutor<TestProcEnv> procExecutor;

  private static HBaseCommonTestingUtility htu;

  private static FileSystem fs;
  private static Path testDir;
  private static Path logDir;

  private static class TestProcEnv {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    htu = new HBaseCommonTestingUtility();

    // NOTE: The executor will be created by each test
    procEnv = new TestProcEnv();
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<>(htu.getConfiguration(), procEnv,
        procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    ProcedureTestingUtility
        .initAndStartWorkers(procExecutor, PROCEDURE_EXECUTOR_SLOTS, true);
  }

  @Test
  public void testBypassSuspendProcedure() throws Exception {
    final SuspendProcedure proc = new SuspendProcedure();
    long id = procExecutor.submitProcedure(proc);
    Thread.sleep(500);
    //bypass the procedure
    assertTrue(procExecutor.bypassProcedure(id, 30000, false, false));
    htu.waitFor(5000, () -> proc.isSuccess() && proc.isBypass());
    LOG.info("{} finished", proc);
  }

  @Test
  public void testStuckProcedure() throws Exception {
    final StuckProcedure proc = new StuckProcedure();
    long id = procExecutor.submitProcedure(proc);
    Thread.sleep(500);
    //bypass the procedure
    assertTrue(procExecutor.bypassProcedure(id, 1000, true, false));
    //Since the procedure is stuck there, we need to restart the executor to recovery.
    ProcedureTestingUtility.restart(procExecutor);
    htu.waitFor(5000, () -> proc.isSuccess() && proc.isBypass());
    LOG.info("{} finished", proc);
  }

  @Test
  public void testBypassingProcedureWithParent() throws Exception {
    final RootProcedure proc = new RootProcedure();
    long rootId = procExecutor.submitProcedure(proc);
    htu.waitFor(5000, () -> procExecutor.getProcedures().stream()
      .filter(p -> p.getParentProcId() == rootId).collect(Collectors.toList())
      .size() > 0);
    SuspendProcedure suspendProcedure = (SuspendProcedure)procExecutor.getProcedures().stream()
        .filter(p -> p.getParentProcId() == rootId).collect(Collectors.toList()).get(0);
    assertTrue(procExecutor.bypassProcedure(suspendProcedure.getProcId(), 1000, false, false));
    htu.waitFor(5000, () -> proc.isSuccess() && proc.isBypass());
    LOG.info("{} finished", proc);
  }

  @Test
  public void testBypassingStuckStateMachineProcedure() throws Exception {
    final StuckStateMachineProcedure proc =
        new StuckStateMachineProcedure(procEnv, StuckStateMachineState.START);
    long id = procExecutor.submitProcedure(proc);
    Thread.sleep(500);
    // bypass the procedure
    assertFalse(procExecutor.bypassProcedure(id, 1000, false, false));
    assertTrue(procExecutor.bypassProcedure(id, 1000, true, false));

    htu.waitFor(5000, () -> proc.isSuccess() && proc.isBypass());
    LOG.info("{} finished", proc);
  }

  @Test
  public void testBypassingProcedureWithParentRecursive() throws Exception {
    final RootProcedure proc = new RootProcedure();
    long rootId = procExecutor.submitProcedure(proc);
    htu.waitFor(5000, () -> procExecutor.getProcedures().stream()
        .filter(p -> p.getParentProcId() == rootId).collect(Collectors.toList())
        .size() > 0);
    SuspendProcedure suspendProcedure = (SuspendProcedure)procExecutor.getProcedures().stream()
        .filter(p -> p.getParentProcId() == rootId).collect(Collectors.toList()).get(0);
    assertTrue(procExecutor.bypassProcedure(rootId, 1000, false, true));
    htu.waitFor(5000, () -> proc.isSuccess() && proc.isBypass());
    LOG.info("{} finished", proc);
  }

  @Test
  public void testBypassingWaitingTimeoutProcedures() throws Exception {
    final WaitingTimeoutProcedure proc = new WaitingTimeoutProcedure();
    long id = procExecutor.submitProcedure(proc);
    Thread.sleep(500);
    // bypass the procedure
    assertTrue(procExecutor.bypassProcedure(id, 1000, true, false));

    htu.waitFor(5000, () -> proc.isSuccess() && proc.isBypass());
    LOG.info("{} finished", proc);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    procExecutor.stop();
    procStore.stop(false);
    procExecutor.join();
  }

  public static class SuspendProcedure extends ProcedureTestingUtility.NoopProcedure<TestProcEnv> {

    public SuspendProcedure() {
      super();
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env)
        throws ProcedureSuspendedException {
      // Always suspend the procedure
      throw new ProcedureSuspendedException();
    }
  }

  public static class StuckProcedure extends ProcedureTestingUtility.NoopProcedure<TestProcEnv> {

    public StuckProcedure() {
      super();
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env) {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (Throwable t) {
        LOG.debug("Sleep is interrupted.", t);
      }
      return null;
    }

  }


  public static class RootProcedure extends ProcedureTestingUtility.NoopProcedure<TestProcEnv> {
    private boolean childSpwaned = false;

    public RootProcedure() {
      super();
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env)
        throws ProcedureSuspendedException {
      if (!childSpwaned) {
        childSpwaned = true;
        return new Procedure[] {new SuspendProcedure()};
      } else {
        return null;
      }
    }
  }

  public static class WaitingTimeoutProcedure
      extends ProcedureTestingUtility.NoopProcedure<TestProcEnv> {
    public WaitingTimeoutProcedure() {
      super();
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env)
        throws ProcedureSuspendedException {
      // Always suspend the procedure
      setTimeout(50000);
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }

    @Override
    protected synchronized boolean setTimeoutFailure(TestProcEnv env) {
      setState(ProcedureProtos.ProcedureState.RUNNABLE);
      procExecutor.getScheduler().addFront(this);
      return false; // 'false' means that this procedure handled the timeout
    }
  }

  public enum StuckStateMachineState {
    START, THEN, END
  }

  public static class StuckStateMachineProcedure extends
      ProcedureTestingUtility.NoopStateMachineProcedure<TestProcEnv, StuckStateMachineState> {
    private AtomicBoolean stop = new AtomicBoolean(false);

    public StuckStateMachineProcedure() {
      super();
    }

    public StuckStateMachineProcedure(TestProcEnv env, StuckStateMachineState initialState) {
      super(env, initialState);
    }

    @Override
    protected Flow executeFromState(TestProcEnv env, StuckStateMachineState tState)
            throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
      switch (tState) {
        case START:
          LOG.info("PHASE 1: START");
          setNextState(StuckStateMachineState.THEN);
          return Flow.HAS_MORE_STATE;
        case THEN:
          if (stop.get()) {
            setNextState(StuckStateMachineState.END);
          }
          return Flow.HAS_MORE_STATE;
        case END:
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + tState);
      }
    }

    @Override
    protected StuckStateMachineState getState(int stateId) {
      return StuckStateMachineState.values()[stateId];
    }

    @Override
    protected int getStateId(StuckStateMachineState tState) {
      return tState.ordinal();
    }
  }


}
