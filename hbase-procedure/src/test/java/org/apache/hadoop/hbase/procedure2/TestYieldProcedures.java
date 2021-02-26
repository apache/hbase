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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, SmallTests.class})
public class TestYieldProcedures {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestYieldProcedures.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestYieldProcedures.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final Procedure NULL_PROC = null;

  private ProcedureExecutor<TestProcEnv> procExecutor;
  private TestScheduler procRunnables;
  private ProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procRunnables = new TestScheduler();
    procExecutor =
      new ProcedureExecutor<>(htu.getConfiguration(), new TestProcEnv(), procStore, procRunnables);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    ProcedureTestingUtility.initAndStartWorkers(procExecutor, PROCEDURE_EXECUTOR_SLOTS, true);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  @Test
  public void testYieldEachExecutionStep() throws Exception {
    final int NUM_STATES = 3;

    TestStateMachineProcedure[] procs = new TestStateMachineProcedure[3];
    for (int i = 0; i < procs.length; ++i) {
      procs[i] = new TestStateMachineProcedure(true, false);
      procExecutor.submitProcedure(procs[i]);
    }
    ProcedureTestingUtility.waitNoProcedureRunning(procExecutor);

    for (int i = 0; i < procs.length; ++i) {
      assertEquals(NUM_STATES * 2, procs[i].getExecutionInfo().size());

      // verify execution
      int index = 0;
      for (int execStep = 0; execStep < NUM_STATES; ++execStep) {
        TestStateMachineProcedure.ExecutionInfo info = procs[i].getExecutionInfo().get(index++);
        assertEquals(false, info.isRollback());
        assertEquals(execStep, info.getStep().ordinal());
      }

      // verify rollback
      for (int execStep = NUM_STATES - 1; execStep >= 0; --execStep) {
        TestStateMachineProcedure.ExecutionInfo info = procs[i].getExecutionInfo().get(index++);
        assertEquals(true, info.isRollback());
        assertEquals(execStep, info.getStep().ordinal());
      }
    }

    // check runnable queue stats
    assertEquals(0, procRunnables.size());
    assertEquals(0, procRunnables.addFrontCalls);
    assertEquals(15, procRunnables.addBackCalls);
    assertEquals(12, procRunnables.yieldCalls);
    assertEquals(16, procRunnables.pollCalls);
    assertEquals(3, procRunnables.completionCalls);
  }

  @Test
  public void testYieldOnInterrupt() throws Exception {
    final int NUM_STATES = 3;
    int count = 0;

    TestStateMachineProcedure proc = new TestStateMachineProcedure(true, true);
    ProcedureTestingUtility.submitAndWait(procExecutor, proc);

    // test execute (we execute steps twice, one has the IE the other completes)
    assertEquals(NUM_STATES * 4, proc.getExecutionInfo().size());
    for (int i = 0; i < NUM_STATES; ++i) {
      TestStateMachineProcedure.ExecutionInfo info = proc.getExecutionInfo().get(count++);
      assertEquals(false, info.isRollback());
      assertEquals(i, info.getStep().ordinal());

      info = proc.getExecutionInfo().get(count++);
      assertEquals(false, info.isRollback());
      assertEquals(i, info.getStep().ordinal());
    }

    // test rollback (we execute steps twice, rollback counts both IE and completed)
    for (int i = NUM_STATES - 1; i >= 0; --i) {
      TestStateMachineProcedure.ExecutionInfo info = proc.getExecutionInfo().get(count++);
      assertEquals(true, info.isRollback());
      assertEquals(i, info.getStep().ordinal());
    }

    for (int i = NUM_STATES - 1; i >= 0; --i) {
      TestStateMachineProcedure.ExecutionInfo info = proc.getExecutionInfo().get(count++);
      assertEquals(true, info.isRollback());
      assertEquals(0, info.getStep().ordinal());
    }

    // check runnable queue stats
    assertEquals(0, procRunnables.size());
    assertEquals(0, procRunnables.addFrontCalls);
    assertEquals(11, procRunnables.addBackCalls);
    assertEquals(10, procRunnables.yieldCalls);
    assertEquals(12, procRunnables.pollCalls);
    assertEquals(1, procRunnables.completionCalls);
  }

  @Test
  public void testYieldException() {
    TestYieldProcedure proc = new TestYieldProcedure();
    ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertEquals(6, proc.step);

    // check runnable queue stats
    assertEquals(0, procRunnables.size());
    assertEquals(0, procRunnables.addFrontCalls);
    assertEquals(6, procRunnables.addBackCalls);
    assertEquals(5, procRunnables.yieldCalls);
    assertEquals(7, procRunnables.pollCalls);
    assertEquals(1, procRunnables.completionCalls);
  }

  private static class TestProcEnv {
    public final AtomicLong timestamp = new AtomicLong(0);

    public long nextTimestamp() {
      return timestamp.incrementAndGet();
    }
  }

  public static class TestStateMachineProcedure
      extends StateMachineProcedure<TestProcEnv, TestStateMachineProcedure.State> {
    enum State { STATE_1, STATE_2, STATE_3 }

    public static class ExecutionInfo {
      private final boolean rollback;
      private final long timestamp;
      private final State step;

      public ExecutionInfo(long timestamp, State step, boolean isRollback) {
        this.timestamp = timestamp;
        this.step = step;
        this.rollback = isRollback;
      }

      public State getStep() {
        return step;
      }

      public long getTimestamp() {
        return timestamp;
      }

      public boolean isRollback() {
        return rollback;
      }
    }

    private final ArrayList<ExecutionInfo> executionInfo = new ArrayList<>();
    private final AtomicBoolean aborted = new AtomicBoolean(false);
    private final boolean throwInterruptOnceOnEachStep;
    private final boolean abortOnFinalStep;

    public TestStateMachineProcedure() {
      this(false, false);
    }

    public TestStateMachineProcedure(boolean abortOnFinalStep,
        boolean throwInterruptOnceOnEachStep) {
      this.abortOnFinalStep = abortOnFinalStep;
      this.throwInterruptOnceOnEachStep = throwInterruptOnceOnEachStep;
    }

    public ArrayList<ExecutionInfo> getExecutionInfo() {
      return executionInfo;
    }

    @Override
    protected StateMachineProcedure.Flow executeFromState(TestProcEnv env, State state)
        throws InterruptedException {
      final long ts = env.nextTimestamp();
      LOG.info(getProcId() + " execute step " + state + " ts=" + ts);
      executionInfo.add(new ExecutionInfo(ts, state, false));
      Thread.sleep(150);

      if (throwInterruptOnceOnEachStep && ((executionInfo.size() - 1) % 2) == 0) {
        LOG.debug("THROW INTERRUPT");
        throw new InterruptedException("test interrupt");
      }

      switch (state) {
        case STATE_1:
          setNextState(State.STATE_2);
          break;
        case STATE_2:
          setNextState(State.STATE_3);
          break;
        case STATE_3:
          if (abortOnFinalStep) {
            setFailure("test", new IOException("Requested abort on final step"));
          }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException();
      }
      return Flow.HAS_MORE_STATE;
    }

    @Override
    protected void rollbackState(TestProcEnv env, final State state)
        throws InterruptedException {
      final long ts = env.nextTimestamp();
      LOG.debug(getProcId() + " rollback state " + state + " ts=" + ts);
      executionInfo.add(new ExecutionInfo(ts, state, true));
      Thread.sleep(150);

      if (throwInterruptOnceOnEachStep && ((executionInfo.size() - 1) % 2) == 0) {
        LOG.debug("THROW INTERRUPT");
        throw new InterruptedException("test interrupt");
      }

      switch (state) {
        case STATE_1:
          break;
        case STATE_2:
          break;
        case STATE_3:
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    protected State getState(final int stateId) {
      return State.values()[stateId];
    }

    @Override
    protected int getStateId(final State state) {
      return state.ordinal();
    }

    @Override
    protected State getInitialState() {
      return State.STATE_1;
    }

    @Override
    protected boolean isYieldBeforeExecuteFromState(TestProcEnv env, State state) {
      return true;
    }

    @Override
    protected boolean abort(TestProcEnv env) {
      aborted.set(true);
      return true;
    }
  }

  public static class TestYieldProcedure extends Procedure<TestProcEnv> {
    private int step = 0;

    public TestYieldProcedure() {
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env) throws ProcedureYieldException {
      LOG.info("execute step " + step);
      if (step++ < 5) {
        throw new ProcedureYieldException();
      }
      return null;
    }

    @Override
    protected void rollback(TestProcEnv env) {
    }

    @Override
    protected boolean abort(TestProcEnv env) {
      return false;
    }

    @Override
    protected boolean isYieldAfterExecutionStep(final TestProcEnv env) {
      return true;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
    }
  }

  private static class TestScheduler extends SimpleProcedureScheduler {
    private int completionCalls;
    private int addFrontCalls;
    private int addBackCalls;
    private int yieldCalls;
    private int pollCalls;

    public TestScheduler() {}

    @Override
    public void addFront(final Procedure proc) {
      addFrontCalls++;
      super.addFront(proc);
    }

    @Override
    public void addBack(final Procedure proc) {
      addBackCalls++;
      super.addBack(proc);
    }

    @Override
    public void yield(final Procedure proc) {
      yieldCalls++;
      super.yield(proc);
    }

    @Override
    public Procedure poll() {
      pollCalls++;
      return super.poll();
    }

    @Override
    public Procedure poll(long timeout, TimeUnit unit) {
      pollCalls++;
      return super.poll(timeout, unit);
    }

    @Override
    public void completionCleanup(Procedure proc) {
      completionCalls++;
    }
  }
}
