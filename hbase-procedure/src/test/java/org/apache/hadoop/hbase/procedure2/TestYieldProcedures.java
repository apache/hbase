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

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;

import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, SmallTests.class})
public class TestYieldProcedures {
  private static final Log LOG = LogFactory.getLog(TestYieldProcedures.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final Procedure NULL_PROC = null;

  private ProcedureExecutor<TestProcEnv> procExecutor;
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
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), fs, logDir);
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), new TestProcEnv(), procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS, true);
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

    // verify yield during execute()
    long prevTimestamp = 0;
    for (int execStep = 0; execStep < NUM_STATES; ++execStep) {
      for (int i = 0; i < procs.length; ++i) {
        assertEquals(NUM_STATES * 2, procs[i].getExecutionInfo().size());
        TestStateMachineProcedure.ExecutionInfo info = procs[i].getExecutionInfo().get(execStep);
        LOG.info("i=" + i + " execStep=" + execStep + " timestamp=" + info.getTimestamp());
        assertEquals(false, info.isRollback());
        assertEquals(execStep, info.getStep().ordinal());
        assertEquals(prevTimestamp + 1, info.getTimestamp());
        prevTimestamp++;
      }
    }

    // verify yield during rollback()
    int count = NUM_STATES;
    for (int execStep = NUM_STATES - 1; execStep >= 0; --execStep) {
      for (int i = 0; i < procs.length; ++i) {
        assertEquals(NUM_STATES * 2, procs[i].getExecutionInfo().size());
        TestStateMachineProcedure.ExecutionInfo info = procs[i].getExecutionInfo().get(count);
        LOG.info("i=" + i + " execStep=" + execStep + " timestamp=" + info.getTimestamp());
        assertEquals(true, info.isRollback());
        assertEquals(execStep, info.getStep().ordinal());
        assertEquals(prevTimestamp + 1, info.getTimestamp());
        prevTimestamp++;
      }
      count++;
    }
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

    // test rollback (we execute steps twice, one has the IE the other completes)
    for (int i = NUM_STATES - 1; i >= 0; --i) {
      TestStateMachineProcedure.ExecutionInfo info = proc.getExecutionInfo().get(count++);
      assertEquals(true, info.isRollback());
      assertEquals(i, info.getStep().ordinal());

      info = proc.getExecutionInfo().get(count++);
      assertEquals(true, info.isRollback());
      assertEquals(i, info.getStep().ordinal());
    }
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

    public class ExecutionInfo {
      private final boolean rollback;
      private final long timestamp;
      private final State step;

      public ExecutionInfo(long timestamp, State step, boolean isRollback) {
        this.timestamp = timestamp;
        this.step = step;
        this.rollback = isRollback;
      }

      public State getStep() { return step; }
      public long getTimestamp() { return timestamp; }
      public boolean isRollback() { return rollback; }
    }

    private final ArrayList<ExecutionInfo> executionInfo = new ArrayList<ExecutionInfo>();
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
      LOG.info("execute step " + state);
      executionInfo.add(new ExecutionInfo(env.nextTimestamp(), state, false));
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
      LOG.debug("rollback state " + state);
      executionInfo.add(new ExecutionInfo(env.nextTimestamp(), state, true));
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
}