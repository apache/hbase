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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestStateMachineProcedure {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStateMachineProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestStateMachineProcedure.class);

  private static final Exception TEST_FAILURE_EXCEPTION = new Exception("test failure") {

    private static final long serialVersionUID = 2147942238987041310L;

    @Override
    public boolean equals(final Object other) {
      if (this == other) {
        return true;
      }

      if (!(other instanceof Exception)) {
        return false;
      }

      // we are going to serialize the exception in the test,
      // so the instance comparison will not match
      return getMessage().equals(((Exception)other).getMessage());
    }

    @Override
    public int hashCode() {
      return getMessage().hashCode();
    }
  };

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;

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

    logDir = new Path(testDir, "proc-logs");
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<>(htu.getConfiguration(), new TestProcEnv(), procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    ProcedureTestingUtility.initAndStartWorkers(procExecutor, PROCEDURE_EXECUTOR_SLOTS, true);
  }

  @After
  public void tearDown() throws IOException {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExecutor, false);
    assertTrue("expected executor to be running", procExecutor.isRunning());

    procExecutor.stop();
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  @Test
  public void testAbortStuckProcedure() throws InterruptedException {
    try {
      procExecutor.getEnvironment().loop = true;
      TestSMProcedure proc = new TestSMProcedure();
      long procId = procExecutor.submitProcedure(proc);
      Thread.sleep(1000 + (int) (Math.random() * 4001));
      proc.abort(procExecutor.getEnvironment());
      ProcedureTestingUtility.waitProcedure(procExecutor, procId);
      assertEquals(true, proc.isFailed());
    } finally {
      procExecutor.getEnvironment().loop = false;
    }
  }

  @Test
  public void testChildOnLastStep() {
    long procId = procExecutor.submitProcedure(new TestSMProcedure());
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
    assertEquals(3, procExecutor.getEnvironment().execCount.get());
    assertEquals(0, procExecutor.getEnvironment().rollbackCount.get());
    ProcedureTestingUtility.assertProcNotFailed(procExecutor, procId);
  }

  @Test
  public void testChildOnLastStepDoubleExecution() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExecutor, true);
    long procId = procExecutor.submitProcedure(new TestSMProcedure());
    ProcedureTestingUtility.testRecoveryAndDoubleExecution(procExecutor, procId);
    assertEquals(6, procExecutor.getEnvironment().execCount.get());
    assertEquals(0, procExecutor.getEnvironment().rollbackCount.get());
    ProcedureTestingUtility.assertProcNotFailed(procExecutor, procId);
  }

  @Test
  public void testChildOnLastStepWithRollback() {
    procExecutor.getEnvironment().triggerChildRollback = true;
    long procId = procExecutor.submitProcedure(new TestSMProcedure());
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
    assertEquals(3, procExecutor.getEnvironment().execCount.get());
    assertEquals(3, procExecutor.getEnvironment().rollbackCount.get());
    Throwable cause = ProcedureTestingUtility.assertProcFailed(procExecutor, procId);
    assertEquals(TEST_FAILURE_EXCEPTION, cause);
  }

  @Test
  public void testChildNormalRollbackStateCount() {
    procExecutor.getEnvironment().triggerChildRollback = true;
    TestSMProcedureBadRollback testNormalRollback = new TestSMProcedureBadRollback();
    long procId = procExecutor.submitProcedure(testNormalRollback);
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
    assertEquals(0, testNormalRollback.stateCount);
  }

  @Test
  public void testChildBadRollbackStateCount() {
    procExecutor.getEnvironment().triggerChildRollback = true;
    TestSMProcedureBadRollback testBadRollback = new TestSMProcedureBadRollback();
    long procId = procExecutor.submitProcedure(testBadRollback);
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
    assertEquals(0, testBadRollback.stateCount);
  }

  @Test
  public void testChildOnLastStepWithRollbackDoubleExecution() throws Exception {
    procExecutor.getEnvironment().triggerChildRollback = true;
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExecutor, true);
    long procId = procExecutor.submitProcedure(new TestSMProcedure());
    ProcedureTestingUtility.testRecoveryAndDoubleExecution(procExecutor, procId, true);
    assertEquals(6, procExecutor.getEnvironment().execCount.get());
    assertEquals(6, procExecutor.getEnvironment().rollbackCount.get());
    Throwable cause = ProcedureTestingUtility.assertProcFailed(procExecutor, procId);
    assertEquals(TEST_FAILURE_EXCEPTION, cause);
  }

  public enum TestSMProcedureState { STEP_1, STEP_2 };
  public static class TestSMProcedure
      extends StateMachineProcedure<TestProcEnv, TestSMProcedureState> {
    @Override
    protected Flow executeFromState(TestProcEnv env, TestSMProcedureState state) {
      LOG.info("EXEC " + state + " " + this);
      env.execCount.incrementAndGet();
      switch (state) {
        case STEP_1:
          if (!env.loop) {
            setNextState(TestSMProcedureState.STEP_2);
          }
          break;
        case STEP_2:
          addChildProcedure(new SimpleChildProcedure());
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    }

    @Override
    protected boolean isRollbackSupported(TestSMProcedureState state) {
      return true;
    }

    @Override
    protected void rollbackState(TestProcEnv env, TestSMProcedureState state) {
      LOG.info("ROLLBACK " + state + " " + this);
      env.rollbackCount.incrementAndGet();
    }

    @Override
    protected TestSMProcedureState getState(int stateId) {
      return TestSMProcedureState.values()[stateId];
    }

    @Override
    protected int getStateId(TestSMProcedureState state) {
      return state.ordinal();
    }

    @Override
    protected TestSMProcedureState getInitialState() {
      return TestSMProcedureState.STEP_1;
    }
  }

  public static class TestSMProcedureBadRollback
          extends StateMachineProcedure<TestProcEnv, TestSMProcedureState> {
    @Override
    protected Flow executeFromState(TestProcEnv env, TestSMProcedureState state) {
      LOG.info("EXEC " + state + " " + this);
      env.execCount.incrementAndGet();
      switch (state) {
        case STEP_1:
          if (!env.loop) {
            setNextState(TestSMProcedureState.STEP_2);
          }
          break;
        case STEP_2:
          addChildProcedure(new SimpleChildProcedure());
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    }
    @Override
    protected void rollbackState(TestProcEnv env, TestSMProcedureState state) {
      LOG.info("ROLLBACK " + state + " " + this);
      env.rollbackCount.incrementAndGet();
    }

    @Override
    protected TestSMProcedureState getState(int stateId) {
      return TestSMProcedureState.values()[stateId];
    }

    @Override
    protected int getStateId(TestSMProcedureState state) {
      return state.ordinal();
    }

    @Override
    protected TestSMProcedureState getInitialState() {
      return TestSMProcedureState.STEP_1;
    }

    @Override
    protected void rollback(final TestProcEnv env)
            throws IOException, InterruptedException {
      if (isEofState()) {
        stateCount--;
      }
      try {
        updateTimestamp();
        rollbackState(env, getCurrentState());
        throw new IOException();
      } catch(IOException e) {
        //do nothing for now
      } finally {
        stateCount--;
        updateTimestamp();
      }
    }
  }

  public static class SimpleChildProcedure extends NoopProcedure<TestProcEnv> {
    @Override
    protected Procedure<TestProcEnv>[] execute(TestProcEnv env) {
      LOG.info("EXEC " + this);
      env.execCount.incrementAndGet();
      if (env.triggerChildRollback) {
        setFailure("test-failure", TEST_FAILURE_EXCEPTION);
      }
      return null;
    }

    @Override
    protected void rollback(TestProcEnv env) {
      LOG.info("ROLLBACK " + this);
      env.rollbackCount.incrementAndGet();
    }
  }

  public static class TestProcEnv {
    AtomicInteger execCount = new AtomicInteger(0);
    AtomicInteger rollbackCount = new AtomicInteger(0);
    boolean triggerChildRollback = false;
    boolean loop = false;
  }
}
