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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestStateMachineProcedure {
  private static final Log LOG = LogFactory.getLog(TestStateMachineProcedure.class);

  private static final Exception TEST_FAILURE_EXCEPTION = new Exception("test failure") {
    @Override
    public boolean equals(final Object other) {
      if (this == other) return true;
      if (!(other instanceof Exception)) return false;
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
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), fs, logDir);
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), new TestProcEnv(), procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS, true);
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
    protected Flow executeFromState(TestProcEnv env, TestSMProcedureState state) {
      LOG.info("EXEC " + state + " " + this);
      env.execCount.incrementAndGet();
      switch (state) {
        case STEP_1:
          setNextState(TestSMProcedureState.STEP_2);
          break;
        case STEP_2:
          addChildProcedure(new SimpleChildProcedure());
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    }

    protected void rollbackState(TestProcEnv env, TestSMProcedureState state) {
      LOG.info("ROLLBACK " + state + " " + this);
      env.rollbackCount.incrementAndGet();
    }

    protected TestSMProcedureState getState(int stateId) {
      return TestSMProcedureState.values()[stateId];
    }

    protected int getStateId(TestSMProcedureState state) {
      return state.ordinal();
    }

    protected TestSMProcedureState getInitialState() {
      return TestSMProcedureState.STEP_1;
    }
  }

  public static class SimpleChildProcedure extends NoopProcedure<TestProcEnv> {
    protected Procedure[] execute(TestProcEnv env) {
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

  public class TestProcEnv {
    AtomicInteger execCount = new AtomicInteger(0);
    AtomicInteger rollbackCount = new AtomicInteger(0);
    boolean triggerChildRollback = false;
  }
}
