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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestProcedureRecovery {
  private static final Log LOG = LogFactory.getLog(TestProcedureRecovery.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;

  private static TestProcEnv procEnv;
  private static ProcedureExecutor<TestProcEnv> procExecutor;
  private static ProcedureStore procStore;
  private static int procSleepInterval;

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
    procEnv = new TestProcEnv();
    procStore = ProcedureTestingUtility.createStore(htu.getConfiguration(), fs, logDir);
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), procEnv, procStore);
    procExecutor.testing = new ProcedureExecutor.Testing();
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS, true);
    procSleepInterval = 0;
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  private void restart() throws Exception {
    dumpLogDirState();
    ProcedureTestingUtility.restart(procExecutor);
    dumpLogDirState();
  }

  public static class TestSingleStepProcedure extends SequentialProcedure<TestProcEnv> {
    private int step = 0;

    public TestSingleStepProcedure() { }

    @Override
    protected Procedure[] execute(TestProcEnv env) throws InterruptedException {
      env.waitOnLatch();
      LOG.debug("execute procedure " + this + " step=" + step);
      step++;
      setResult(Bytes.toBytes(step));
      return null;
    }

    @Override
    protected void rollback(TestProcEnv env) { }

    @Override
    protected boolean abort(TestProcEnv env) { return true; }
  }

  public static class BaseTestStepProcedure extends SequentialProcedure<TestProcEnv> {
    private AtomicBoolean abort = new AtomicBoolean(false);
    private int step = 0;

    @Override
    protected Procedure[] execute(TestProcEnv env) throws InterruptedException {
      env.waitOnLatch();
      LOG.debug("execute procedure " + this + " step=" + step);
      ProcedureTestingUtility.toggleKillBeforeStoreUpdate(procExecutor);
      step++;
      Threads.sleepWithoutInterrupt(procSleepInterval);
      if (isAborted()) {
        setFailure(new RemoteProcedureException(getClass().getName(),
          new ProcedureAbortedException(
            "got an abort at " + getClass().getName() + " step=" + step)));
        return null;
      }
      return null;
    }

    @Override
    protected void rollback(TestProcEnv env) {
      LOG.debug("rollback procedure " + this + " step=" + step);
      ProcedureTestingUtility.toggleKillBeforeStoreUpdate(procExecutor);
      step++;
    }

    @Override
    protected boolean abort(TestProcEnv env) {
      abort.set(true);
      return true;
    }

    private boolean isAborted() {
      boolean aborted = abort.get();
      BaseTestStepProcedure proc = this;
      while (proc.hasParent() && !aborted) {
        proc = (BaseTestStepProcedure)procExecutor.getProcedure(proc.getParentProcId());
        aborted = proc.isAborted();
      }
      return aborted;
    }
  }

  public static class TestMultiStepProcedure extends BaseTestStepProcedure {
    public TestMultiStepProcedure() { }

    @Override
    public Procedure[] execute(TestProcEnv env) throws InterruptedException {
      super.execute(env);
      return isFailed() ? null : new Procedure[] { new Step1Procedure() };
    }

    public static class Step1Procedure extends BaseTestStepProcedure {
      public Step1Procedure() { }

      @Override
      protected Procedure[] execute(TestProcEnv env) throws InterruptedException {
        super.execute(env);
        return isFailed() ? null : new Procedure[] { new Step2Procedure() };
      }
    }

    public static class Step2Procedure extends BaseTestStepProcedure {
      public Step2Procedure() { }
    }
  }

  @Test
  public void testNoopLoad() throws Exception {
    restart();
  }

  @Test(timeout=30000)
  public void testSingleStepProcRecovery() throws Exception {
    Procedure proc = new TestSingleStepProcedure();
    procExecutor.testing.killBeforeStoreUpdate = true;
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertFalse(procExecutor.isRunning());
    procExecutor.testing.killBeforeStoreUpdate = false;

    // Restart and verify that the procedures restart
    long restartTs = EnvironmentEdgeManager.currentTime();
    restart();
    waitProcedure(procId);
    ProcedureInfo result = procExecutor.getResult(procId);
    assertTrue(result.getLastUpdate() > restartTs);
    ProcedureTestingUtility.assertProcNotFailed(result);
    assertEquals(1, Bytes.toInt(result.getResult()));
    long resultTs = result.getLastUpdate();

    // Verify that after another restart the result is still there
    restart();
    result = procExecutor.getResult(procId);
    ProcedureTestingUtility.assertProcNotFailed(result);
    assertEquals(resultTs, result.getLastUpdate());
    assertEquals(1, Bytes.toInt(result.getResult()));
  }

  @Test(timeout=30000)
  public void testMultiStepProcRecovery() throws Exception {
    // Step 0 - kill
    Procedure proc = new TestMultiStepProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertFalse(procExecutor.isRunning());

    // Step 0 exec && Step 1 - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 1 exec && step 2 - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 2 exec
    restart();
    waitProcedure(procId);
    assertTrue(procExecutor.isRunning());

    // The procedure is completed
    ProcedureInfo result = procExecutor.getResult(procId);
    ProcedureTestingUtility.assertProcNotFailed(result);
  }

  @Test(timeout=30000)
  public void testMultiStepRollbackRecovery() throws Exception {
    // Step 0 - kill
    Procedure proc = new TestMultiStepProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertFalse(procExecutor.isRunning());

    // Step 0 exec && Step 1 - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 1 exec && step 2 - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 2 exec - rollback - kill
    procSleepInterval = 2500;
    restart();
    assertTrue(procExecutor.abort(procId));
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // rollback - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // rollback - complete
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Restart the executor and get the result
    restart();
    waitProcedure(procId);

    // The procedure is completed
    ProcedureInfo result = procExecutor.getResult(procId);
    ProcedureTestingUtility.assertIsAbortException(result);
  }

  public static class TestStateMachineProcedure
      extends StateMachineProcedure<TestProcEnv, TestStateMachineProcedure.State> {
    enum State { STATE_1, STATE_2, STATE_3, DONE }

    public TestStateMachineProcedure() {}

    private AtomicBoolean aborted = new AtomicBoolean(false);
    private int iResult = 0;

    @Override
    protected StateMachineProcedure.Flow executeFromState(TestProcEnv env, State state) {
      switch (state) {
        case STATE_1:
          LOG.info("execute step 1 " + this);
          setNextState(State.STATE_2);
          iResult += 3;
          break;
        case STATE_2:
          LOG.info("execute step 2 " + this);
          setNextState(State.STATE_3);
          iResult += 5;
          break;
        case STATE_3:
          LOG.info("execute step 3 " + this);
          Threads.sleepWithoutInterrupt(procSleepInterval);
          if (aborted.get()) {
            LOG.info("aborted step 3 " + this);
            setAbortFailure("test", "aborted");
            break;
          }
          setNextState(State.DONE);
          iResult += 7;
          setResult(Bytes.toBytes(iResult));
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException();
      }
      return Flow.HAS_MORE_STATE;
    }

    @Override
    protected void rollbackState(TestProcEnv env, final State state) {
      switch (state) {
        case STATE_1:
          LOG.info("rollback step 1 " + this);
          break;
        case STATE_2:
          LOG.info("rollback step 2 " + this);
          break;
        case STATE_3:
          LOG.info("rollback step 3 " + this);
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
    protected boolean abort(TestProcEnv env) {
      aborted.set(true);
      return true;
    }

    @Override
    protected void serializeStateData(final OutputStream stream) throws IOException {
      super.serializeStateData(stream);
      stream.write(Bytes.toBytes(iResult));
    }

    @Override
    protected void deserializeStateData(final InputStream stream) throws IOException {
      super.deserializeStateData(stream);
      byte[] data = new byte[4];
      stream.read(data);
      iResult = Bytes.toInt(data);
    }
  }

  @Test(timeout=30000)
  public void testStateMachineRecovery() throws Exception {
    ProcedureTestingUtility.setToggleKillBeforeStoreUpdate(procExecutor, true);
    ProcedureTestingUtility.setKillBeforeStoreUpdate(procExecutor, true);

    // Step 1 - kill
    Procedure proc = new TestStateMachineProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertFalse(procExecutor.isRunning());

    // Step 1 exec && Step 2 - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 2 exec && step 3 - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 3 exec
    restart();
    waitProcedure(procId);
    assertTrue(procExecutor.isRunning());

    // The procedure is completed
    ProcedureInfo result = procExecutor.getResult(procId);
    ProcedureTestingUtility.assertProcNotFailed(result);
    assertEquals(15, Bytes.toInt(result.getResult()));
  }

  @Test(timeout=30000)
  public void testStateMachineRollbackRecovery() throws Exception {
    ProcedureTestingUtility.setToggleKillBeforeStoreUpdate(procExecutor, true);
    ProcedureTestingUtility.setKillBeforeStoreUpdate(procExecutor, true);

    // Step 1 - kill
    Procedure proc = new TestStateMachineProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 1 exec && Step 2 - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 2 exec && step 3 - kill
    restart();
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Step 3 exec - rollback step 3 - kill
    procSleepInterval = 2500;
    restart();
    assertTrue(procExecutor.abort(procId));
    waitProcedure(procId);
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);
    assertFalse(procExecutor.isRunning());

    // Rollback step 3 - rollback step 2 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);

    // Rollback step 2 - step 1 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());
    ProcedureTestingUtility.assertProcNotYetCompleted(procExecutor, procId);

    // Rollback step 1 - complete
    restart();
    waitProcedure(procId);
    assertTrue(procExecutor.isRunning());

    // The procedure is completed
    ProcedureInfo result = procExecutor.getResult(procId);
    ProcedureTestingUtility.assertIsAbortException(result);
  }

  private void waitProcedure(final long procId) {
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
    dumpLogDirState();
  }

  private void dumpLogDirState() {
    try {
      FileStatus[] files = fs.listStatus(logDir);
      if (files != null && files.length > 0) {
        for (FileStatus file: files) {
          assertTrue(file.toString(), file.isFile());
          LOG.debug("log file " + file.getPath() + " size=" + file.getLen());
        }
      } else {
        LOG.debug("no files under: " + logDir);
      }
    } catch (IOException e) {
      LOG.warn("Unable to dump " + logDir, e);
    }
  }

  private static class TestProcEnv {
    private CountDownLatch latch = null;

    /**
     * set/unset a latch. every procedure execute() step will wait on the latch if any.
     */
    public void setWaitLatch(CountDownLatch latch) {
      this.latch = latch;
    }

    public void waitOnLatch() throws InterruptedException {
      if (latch != null) {
        latch.await();
      }
    }
  }
}
