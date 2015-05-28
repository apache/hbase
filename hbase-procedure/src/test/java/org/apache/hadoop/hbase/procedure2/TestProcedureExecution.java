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
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(SmallTests.class)
public class TestProcedureExecution {
  private static final Log LOG = LogFactory.getLog(TestProcedureExecution.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final Procedure NULL_PROC = null;

  private ProcedureExecutor<Void> procExecutor;
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
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), null, procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS, true);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  private static class TestProcedureException extends Exception {
    public TestProcedureException(String msg) { super(msg); }
  }

  public static class TestSequentialProcedure extends SequentialProcedure<Void> {
    private final Procedure[] subProcs;
    private final List<String> state;
    private final Exception failure;
    private final String name;

    public TestSequentialProcedure() {
      throw new UnsupportedOperationException("recovery should not be triggered here");
    }

    public TestSequentialProcedure(String name, List<String> state, Procedure... subProcs) {
      this.state = state;
      this.subProcs = subProcs;
      this.name = name;
      this.failure = null;
    }

    public TestSequentialProcedure(String name, List<String> state, Exception failure) {
      this.state = state;
      this.subProcs = null;
      this.name = name;
      this.failure = failure;
    }

    @Override
    protected Procedure[] execute(Void env) {
      state.add(name + "-execute");
      if (failure != null) {
        setFailure(new RemoteProcedureException(name + "-failure", failure));
        return null;
      }
      return subProcs;
    }

    @Override
    protected void rollback(Void env) {
      state.add(name + "-rollback");
    }

    @Override
    protected boolean abort(Void env) {
      state.add(name + "-abort");
      return true;
    }
  }

  @Test(timeout=30000)
  public void testBadSubprocList() {
    List<String> state = new ArrayList<String>();
    Procedure subProc2 = new TestSequentialProcedure("subProc2", state);
    Procedure subProc1 = new TestSequentialProcedure("subProc1", state, subProc2, NULL_PROC);
    Procedure rootProc = new TestSequentialProcedure("rootProc", state, subProc1);
    long rootId = ProcedureTestingUtility.submitAndWait(procExecutor, rootProc);

    // subProc1 has a "null" subprocedure which is catched as InvalidArgument
    // failed state with 2 execute and 2 rollback
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
      result.getException().getCause() instanceof IllegalArgumentException);

    assertEquals(state.toString(), 4, state.size());
    assertEquals("rootProc-execute", state.get(0));
    assertEquals("subProc1-execute", state.get(1));
    assertEquals("subProc1-rollback", state.get(2));
    assertEquals("rootProc-rollback", state.get(3));
  }

  @Test(timeout=30000)
  public void testSingleSequentialProc() {
    List<String> state = new ArrayList<String>();
    Procedure subProc2 = new TestSequentialProcedure("subProc2", state);
    Procedure subProc1 = new TestSequentialProcedure("subProc1", state, subProc2);
    Procedure rootProc = new TestSequentialProcedure("rootProc", state, subProc1);
    long rootId = ProcedureTestingUtility.submitAndWait(procExecutor, rootProc);

    // successful state, with 3 execute
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    ProcedureTestingUtility.assertProcNotFailed(result);
    assertEquals(state.toString(), 3, state.size());
  }

  @Test(timeout=30000)
  public void testSingleSequentialProcRollback() {
    List<String> state = new ArrayList<String>();
    Procedure subProc2 = new TestSequentialProcedure("subProc2", state,
                                                     new TestProcedureException("fail test"));
    Procedure subProc1 = new TestSequentialProcedure("subProc1", state, subProc2);
    Procedure rootProc = new TestSequentialProcedure("rootProc", state, subProc1);
    long rootId = ProcedureTestingUtility.submitAndWait(procExecutor, rootProc);

    // the 3rd proc fail, rollback after 2 successful execution
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
      result.getException().getCause() instanceof TestProcedureException);

    assertEquals(state.toString(), 6, state.size());
    assertEquals("rootProc-execute", state.get(0));
    assertEquals("subProc1-execute", state.get(1));
    assertEquals("subProc2-execute", state.get(2));
    assertEquals("subProc2-rollback", state.get(3));
    assertEquals("subProc1-rollback", state.get(4));
    assertEquals("rootProc-rollback", state.get(5));
  }

  public static class TestFaultyRollback extends SequentialProcedure<Void> {
    private int retries = 0;

    public TestFaultyRollback() { }

    @Override
    protected Procedure[] execute(Void env) {
      setFailure("faulty-rollback-test", new TestProcedureException("test faulty rollback"));
      return null;
    }

    @Override
    protected void rollback(Void env) throws IOException {
      if (++retries < 3) {
        LOG.info("inject rollback failure " + retries);
        throw new IOException("injected failure number " + retries);
      }
      LOG.info("execute non faulty rollback step retries=" + retries);
    }

    @Override
    protected boolean abort(Void env) { return false; }
  }

  @Test(timeout=30000)
  public void testRollbackRetriableFailure() {
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, new TestFaultyRollback());

    ProcedureResult result = procExecutor.getResult(procId);
    LOG.info(result.getException());
    assertTrue("expected a failure", result.isFailed());
    assertTrue(result.getException().toString(),
      result.getException().getCause() instanceof TestProcedureException);
  }

  public static class TestWaitingProcedure extends SequentialProcedure<Void> {
    private final List<String> state;
    private final boolean hasChild;
    private final String name;

    public TestWaitingProcedure() {
      throw new UnsupportedOperationException("recovery should not be triggered here");
    }

    public TestWaitingProcedure(String name, List<String> state, boolean hasChild) {
      this.hasChild = hasChild;
      this.state = state;
      this.name = name;
    }

    @Override
    protected Procedure[] execute(Void env) {
      state.add(name + "-execute");
      setState(ProcedureState.WAITING_TIMEOUT);
      return hasChild ? new Procedure[] { new TestWaitChild(name, state) } : null;
    }

    @Override
    protected void rollback(Void env) {
      state.add(name + "-rollback");
    }

    @Override
    protected boolean abort(Void env) {
      state.add(name + "-abort");
      return true;
    }

    public static class TestWaitChild extends SequentialProcedure<Void> {
      private final List<String> state;
      private final String name;

      public TestWaitChild() {
        throw new UnsupportedOperationException("recovery should not be triggered here");
      }

      public TestWaitChild(String name, List<String> state) {
        this.name = name;
        this.state = state;
      }

      @Override
      protected Procedure[] execute(Void env) {
        state.add(name + "-child-execute");
        return null;
      }

      @Override
      protected void rollback(Void env) {
        state.add(name + "-child-rollback");
      }

      @Override
      protected boolean abort(Void env) {
        state.add(name + "-child-abort");
        return true;
      }
    }
  }

  @Test(timeout=30000)
  public void testAbortTimeout() {
    final int PROC_TIMEOUT_MSEC = 2500;
    List<String> state = new ArrayList<String>();
    Procedure proc = new TestWaitingProcedure("wproc", state, false);
    proc.setTimeout(PROC_TIMEOUT_MSEC);
    long startTime = EnvironmentEdgeManager.currentTime();
    long rootId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    long execTime = EnvironmentEdgeManager.currentTime() - startTime;
    LOG.info(state);
    assertTrue("we didn't wait enough execTime=" + execTime, execTime >= PROC_TIMEOUT_MSEC);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
               result.getException().getCause() instanceof TimeoutException);
    assertEquals(state.toString(), 2, state.size());
    assertEquals("wproc-execute", state.get(0));
    assertEquals("wproc-rollback", state.get(1));
  }

  @Test(timeout=30000)
  public void testAbortTimeoutWithChildren() {
    List<String> state = new ArrayList<String>();
    Procedure proc = new TestWaitingProcedure("wproc", state, true);
    proc.setTimeout(2500);
    long rootId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
               result.getException().getCause() instanceof TimeoutException);
    assertEquals(state.toString(), 4, state.size());
    assertEquals("wproc-execute", state.get(0));
    assertEquals("wproc-child-execute", state.get(1));
    assertEquals("wproc-child-rollback", state.get(2));
    assertEquals("wproc-rollback", state.get(3));
  }
}
