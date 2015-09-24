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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestProcedureReplayOrder {
  private static final Log LOG = LogFactory.getLog(TestProcedureReplayOrder.class);

  private static final Procedure NULL_PROC = null;

  private ProcedureExecutor<Void> procExecutor;
  private TestProcedureEnv procEnv;
  private ProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    htu.getConfiguration().setInt("hbase.procedure.store.wal.sync.wait.msec", 10);

    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procEnv = new TestProcedureEnv();
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), fs, logDir);
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), procEnv, procStore);
    procStore.start(24);
    procExecutor.start(1);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  @Test(timeout=90000)
  public void testSingleStepReplyOrder() throws Exception {
    // avoid the procedure to be runnable
    procEnv.setAcquireLock(false);

    // submit the procedures
    submitProcedures(16, 25, TestSingleStepProcedure.class);

    // restart the executor and allow the procedures to run
    ProcedureTestingUtility.restart(procExecutor, new Runnable() {
      @Override
      public void run() {
        procEnv.setAcquireLock(true);
      }
    });

    // wait the execution of all the procedures and
    // assert that the execution order was sorted by procId
    ProcedureTestingUtility.waitNoProcedureRunning(procExecutor);
    procEnv.assertSortedExecList();

    // TODO: FIXME: This should be revisited
  }

  @Ignore
  @Test(timeout=90000)
  public void testMultiStepReplyOrder() throws Exception {
    // avoid the procedure to be runnable
    procEnv.setAcquireLock(false);

    // submit the procedures
    submitProcedures(16, 10, TestTwoStepProcedure.class);

    // restart the executor and allow the procedures to run
    ProcedureTestingUtility.restart(procExecutor, new Runnable() {
      @Override
      public void run() {
        procEnv.setAcquireLock(true);
      }
    });

    fail("TODO: FIXME: NOT IMPLEMENT REPLAY ORDER");
  }

  private void submitProcedures(final int nthreads, final int nprocPerThread,
      final Class<?> procClazz) throws Exception {
    Thread[] submitThreads = new Thread[nthreads];
    for (int i = 0; i < submitThreads.length; ++i) {
      submitThreads[i] = new Thread() {
        @Override
        public void run() {
          for (int i = 0; i < nprocPerThread; ++i) {
            try {
              procExecutor.submitProcedure((Procedure)procClazz.newInstance());
            } catch (InstantiationException|IllegalAccessException e) {
              LOG.error("unable to instantiate the procedure", e);
              fail("failure during the proc.newInstance(): " + e.getMessage());
            }
          }
        }
      };
    }

    for (int i = 0; i < submitThreads.length; ++i) {
      submitThreads[i].start();
    }

    for (int i = 0; i < submitThreads.length; ++i) {
      submitThreads[i].join();
    }
  }

  private static class TestProcedureEnv {
    private ArrayList<Long> execList = new ArrayList<Long>();
    private boolean acquireLock = true;

    public void setAcquireLock(boolean acquireLock) {
      this.acquireLock = acquireLock;
    }

    public boolean canAcquireLock() {
      return acquireLock;
    }

    public void addToExecList(final Procedure proc) {
      execList.add(proc.getProcId());
    }

    public ArrayList<Long> getExecList() {
      return execList;
    }

    public void assertSortedExecList() {
      LOG.debug("EXEC LIST: " + execList);
      for (int i = 1; i < execList.size(); ++i) {
        assertTrue("exec list not sorted: " + execList.get(i-1) + " >= " + execList.get(i),
          execList.get(i-1) < execList.get(i));
      }
    }
  }

  public static class TestSingleStepProcedure extends SequentialProcedure<TestProcedureEnv> {
    public TestSingleStepProcedure() { }

    @Override
    protected Procedure[] execute(TestProcedureEnv env) {
      LOG.debug("execute procedure " + this);
      env.addToExecList(this);
      return null;
    }

    protected boolean acquireLock(final TestProcedureEnv env) {
      return env.canAcquireLock();
    }

    @Override
    protected void rollback(TestProcedureEnv env) { }

    @Override
    protected boolean abort(TestProcedureEnv env) { return true; }
  }

  public static class TestTwoStepProcedure extends SequentialProcedure<TestProcedureEnv> {
    public TestTwoStepProcedure() { }

    @Override
    protected Procedure[] execute(TestProcedureEnv env) {
      LOG.debug("execute procedure " + this);
      env.addToExecList(this);
      return new Procedure[] { new TestSingleStepProcedure() };
    }

    protected boolean acquireLock(final TestProcedureEnv env) {
      return true;
    }

    @Override
    protected void rollback(TestProcedureEnv env) { }

    @Override
    protected boolean abort(TestProcedureEnv env) { return true; }
  }
}
