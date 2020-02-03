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
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Int64Value;

/**
 * For now we do not guarantee this, we will restore the locks when restarting ProcedureExecutor so
 * we should use lock to obtain the correct order. Ignored.
 */
@Ignore
@Category({ MasterTests.class, SmallTests.class })
public class TestProcedureReplayOrder {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureReplayOrder.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureReplayOrder.class);

  private static final int NUM_THREADS = 16;

  private ProcedureExecutor<TestProcedureEnv> procExecutor;
  private TestProcedureEnv procEnv;
  private ProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    htu.getConfiguration().setInt(WALProcedureStore.SYNC_WAIT_MSEC_CONF_KEY, 25);

    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procEnv = new TestProcedureEnv();
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<>(htu.getConfiguration(), procEnv, procStore);
    procStore.start(NUM_THREADS);
    ProcedureTestingUtility.initAndStartWorkers(procExecutor, 1, true);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  @Test
  public void testSingleStepReplayOrder() throws Exception {
    final int NUM_PROC_XTHREAD = 32;
    final int NUM_PROCS = NUM_THREADS * NUM_PROC_XTHREAD;

    // submit the procedures
    submitProcedures(NUM_THREADS, NUM_PROC_XTHREAD, TestSingleStepProcedure.class);

    while (procEnv.getExecId() < NUM_PROCS) {
      Thread.sleep(100);
    }

    // restart the executor and allow the procedures to run
    ProcedureTestingUtility.restart(procExecutor);

    // wait the execution of all the procedures and
    // assert that the execution order was sorted by procId
    ProcedureTestingUtility.waitNoProcedureRunning(procExecutor);
    procEnv.assertSortedExecList(NUM_PROCS);
  }

  @Test
  public void testMultiStepReplayOrder() throws Exception {
    final int NUM_PROC_XTHREAD = 24;
    final int NUM_PROCS = NUM_THREADS * (NUM_PROC_XTHREAD * 2);

    // submit the procedures
    submitProcedures(NUM_THREADS, NUM_PROC_XTHREAD, TestTwoStepProcedure.class);

    while (procEnv.getExecId() < NUM_PROCS) {
      Thread.sleep(100);
    }

    // restart the executor and allow the procedures to run
    ProcedureTestingUtility.restart(procExecutor);

    // wait the execution of all the procedures and
    // assert that the execution order was sorted by procId
    ProcedureTestingUtility.waitNoProcedureRunning(procExecutor);
    procEnv.assertSortedExecList(NUM_PROCS);
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
              procExecutor.submitProcedure((Procedure)
                procClazz.getDeclaredConstructor().newInstance());
            } catch (Exception e) {
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
    private ArrayList<TestProcedure> execList = new ArrayList<>();
    private AtomicLong execTimestamp = new AtomicLong(0);

    public long getExecId() {
      return execTimestamp.get();
    }

    public long nextExecId() {
      return execTimestamp.incrementAndGet();
    }

    public void addToExecList(final TestProcedure proc) {
      execList.add(proc);
    }

    public void assertSortedExecList(int numProcs) {
      assertEquals(numProcs, execList.size());
      LOG.debug("EXEC LIST: " + execList);
      for (int i = 0; i < execList.size() - 1; ++i) {
        TestProcedure a = execList.get(i);
        TestProcedure b = execList.get(i + 1);
        assertTrue("exec list not sorted: " + a + " < " + b, a.getExecId() > b.getExecId());
      }
    }
  }

  public static abstract class TestProcedure extends Procedure<TestProcedureEnv> {
    protected long execId = 0;
    protected int step = 0;

    public long getExecId() {
      return execId;
    }

    @Override
    protected void rollback(TestProcedureEnv env) { }

    @Override
    protected boolean abort(TestProcedureEnv env) {
      return true;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      Int64Value.Builder builder = Int64Value.newBuilder().setValue(execId);
      serializer.serialize(builder.build());
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      Int64Value value = serializer.deserialize(Int64Value.class);
      execId = value.getValue();
      step = 2;
    }
  }

  public static class TestSingleStepProcedure extends TestProcedure {
    public TestSingleStepProcedure() { }

    @Override
    protected Procedure[] execute(TestProcedureEnv env) throws ProcedureYieldException {
      LOG.trace("execute procedure step=" + step + ": " + this);
      if (step == 0) {
        step = 1;
        execId = env.nextExecId();
        return new Procedure[] { this };
      } else if (step == 2) {
        env.addToExecList(this);
        return null;
      }
      throw new ProcedureYieldException();
    }

    @Override
    public String toString() {
      return "SingleStep(procId=" + getProcId() + " execId=" + execId + ")";
    }
  }

  public static class TestTwoStepProcedure extends TestProcedure {
    public TestTwoStepProcedure() { }

    @Override
    protected Procedure[] execute(TestProcedureEnv env) throws ProcedureYieldException {
      LOG.trace("execute procedure step=" + step + ": " + this);
      if (step == 0) {
        step = 1;
        execId = env.nextExecId();
        return new Procedure[] { new TestSingleStepProcedure() };
      } else if (step == 2) {
        env.addToExecList(this);
        return null;
      }
      throw new ProcedureYieldException();
    }

    @Override
    public String toString() {
      return "TwoStep(procId=" + getProcId() + " execId=" + execId + ")";
    }
  }
}
