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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureEvents {
  private static final Log LOG = LogFactory.getLog(TestProcedureEvents.class);

  private TestProcEnv procEnv;
  private ProcedureStore procStore;
  private ProcedureExecutor<TestProcEnv> procExecutor;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    Path testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    logDir = new Path(testDir, "proc-logs");

    procEnv = new TestProcEnv();
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), fs, logDir);
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), procEnv, procStore);
    procStore.start(1);
    procExecutor.start(1, true);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    procExecutor.join();
    fs.delete(logDir, true);
  }

  @Test(timeout=30000)
  public void testTimeoutEventProcedure() throws Exception {
    final int NTIMEOUTS = 5;

    TestTimeoutEventProcedure proc = new TestTimeoutEventProcedure(500, NTIMEOUTS);
    procExecutor.submitProcedure(proc);

    ProcedureTestingUtility.waitProcedure(procExecutor, proc.getProcId());
    ProcedureTestingUtility.assertIsAbortException(procExecutor.getResult(proc.getProcId()));
    assertEquals(NTIMEOUTS + 1, proc.getTimeoutsCount());
  }

  @Test(timeout=30000)
  public void testTimeoutEventProcedureDoubleExecution() throws Exception {
    testTimeoutEventProcedureDoubleExecution(false);
  }

  @Test(timeout=30000)
  public void testTimeoutEventProcedureDoubleExecutionKillIfSuspended() throws Exception {
    testTimeoutEventProcedureDoubleExecution(true);
  }

  private void testTimeoutEventProcedureDoubleExecution(final boolean killIfSuspended)
      throws Exception {
    TestTimeoutEventProcedure proc = new TestTimeoutEventProcedure(1000, 3);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExecutor, true);
    ProcedureTestingUtility.setKillIfSuspended(procExecutor, killIfSuspended);
    long procId = procExecutor.submitProcedure(proc);
    ProcedureTestingUtility.testRecoveryAndDoubleExecution(procExecutor, procId, true);
    ProcedureTestingUtility.assertIsAbortException(procExecutor.getResult(proc.getProcId()));
  }

  public static class TestTimeoutEventProcedure extends NoopProcedure<TestProcEnv> {
    private final ProcedureEvent event = new ProcedureEvent("timeout-event");

    private final AtomicInteger ntimeouts = new AtomicInteger(0);
    private int maxTimeouts = 1;

    public TestTimeoutEventProcedure() {}

    public TestTimeoutEventProcedure(final int timeoutMsec, final int maxTimeouts) {
      this.maxTimeouts = maxTimeouts;
      setTimeout(timeoutMsec);
    }

    public int getTimeoutsCount() {
      return ntimeouts.get();
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env) throws ProcedureSuspendedException {
      LOG.info("EXECUTE " + this + " ntimeouts=" + ntimeouts);
      if (ntimeouts.get() > maxTimeouts) {
        setAbortFailure("test", "give up after " + ntimeouts.get());
        return null;
      }

      env.getProcedureScheduler().suspendEvent(event);
      if (env.getProcedureScheduler().waitEvent(event, this)) {
        setState(ProcedureState.WAITING_TIMEOUT);
        throw new ProcedureSuspendedException();
      }

      return null;
    }

    @Override
    protected boolean setTimeoutFailure(final TestProcEnv env) {
      int n = ntimeouts.incrementAndGet();
      LOG.info("HANDLE TIMEOUT " + this + " ntimeouts=" + n);
      setState(ProcedureState.RUNNABLE);
      env.getProcedureScheduler().wakeEvent(event);
      return false;
    }

    @Override
    protected void afterReplay(final TestProcEnv env) {
      if (getState() == ProcedureState.WAITING_TIMEOUT) {
        env.getProcedureScheduler().suspendEvent(event);
        env.getProcedureScheduler().waitEvent(event, this);
      }
    }

    @Override
    protected void serializeStateData(final OutputStream stream) throws IOException {
      StreamUtils.writeRawVInt32(stream, ntimeouts.get());
      StreamUtils.writeRawVInt32(stream, maxTimeouts);
    }

    @Override
    protected void deserializeStateData(final InputStream stream) throws IOException {
      ntimeouts.set(StreamUtils.readRawVarint32(stream));
      maxTimeouts = StreamUtils.readRawVarint32(stream);
    }
  }

  private class TestProcEnv {
    public ProcedureScheduler getProcedureScheduler() {
      return procExecutor.getScheduler();
    }
  }
}
