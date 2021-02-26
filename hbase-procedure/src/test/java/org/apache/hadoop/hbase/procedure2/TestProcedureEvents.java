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

import org.apache.hbase.thirdparty.com.google.protobuf.Int32Value;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@Category({MasterTests.class, MediumTests.class})
public class TestProcedureEvents {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureEvents.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureEvents.class);

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
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<>(htu.getConfiguration(), procEnv, procStore);
    procStore.start(1);
    ProcedureTestingUtility.initAndStartWorkers(procExecutor, 1, true);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    procExecutor.join();
    fs.delete(logDir, true);
  }

  /**
   * Tests being able to suspend a Procedure for N timeouts and then failing.s
   * Resets the timeout after each elapses. See {@link TestTimeoutEventProcedure} for example
   * of how to do this sort of trickery with the ProcedureExecutor; i.e. suspend for a while,
   * check for a condition and if not set, suspend again, etc., ultimately failing or succeeding
   * eventually.
   */
  @Test
  public void testTimeoutEventProcedure() throws Exception {
    final int NTIMEOUTS = 5;

    TestTimeoutEventProcedure proc = new TestTimeoutEventProcedure(500, NTIMEOUTS);
    procExecutor.submitProcedure(proc);

    ProcedureTestingUtility.waitProcedure(procExecutor, proc.getProcId());
    ProcedureTestingUtility.assertIsAbortException(procExecutor.getResult(proc.getProcId()));
    assertEquals(NTIMEOUTS + 1, proc.getTimeoutsCount());
  }

  @Test
  public void testTimeoutEventProcedureDoubleExecution() throws Exception {
    testTimeoutEventProcedureDoubleExecution(false);
  }

  @Test
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

  /**
   * This Event+Procedure exhibits following behavior:
   * <ul>
   *   <li>On procedure execute()
   *     <ul>
   *       <li>If had enough timeouts, abort the procedure. Else....</li>
   *       <li>Suspend the event and add self to its suspend queue</li>
   *       <li>Go into waiting state</li>
   *     </ul>
   *   </li>
   *   <li>
   *     On waiting timeout
   *     <ul>
   *       <li>Wake the event (which adds this procedure back into scheduler queue), and set own's
   *       state to RUNNABLE (so can be executed again).</li>
   *     </ul>
   *   </li>
   * </ul>
   */
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

      event.suspend();
      if (event.suspendIfNotReady(this)) {
        setState(ProcedureState.WAITING_TIMEOUT);
        throw new ProcedureSuspendedException();
      }

      return null;
    }

    @Override
    protected synchronized boolean setTimeoutFailure(final TestProcEnv env) {
      int n = ntimeouts.incrementAndGet();
      LOG.info("HANDLE TIMEOUT " + this + " ntimeouts=" + n);
      setState(ProcedureState.RUNNABLE);
      event.wake((AbstractProcedureScheduler) env.getProcedureScheduler());
      return false;
    }

    @Override
    protected void afterReplay(final TestProcEnv env) {
      if (getState() == ProcedureState.WAITING_TIMEOUT) {
        event.suspend();
        event.suspendIfNotReady(this);
      }
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      Int32Value.Builder ntimeoutsBuilder = Int32Value.newBuilder().setValue(ntimeouts.get());
      serializer.serialize(ntimeoutsBuilder.build());

      Int32Value.Builder maxTimeoutsBuilder = Int32Value.newBuilder().setValue(maxTimeouts);
      serializer.serialize(maxTimeoutsBuilder.build());
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      Int32Value ntimeoutsValue = serializer.deserialize(Int32Value.class);
      ntimeouts.set(ntimeoutsValue.getValue());

      Int32Value maxTimeoutsValue = serializer.deserialize(Int32Value.class);
      maxTimeouts = maxTimeoutsValue.getValue();
    }
  }

  private class TestProcEnv {
    public ProcedureScheduler getProcedureScheduler() {
      return procExecutor.getScheduler();
    }
  }
}
