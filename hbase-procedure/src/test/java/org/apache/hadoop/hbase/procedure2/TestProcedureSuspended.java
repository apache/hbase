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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.NoopProcedureStore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Threads;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureSuspended {
  private static final Log LOG = LogFactory.getLog(TestProcedureSuspended.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final Procedure NULL_PROC = null;

  private ProcedureExecutor<TestProcEnv> procExecutor;
  private ProcedureStore procStore;

  private HBaseCommonTestingUtility htu;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();

    procStore = new NoopProcedureStore();
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), new TestProcEnv(), procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS, true);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
  }

  @Test(timeout=10000)
  public void testSuspendWhileHoldingLocks() {
    final AtomicBoolean lockA = new AtomicBoolean(false);
    final AtomicBoolean lockB = new AtomicBoolean(false);

    final TestLockProcedure p1keyA = new TestLockProcedure(lockA, "keyA", false, true);
    final TestLockProcedure p2keyA = new TestLockProcedure(lockA, "keyA", false, true);
    final TestLockProcedure p3keyB = new TestLockProcedure(lockB, "keyB", false, true);

    procExecutor.submitProcedure(p1keyA);
    procExecutor.submitProcedure(p2keyA);
    procExecutor.submitProcedure(p3keyB);

    // first run p1, p3 are able to run p2 is blocked by p1
    waitAndAssertTimestamp(p1keyA, 1, 1);
    waitAndAssertTimestamp(p2keyA, 0, -1);
    waitAndAssertTimestamp(p3keyB, 1, 2);
    assertEquals(true, lockA.get());
    assertEquals(true, lockB.get());

    // release p3
    p3keyB.setThrowSuspend(false);
    procExecutor.getRunnableSet().addFront(p3keyB);
    waitAndAssertTimestamp(p1keyA, 1, 1);
    waitAndAssertTimestamp(p2keyA, 0, -1);
    waitAndAssertTimestamp(p3keyB, 2, 3);
    assertEquals(true, lockA.get());

    // wait until p3 is fully completed
    ProcedureTestingUtility.waitProcedure(procExecutor, p3keyB);
    assertEquals(false, lockB.get());

    // rollback p2 and wait until is fully completed
    p1keyA.setTriggerRollback(true);
    procExecutor.getRunnableSet().addFront(p1keyA);
    ProcedureTestingUtility.waitProcedure(procExecutor, p1keyA);

    // p2 should start and suspend
    waitAndAssertTimestamp(p1keyA, 4, 60000);
    waitAndAssertTimestamp(p2keyA, 1, 7);
    waitAndAssertTimestamp(p3keyB, 2, 3);
    assertEquals(true, lockA.get());

    // wait until p2 is fully completed
    p2keyA.setThrowSuspend(false);
    procExecutor.getRunnableSet().addFront(p2keyA);
    ProcedureTestingUtility.waitProcedure(procExecutor, p2keyA);
    waitAndAssertTimestamp(p1keyA, 4, 60000);
    waitAndAssertTimestamp(p2keyA, 2, 8);
    waitAndAssertTimestamp(p3keyB, 2, 3);
    assertEquals(false, lockA.get());
    assertEquals(false, lockB.get());
  }

  @Test(timeout=10000)
  public void testYieldWhileHoldingLocks() {
    final AtomicBoolean lock = new AtomicBoolean(false);

    final TestLockProcedure p1 = new TestLockProcedure(lock, "key", true, false);
    final TestLockProcedure p2 = new TestLockProcedure(lock, "key", true, false);

    procExecutor.submitProcedure(p1);
    procExecutor.submitProcedure(p2);

    // try to execute a bunch of yield on p1, p2 should be blocked
    while (p1.getTimestamps().size() < 100) Threads.sleep(10);
    assertEquals(0, p2.getTimestamps().size());

    // wait until p1 is completed
    p1.setThrowYield(false);
    ProcedureTestingUtility.waitProcedure(procExecutor, p1);

    // try to execute a bunch of yield on p2
    while (p2.getTimestamps().size() < 100) Threads.sleep(10);
    assertEquals(p1.getTimestamps().get(p1.getTimestamps().size() - 1).longValue() + 1,
      p2.getTimestamps().get(0).longValue());

    // wait until p2 is completed
    p1.setThrowYield(false);
    ProcedureTestingUtility.waitProcedure(procExecutor, p1);
  }

  private void waitAndAssertTimestamp(TestLockProcedure proc, int size, int lastTs) {
    final ArrayList<Long> timestamps = proc.getTimestamps();
    while (timestamps.size() < size) Threads.sleep(10);
    LOG.info(proc + " -> " + timestamps);
    assertEquals(size, timestamps.size());
    if (size > 0) {
      assertEquals(lastTs, timestamps.get(timestamps.size() - 1).longValue());
    }
  }

  public static class TestLockProcedure extends Procedure<TestProcEnv> {
    private final ArrayList<Long> timestamps = new ArrayList<Long>();
    private final String key;

    private boolean triggerRollback = false;
    private boolean throwSuspend = false;
    private boolean throwYield = false;
    private AtomicBoolean lock = null;
    private boolean hasLock = false;

    public TestLockProcedure(final AtomicBoolean lock, final String key,
        final boolean throwYield, final boolean throwSuspend) {
      this.lock = lock;
      this.key = key;
      this.throwYield = throwYield;
      this.throwSuspend = throwSuspend;
    }

    public void setThrowYield(final boolean throwYield) {
      this.throwYield = throwYield;
    }

    public void setThrowSuspend(final boolean throwSuspend) {
      this.throwSuspend = throwSuspend;
    }

    public void setTriggerRollback(final boolean triggerRollback) {
      this.triggerRollback = triggerRollback;
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException {
      LOG.info("EXECUTE " + this + " suspend " + (lock != null));
      timestamps.add(env.nextTimestamp());
      if (triggerRollback) {
        setFailure(getClass().getSimpleName(), new Exception("injected failure"));
      } else if (throwYield) {
        throw new ProcedureYieldException();
      } else if (throwSuspend) {
        throw new ProcedureSuspendedException();
      }
      return null;
    }

    @Override
    protected void rollback(final TestProcEnv env) {
      LOG.info("ROLLBACK " + this);
      timestamps.add(env.nextTimestamp() * 10000);
    }

    @Override
    protected boolean acquireLock(final TestProcEnv env) {
      if ((hasLock = lock.compareAndSet(false, true))) {
        LOG.info("ACQUIRE LOCK " + this + " " + (hasLock));
      }
      return hasLock;
    }

    @Override
    protected void releaseLock(final TestProcEnv env) {
      LOG.info("RELEASE LOCK " + this + " " + hasLock);
      lock.set(false);
      hasLock = false;
    }

    @Override
    protected boolean holdLock(final TestProcEnv env) {
      return true;
    }

    @Override
    protected boolean hasLock(final TestProcEnv env) {
      return hasLock;
    }

    public ArrayList<Long> getTimestamps() {
      return timestamps;
    }

    @Override
    protected void toStringClassDetails(StringBuilder builder) {
      builder.append(getClass().getName());
      builder.append("(" + key + ")");
    }

    @Override
    protected boolean abort(TestProcEnv env) { return false; }

    @Override
    protected void serializeStateData(final OutputStream stream) throws IOException {
    }

    @Override
    protected void deserializeStateData(final InputStream stream) throws IOException {
    }
  }

  private static class TestProcEnv {
    public final AtomicLong timestamp = new AtomicLong(0);

    public long nextTimestamp() {
      return timestamp.incrementAndGet();
    }
  }
}