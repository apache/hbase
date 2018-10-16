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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.NO_NONCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category({RegionServerTests.class, SmallTests.class})
public class TestServerNonceManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestServerNonceManager.class);

  @Test
  public void testMvcc() throws Exception {
    ServerNonceManager nm = createManager();
    final long group = 100;
    final long nonce = 1;
    final long initMvcc = 999;
    assertTrue(nm.startOperation(group, nonce, createStoppable()));
    nm.addMvccToOperationContext(group, nonce, initMvcc);
    nm.endOperation(group, nonce, true);
    assertEquals(initMvcc, nm.getMvccFromOperationContext(group, nonce));
    long newMvcc = initMvcc + 1;
    for (long newNonce = nonce + 1; newNonce != (nonce + 5); ++newNonce) {
      assertTrue(nm.startOperation(group, newNonce, createStoppable()));
      nm.addMvccToOperationContext(group, newNonce, newMvcc);
      nm.endOperation(group, newNonce, true);
      assertEquals(newMvcc, nm.getMvccFromOperationContext(group, newNonce));
      ++newMvcc;
    }
    assertEquals(initMvcc, nm.getMvccFromOperationContext(group, nonce));
  }

  @Test
  public void testNormalStartEnd() throws Exception {
    final long[] numbers = new long[] { NO_NONCE, 1, 2, Long.MAX_VALUE, Long.MIN_VALUE };
    ServerNonceManager nm = createManager();
    for (int i = 0; i < numbers.length; ++i) {
      for (int j = 0; j < numbers.length; ++j) {
        assertTrue(nm.startOperation(numbers[i], numbers[j], createStoppable()));
      }
    }
    // Should be able to start operation the second time w/o nonces.
    for (int i = 0; i < numbers.length; ++i) {
      assertTrue(nm.startOperation(numbers[i], NO_NONCE, createStoppable()));
    }
    // Fail all operations - should be able to restart.
    for (int i = 0; i < numbers.length; ++i) {
      for (int j = 0; j < numbers.length; ++j) {
        nm.endOperation(numbers[i], numbers[j], false);
        assertTrue(nm.startOperation(numbers[i], numbers[j], createStoppable()));
      }
    }
    // Succeed all operations - should not be able to restart, except for NO_NONCE.
    for (int i = 0; i < numbers.length; ++i) {
      for (int j = 0; j < numbers.length; ++j) {
        nm.endOperation(numbers[i], numbers[j], true);
        assertEquals(numbers[j] == NO_NONCE,
            nm.startOperation(numbers[i], numbers[j], createStoppable()));
      }
    }
  }

  @Test
  public void testNoEndWithoutStart() {
    ServerNonceManager nm = createManager();
    try {
      nm.endOperation(NO_NONCE, 1, true);
      throw new Error("Should have thrown");
    } catch (AssertionError err) {}
  }

  @Test
  public void testCleanup() throws Exception {
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);
    try {
      ServerNonceManager nm = createManager(6);
      ScheduledChore cleanup = nm.createCleanupScheduledChore(Mockito.mock(Stoppable.class));
      edge.setValue(1);
      assertTrue(nm.startOperation(NO_NONCE, 1, createStoppable()));
      assertTrue(nm.startOperation(NO_NONCE, 2, createStoppable()));
      assertTrue(nm.startOperation(NO_NONCE, 3, createStoppable()));
      edge.setValue(2);
      nm.endOperation(NO_NONCE, 1, true);
      edge.setValue(4);
      nm.endOperation(NO_NONCE, 2, true);
      edge.setValue(9);
      cleanup.choreForTesting();
      // Nonce 1 has been cleaned up.
      assertTrue(nm.startOperation(NO_NONCE, 1, createStoppable()));
      // Nonce 2 has not been cleaned up.
      assertFalse(nm.startOperation(NO_NONCE, 2, createStoppable()));
      // Nonce 3 was active and active ops should never be cleaned up; try to end and start.
      nm.endOperation(NO_NONCE, 3, false);
      assertTrue(nm.startOperation(NO_NONCE, 3, createStoppable()));
      edge.setValue(11);
      cleanup.choreForTesting();
      // Now, nonce 2 has been cleaned up.
      assertTrue(nm.startOperation(NO_NONCE, 2, createStoppable()));
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  @Test
  public void testWalNonces() throws Exception {
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);
    try {
      ServerNonceManager nm = createManager(6);
      ScheduledChore cleanup = nm.createCleanupScheduledChore(Mockito.mock(Stoppable.class));
      // Add nonces from WAL, including dups.
      edge.setValue(12);
      nm.reportOperationFromWal(NO_NONCE, 1, 8);
      nm.reportOperationFromWal(NO_NONCE, 2, 2);
      nm.reportOperationFromWal(NO_NONCE, 3, 5);
      nm.reportOperationFromWal(NO_NONCE, 3, 6);
      // WAL nonces should prevent cross-server conflicts.
      assertFalse(nm.startOperation(NO_NONCE, 1, createStoppable()));
      // Make sure we ignore very old nonces, but not borderline old nonces.
      assertTrue(nm.startOperation(NO_NONCE, 2, createStoppable()));
      assertFalse(nm.startOperation(NO_NONCE, 3, createStoppable()));
      // Make sure grace period is counted from recovery time.
      edge.setValue(17);
      cleanup.choreForTesting();
      assertFalse(nm.startOperation(NO_NONCE, 1, createStoppable()));
      assertFalse(nm.startOperation(NO_NONCE, 3, createStoppable()));
      edge.setValue(19);
      cleanup.choreForTesting();
      assertTrue(nm.startOperation(NO_NONCE, 1, createStoppable()));
      assertTrue(nm.startOperation(NO_NONCE, 3, createStoppable()));
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  @Test
  public void testConcurrentAttempts() throws Exception {
    final ServerNonceManager nm = createManager();

    nm.startOperation(NO_NONCE, 1, createStoppable());
    TestRunnable tr = new TestRunnable(nm, 1, false, createStoppable());
    Thread t = tr.start();
    waitForThreadToBlockOrExit(t);
    nm.endOperation(NO_NONCE, 1, true); // operation succeeded
    t.join(); // thread must now unblock and not proceed (result checked inside).
    tr.propagateError();

    nm.startOperation(NO_NONCE, 2, createStoppable());
    tr = new TestRunnable(nm, 2, true, createStoppable());
    t = tr.start();
    waitForThreadToBlockOrExit(t);
    nm.endOperation(NO_NONCE, 2, false);
    t.join(); // thread must now unblock and allow us to proceed (result checked inside).
    tr.propagateError();
    nm.endOperation(NO_NONCE, 2, true); // that is to say we should be able to end operation

    nm.startOperation(NO_NONCE, 3, createStoppable());
    tr = new TestRunnable(nm, 4, true, createStoppable());
    tr.start().join();  // nonce 3 must have no bearing on nonce 4
    tr.propagateError();
  }

  @Test
  public void testStopWaiting() throws Exception {
    final ServerNonceManager nm = createManager();
    nm.setConflictWaitIterationMs(1);
    Stoppable stoppingStoppable = createStoppable();
    Mockito.when(stoppingStoppable.isStopped()).thenAnswer(new Answer<Boolean>() {
      AtomicInteger answer = new AtomicInteger(3);
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return 0 < answer.decrementAndGet();
      }
    });

    nm.startOperation(NO_NONCE, 1, createStoppable());
    TestRunnable tr = new TestRunnable(nm, 1, null, stoppingStoppable);
    Thread t = tr.start();
    waitForThreadToBlockOrExit(t);
    // thread must eventually throw
    t.join();
    tr.propagateError();
  }

  private void waitForThreadToBlockOrExit(Thread t) throws InterruptedException {
    for (int i = 9; i >= 0; --i) {
      if (t.getState() == Thread.State.TIMED_WAITING || t.getState() == Thread.State.WAITING
          || t.getState() == Thread.State.BLOCKED || t.getState() == Thread.State.TERMINATED) {
        return;
      }
      if (i > 0) Thread.sleep(300);
    }
    // Thread didn't block in 3 seconds. What is it doing? Continue the test, we'd rather
    // have a very strange false positive then false negative due to timing.
  }

  private static class TestRunnable implements Runnable {
    public final CountDownLatch startedLatch = new CountDownLatch(1); // It's the final countdown!

    private final ServerNonceManager nm;
    private final long nonce;
    private final Boolean expected;
    private final Stoppable stoppable;

    private Throwable throwable = null;

    public TestRunnable(ServerNonceManager nm, long nonce, Boolean expected, Stoppable stoppable) {
      this.nm = nm;
      this.nonce = nonce;
      this.expected = expected;
      this.stoppable = stoppable;
    }

    public void propagateError() throws Exception {
      if (throwable == null) return;
      throw new Exception(throwable);
    }

    public Thread start() {
      Thread t = new Thread(this);
      t = Threads.setDaemonThreadRunning(t);
      try {
        startedLatch.await();
      } catch (InterruptedException e) {
        fail("Unexpected");
      }
      return t;
    }

    @Override
    public void run() {
      startedLatch.countDown();
      boolean shouldThrow = expected == null;
      boolean hasThrown = true;
      try {
        boolean result = nm.startOperation(NO_NONCE, nonce, stoppable);
        hasThrown = false;
        if (!shouldThrow) {
          assertEquals(expected.booleanValue(), result);
        }
      } catch (Throwable t) {
        if (!shouldThrow) {
          throwable = t;
        }
      }
      if (shouldThrow && !hasThrown) {
        throwable = new AssertionError("Should have thrown");
      }
    }
  }

  private Stoppable createStoppable() {
    Stoppable s = Mockito.mock(Stoppable.class);
    Mockito.when(s.isStopped()).thenReturn(false);
    return s;
  }

  private ServerNonceManager createManager() {
    return createManager(null);
  }

  private ServerNonceManager createManager(Integer gracePeriod) {
    Configuration conf = HBaseConfiguration.create();
    if (gracePeriod != null) {
      conf.setInt(ServerNonceManager.HASH_NONCE_GRACE_PERIOD_KEY, gracePeriod.intValue());
    }
    return new ServerNonceManager(conf);
  }
}
