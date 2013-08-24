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
package org.apache.hadoop.hbase.procedure;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

/**
 * Test Procedure coordinator operation.
 * <p>
 * This only works correctly when we do <i>class level parallelization</i> of tests. If we do method
 * level serialization this class will likely throw all kinds of errors.
 */
@Category(SmallTests.class)
public class TestProcedureCoordinator {
  // general test constants
  private static final long WAKE_FREQUENCY = 1000;
  private static final long TIMEOUT = 100000;
  private static final long POOL_KEEP_ALIVE = 1;
  private static final String nodeName = "node";
  private static final String procName = "some op";
  private static final byte[] procData = new byte[0];
  private static final List<String> expected = Lists.newArrayList("remote1", "remote2");

  // setup the mocks
  private final ProcedureCoordinatorRpcs controller = mock(ProcedureCoordinatorRpcs.class);
  private final Procedure task = mock(Procedure.class);
  private final ForeignExceptionDispatcher monitor = mock(ForeignExceptionDispatcher.class);

  // handle to the coordinator for each test
  private ProcedureCoordinator coordinator;

  @After
  public void resetTest() throws IOException {
    // reset all the mocks used for the tests
    reset(controller, task, monitor);
    // close the open coordinator, if it was used
    if (coordinator != null) coordinator.close();
  }

  private ProcedureCoordinator buildNewCoordinator() {
    ThreadPoolExecutor pool = ProcedureCoordinator.defaultPool(nodeName, 1, POOL_KEEP_ALIVE);
    return spy(new ProcedureCoordinator(controller, pool));
  }

  /**
   * Currently we can only handle one procedure at a time.  This makes sure we handle that and
   * reject submitting more.
   */
  @Test
  public void testThreadPoolSize() throws Exception {
    ProcedureCoordinator coordinator = buildNewCoordinator();
    Procedure proc = new Procedure(coordinator,  monitor,
        WAKE_FREQUENCY, TIMEOUT, procName, procData, expected);
    Procedure procSpy = spy(proc);

    Procedure proc2 = new Procedure(coordinator,  monitor,
        WAKE_FREQUENCY, TIMEOUT, procName +"2", procData, expected);
    Procedure procSpy2 = spy(proc2);
    when(coordinator.createProcedure(any(ForeignExceptionDispatcher.class), eq(procName), eq(procData), anyListOf(String.class)))
    .thenReturn(procSpy, procSpy2);

    coordinator.startProcedure(procSpy.getErrorMonitor(), procName, procData, expected);
    // null here means second procedure failed to start.
    assertNull("Coordinator successfully ran two tasks at once with a single thread pool.",
      coordinator.startProcedure(proc2.getErrorMonitor(), "another op", procData, expected));
  }

  /**
   * Check handling a connection failure correctly if we get it during the acquiring phase
   */
  @Test(timeout = 60000)
  public void testUnreachableControllerDuringPrepare() throws Exception {
    coordinator = buildNewCoordinator();
    // setup the proc
    List<String> expected = Arrays.asList("cohort");
    Procedure proc = new Procedure(coordinator, WAKE_FREQUENCY,
        TIMEOUT, procName, procData, expected);
    final Procedure procSpy = spy(proc);

    when(coordinator.createProcedure(any(ForeignExceptionDispatcher.class), eq(procName), eq(procData), anyListOf(String.class)))
        .thenReturn(procSpy);

    // use the passed controller responses
    IOException cause = new IOException("Failed to reach comms during acquire");
    doThrow(cause).when(controller)
        .sendGlobalBarrierAcquire(eq(procSpy), eq(procData), anyListOf(String.class));

    // run the operation
    proc = coordinator.startProcedure(proc.getErrorMonitor(), procName, procData, expected);
    // and wait for it to finish
    while(!proc.completedLatch.await(WAKE_FREQUENCY, TimeUnit.MILLISECONDS));
    verify(procSpy, atLeastOnce()).receive(any(ForeignException.class));
    verify(coordinator, times(1)).rpcConnectionFailure(anyString(), eq(cause));
    verify(controller, times(1)).sendGlobalBarrierAcquire(procSpy, procData, expected);
    verify(controller, never()).sendGlobalBarrierReached(any(Procedure.class),
        anyListOf(String.class));
  }

  /**
   * Check handling a connection failure correctly if we get it during the barrier phase
   */
  @Test(timeout = 60000)
  public void testUnreachableControllerDuringCommit() throws Exception {
    coordinator = buildNewCoordinator();

    // setup the task and spy on it
    List<String> expected = Arrays.asList("cohort");
    final Procedure spy = spy(new Procedure(coordinator,
        WAKE_FREQUENCY, TIMEOUT, procName, procData, expected));

    when(coordinator.createProcedure(any(ForeignExceptionDispatcher.class), eq(procName), eq(procData), anyListOf(String.class)))
    .thenReturn(spy);

    // use the passed controller responses
    IOException cause = new IOException("Failed to reach controller during prepare");
    doAnswer(new AcquireBarrierAnswer(procName, new String[] { "cohort" }))
        .when(controller).sendGlobalBarrierAcquire(eq(spy), eq(procData), anyListOf(String.class));
    doThrow(cause).when(controller).sendGlobalBarrierReached(eq(spy), anyListOf(String.class));

    // run the operation
    Procedure task = coordinator.startProcedure(spy.getErrorMonitor(), procName, procData, expected);
    // and wait for it to finish
    while(!task.completedLatch.await(WAKE_FREQUENCY, TimeUnit.MILLISECONDS));
    verify(spy, atLeastOnce()).receive(any(ForeignException.class));
    verify(coordinator, times(1)).rpcConnectionFailure(anyString(), eq(cause));
    verify(controller, times(1)).sendGlobalBarrierAcquire(eq(spy),
        eq(procData), anyListOf(String.class));
    verify(controller, times(1)).sendGlobalBarrierReached(any(Procedure.class),
        anyListOf(String.class));
  }

  @Test(timeout = 60000)
  public void testNoCohort() throws Exception {
    runSimpleProcedure();
  }

  @Test(timeout = 60000)
  public void testSingleCohortOrchestration() throws Exception {
    runSimpleProcedure("one");
  }

  @Test(timeout = 60000)
  public void testMultipleCohortOrchestration() throws Exception {
    runSimpleProcedure("one", "two", "three", "four");
  }

  public void runSimpleProcedure(String... members) throws Exception {
    coordinator = buildNewCoordinator();
    Procedure task = new Procedure(coordinator, monitor, WAKE_FREQUENCY,
        TIMEOUT, procName, procData, Arrays.asList(members));
    final Procedure spy = spy(task);
    runCoordinatedProcedure(spy, members);
  }

  /**
   * Test that if nodes join the barrier early we still correctly handle the progress
   */
  @Test(timeout = 60000)
  public void testEarlyJoiningBarrier() throws Exception {
    final String[] cohort = new String[] { "one", "two", "three", "four" };
    coordinator = buildNewCoordinator();
    final ProcedureCoordinator ref = coordinator;
    Procedure task = new Procedure(coordinator, monitor, WAKE_FREQUENCY,
        TIMEOUT, procName, procData, Arrays.asList(cohort));
    final Procedure spy = spy(task);

    AcquireBarrierAnswer prepare = new AcquireBarrierAnswer(procName, cohort) {
      public void doWork() {
        // then do some fun where we commit before all nodes have prepared
        // "one" commits before anyone else is done
        ref.memberAcquiredBarrier(this.opName, this.cohort[0]);
        ref.memberFinishedBarrier(this.opName, this.cohort[0]);
        // but "two" takes a while
        ref.memberAcquiredBarrier(this.opName, this.cohort[1]);
        // "three"jumps ahead
        ref.memberAcquiredBarrier(this.opName, this.cohort[2]);
        ref.memberFinishedBarrier(this.opName, this.cohort[2]);
        // and "four" takes a while
        ref.memberAcquiredBarrier(this.opName, this.cohort[3]);
      }
    };

    BarrierAnswer commit = new BarrierAnswer(procName, cohort) {
      @Override
      public void doWork() {
        ref.memberFinishedBarrier(opName, this.cohort[1]);
        ref.memberFinishedBarrier(opName, this.cohort[3]);
      }
    };
    runCoordinatedOperation(spy, prepare, commit, cohort);
  }

  /**
   * Just run a procedure with the standard name and data, with not special task for the mock
   * coordinator (it works just like a regular coordinator). For custom behavior see
   * {@link #runCoordinatedOperation(Procedure, AcquireBarrierAnswer, BarrierAnswer, String[])}
   * .
   * @param spy Spy on a real {@link Procedure}
   * @param cohort expected cohort members
   * @throws Exception on failure
   */
  public void runCoordinatedProcedure(Procedure spy, String... cohort) throws Exception {
    runCoordinatedOperation(spy, new AcquireBarrierAnswer(procName, cohort),
      new BarrierAnswer(procName, cohort), cohort);
  }

  public void runCoordinatedOperation(Procedure spy, AcquireBarrierAnswer prepare,
      String... cohort) throws Exception {
    runCoordinatedOperation(spy, prepare, new BarrierAnswer(procName, cohort), cohort);
  }

  public void runCoordinatedOperation(Procedure spy, BarrierAnswer commit,
      String... cohort) throws Exception {
    runCoordinatedOperation(spy, new AcquireBarrierAnswer(procName, cohort), commit, cohort);
  }

  public void runCoordinatedOperation(Procedure spy, AcquireBarrierAnswer prepareOperation,
      BarrierAnswer commitOperation, String... cohort) throws Exception {
    List<String> expected = Arrays.asList(cohort);
    when(coordinator.createProcedure(any(ForeignExceptionDispatcher.class), eq(procName), eq(procData), anyListOf(String.class)))
      .thenReturn(spy);

    // use the passed controller responses
    doAnswer(prepareOperation).when(controller).sendGlobalBarrierAcquire(spy, procData, expected);
    doAnswer(commitOperation).when(controller)
        .sendGlobalBarrierReached(eq(spy), anyListOf(String.class));

    // run the operation
    Procedure task = coordinator.startProcedure(spy.getErrorMonitor(), procName, procData, expected);
    // and wait for it to finish
    task.waitForCompleted();

    // make sure we mocked correctly
    prepareOperation.ensureRan();
    // we never got an exception
    InOrder inorder = inOrder(spy, controller);
    inorder.verify(spy).sendGlobalBarrierStart();
    inorder.verify(controller).sendGlobalBarrierAcquire(task, procData, expected);
    inorder.verify(spy).sendGlobalBarrierReached();
    inorder.verify(controller).sendGlobalBarrierReached(eq(task), anyListOf(String.class));
  }

  private abstract class OperationAnswer implements Answer<Void> {
    private boolean ran = false;

    public void ensureRan() {
      assertTrue("Prepare mocking didn't actually run!", ran);
    }

    @Override
    public final Void answer(InvocationOnMock invocation) throws Throwable {
      this.ran = true;
      doWork();
      return null;
    }

    protected abstract void doWork() throws Throwable;
  }

  /**
   * Just tell the current coordinator that each of the nodes has prepared
   */
  private class AcquireBarrierAnswer extends OperationAnswer {
    protected final String[] cohort;
    protected final String opName;

    public AcquireBarrierAnswer(String opName, String... cohort) {
      this.cohort = cohort;
      this.opName = opName;
    }

    @Override
    public void doWork() {
      if (cohort == null) return;
      for (String member : cohort) {
        TestProcedureCoordinator.this.coordinator.memberAcquiredBarrier(opName, member);
      }
    }
  }

  /**
   * Just tell the current coordinator that each of the nodes has committed
   */
  private class BarrierAnswer extends OperationAnswer {
    protected final String[] cohort;
    protected final String opName;

    public BarrierAnswer(String opName, String... cohort) {
      this.cohort = cohort;
      this.opName = opName;
    }

    @Override
    public void doWork() {
      if (cohort == null) return;
      for (String member : cohort) {
        TestProcedureCoordinator.this.coordinator.memberFinishedBarrier(opName, member);
      }
    }
  }
}