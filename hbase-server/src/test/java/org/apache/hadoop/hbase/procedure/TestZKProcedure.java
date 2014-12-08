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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.TimeoutException;
import org.apache.hadoop.hbase.procedure.Subprocedure.SubprocedureImpl;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.internal.matchers.ArrayEquals;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import com.google.common.collect.Lists;

/**
 * Cluster-wide testing of a distributed three-phase commit using a 'real' zookeeper cluster
 */
@Category(MediumTests.class)
public class TestZKProcedure {

  private static final Log LOG = LogFactory.getLog(TestZKProcedure.class);
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String COORDINATOR_NODE_NAME = "coordinator";
  private static final long KEEP_ALIVE = 100; // seconds
  private static final int POOL_SIZE = 1;
  private static final long TIMEOUT = 10000; // when debugging make this larger for debugging
  private static final long WAKE_FREQUENCY = 500;
  private static final String opName = "op";
  private static final byte[] data = new byte[] { 1, 2 }; // TODO what is this used for?
  private static final VerificationMode once = Mockito.times(1);

  @BeforeClass
  public static void setupTest() throws Exception {
    UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    UTIL.shutdownMiniZKCluster();
  }

  private static ZooKeeperWatcher newZooKeeperWatcher() throws IOException {
    return new ZooKeeperWatcher(UTIL.getConfiguration(), "testing utility", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(
            "Unexpected abort in distributed three phase commit test:" + why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    });
  }

  @Test
  public void testEmptyMemberSet() throws Exception {
    runCommit();
  }

  @Test
  public void testSingleMember() throws Exception {
    runCommit("one");
  }

  @Test
  public void testMultipleMembers() throws Exception {
    runCommit("one", "two", "three", "four" );
  }

  private void runCommit(String... members) throws Exception {
    // make sure we just have an empty list
    if (members == null) {
      members = new String[0];
    }
    List<String> expected = Arrays.asList(members);

    // setup the constants
    ZooKeeperWatcher coordZkw = newZooKeeperWatcher();
    String opDescription = "coordination test - " + members.length + " cohort members";

    // start running the controller
    ZKProcedureCoordinatorRpcs coordinatorComms = new ZKProcedureCoordinatorRpcs(
        coordZkw, opDescription, COORDINATOR_NODE_NAME);
    ThreadPoolExecutor pool = ProcedureCoordinator.defaultPool(COORDINATOR_NODE_NAME, POOL_SIZE, KEEP_ALIVE);
    ProcedureCoordinator coordinator = new ProcedureCoordinator(coordinatorComms, pool) {
      @Override
      public Procedure createProcedure(ForeignExceptionDispatcher fed, String procName, byte[] procArgs,
          List<String> expectedMembers) {
        return Mockito.spy(super.createProcedure(fed, procName, procArgs, expectedMembers));
      }
    };

    // build and start members
    // NOTE: There is a single subprocedure builder for all members here.
    SubprocedureFactory subprocFactory = Mockito.mock(SubprocedureFactory.class);
    List<Pair<ProcedureMember, ZKProcedureMemberRpcs>> procMembers = new ArrayList<Pair<ProcedureMember, ZKProcedureMemberRpcs>>(
        members.length);
    // start each member
    for (String member : members) {
      ZooKeeperWatcher watcher = newZooKeeperWatcher();
      ZKProcedureMemberRpcs comms = new ZKProcedureMemberRpcs(watcher, opDescription);
      ThreadPoolExecutor pool2 = ProcedureMember.defaultPool(member, 1, KEEP_ALIVE);
      ProcedureMember procMember = new ProcedureMember(comms, pool2, subprocFactory);
      procMembers.add(new Pair<ProcedureMember, ZKProcedureMemberRpcs>(procMember, comms));
      comms.start(member, procMember);
    }

    // setup mock member subprocedures
    final List<Subprocedure> subprocs = new ArrayList<Subprocedure>();
    for (int i = 0; i < procMembers.size(); i++) {
      ForeignExceptionDispatcher cohortMonitor = new ForeignExceptionDispatcher();
      Subprocedure commit = Mockito
      .spy(new SubprocedureImpl(procMembers.get(i).getFirst(), opName, cohortMonitor,
          WAKE_FREQUENCY, TIMEOUT));
      subprocs.add(commit);
    }

    // link subprocedure to buildNewOperation invocation.
    final AtomicInteger i = new AtomicInteger(0); // NOTE: would be racy if not an AtomicInteger
    Mockito.when(subprocFactory.buildSubprocedure(Mockito.eq(opName),
        (byte[]) Mockito.argThat(new ArrayEquals(data)))).thenAnswer(
      new Answer<Subprocedure>() {
        @Override
        public Subprocedure answer(InvocationOnMock invocation) throws Throwable {
          int index = i.getAndIncrement();
          LOG.debug("Task size:" + subprocs.size() + ", getting:" + index);
          Subprocedure commit = subprocs.get(index);
          return commit;
        }
      });

    // setup spying on the coordinator
//    Procedure proc = Mockito.spy(procBuilder.createProcedure(coordinator, opName, data, expected));
//    Mockito.when(procBuilder.build(coordinator, opName, data, expected)).thenReturn(proc);

    // start running the operation
    Procedure task = coordinator.startProcedure(new ForeignExceptionDispatcher(), opName, data, expected);
//    assertEquals("Didn't mock coordinator task", proc, task);

    // verify all things ran as expected
//    waitAndVerifyProc(proc, once, once, never(), once, false);
    waitAndVerifyProc(task, once, once, never(), once, false);
    verifyCohortSuccessful(expected, subprocFactory, subprocs, once, once, never(), once, false);

    // close all the things
    closeAll(coordinator, coordinatorComms, procMembers);
  }

  /**
   * Test a distributed commit with multiple cohort members, where one of the cohort members has a
   * timeout exception during the prepare stage.
   */
  @Test
  public void testMultiCohortWithMemberTimeoutDuringPrepare() throws Exception {
    String opDescription = "error injection coordination";
    String[] cohortMembers = new String[] { "one", "two", "three" };
    List<String> expected = Lists.newArrayList(cohortMembers);
    // error constants
    final int memberErrorIndex = 2;
    final CountDownLatch coordinatorReceivedErrorLatch = new CountDownLatch(1);

    // start running the coordinator and its controller
    ZooKeeperWatcher coordinatorWatcher = newZooKeeperWatcher();
    ZKProcedureCoordinatorRpcs coordinatorController = new ZKProcedureCoordinatorRpcs(
        coordinatorWatcher, opDescription, COORDINATOR_NODE_NAME);
    ThreadPoolExecutor pool = ProcedureCoordinator.defaultPool(COORDINATOR_NODE_NAME, POOL_SIZE, KEEP_ALIVE);
    ProcedureCoordinator coordinator = spy(new ProcedureCoordinator(coordinatorController, pool));

    // start a member for each node
    SubprocedureFactory subprocFactory = Mockito.mock(SubprocedureFactory.class);
    List<Pair<ProcedureMember, ZKProcedureMemberRpcs>> members = new ArrayList<Pair<ProcedureMember, ZKProcedureMemberRpcs>>(
        expected.size());
    for (String member : expected) {
      ZooKeeperWatcher watcher = newZooKeeperWatcher();
      ZKProcedureMemberRpcs controller = new ZKProcedureMemberRpcs(watcher, opDescription);
      ThreadPoolExecutor pool2 = ProcedureMember.defaultPool(member, 1, KEEP_ALIVE);
      ProcedureMember mem = new ProcedureMember(controller, pool2, subprocFactory);
      members.add(new Pair<ProcedureMember, ZKProcedureMemberRpcs>(mem, controller));
      controller.start(member, mem);
    }

    // setup mock subprocedures
    final List<Subprocedure> cohortTasks = new ArrayList<Subprocedure>();
    final int[] elem = new int[1];
    for (int i = 0; i < members.size(); i++) {
      ForeignExceptionDispatcher cohortMonitor = new ForeignExceptionDispatcher();
      final ProcedureMember comms = members.get(i).getFirst();
      Subprocedure commit = Mockito
      .spy(new SubprocedureImpl(comms, opName, cohortMonitor, WAKE_FREQUENCY, TIMEOUT));
      // This nasty bit has one of the impls throw a TimeoutException
      Mockito.doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          int index = elem[0];
          if (index == memberErrorIndex) {
            LOG.debug("Sending error to coordinator");
            ForeignException remoteCause = new ForeignException("TIMER",
                new TimeoutException("subprocTimeout" , 1, 2, 0));
            Subprocedure r = ((Subprocedure) invocation.getMock());
            LOG.error("Remote commit failure, not propagating error:" + remoteCause);
            comms.receiveAbortProcedure(r.getName(), remoteCause);
            assertEquals(r.isComplete(), true);
            // don't complete the error phase until the coordinator has gotten the error
            // notification (which ensures that we never progress past prepare)
            try {
              Procedure.waitForLatch(coordinatorReceivedErrorLatch, new ForeignExceptionDispatcher(),
                  WAKE_FREQUENCY, "coordinator received error");
            } catch (InterruptedException e) {
              LOG.debug("Wait for latch interrupted, done:" + (coordinatorReceivedErrorLatch.getCount() == 0));
              // reset the interrupt status on the thread
              Thread.currentThread().interrupt();
            }
          }
          elem[0] = ++index;
          return null;
        }
      }).when(commit).acquireBarrier();
      cohortTasks.add(commit);
    }

    // pass out a task per member
    final AtomicInteger taskIndex = new AtomicInteger();
    Mockito.when(
      subprocFactory.buildSubprocedure(Mockito.eq(opName),
        (byte[]) Mockito.argThat(new ArrayEquals(data)))).thenAnswer(
      new Answer<Subprocedure>() {
        @Override
        public Subprocedure answer(InvocationOnMock invocation) throws Throwable {
          int index = taskIndex.getAndIncrement();
          Subprocedure commit = cohortTasks.get(index);
          return commit;
        }
      });

    // setup spying on the coordinator
    ForeignExceptionDispatcher coordinatorTaskErrorMonitor = Mockito
        .spy(new ForeignExceptionDispatcher());
    Procedure coordinatorTask = Mockito.spy(new Procedure(coordinator,
        coordinatorTaskErrorMonitor, WAKE_FREQUENCY, TIMEOUT,
        opName, data, expected));
    when(coordinator.createProcedure(any(ForeignExceptionDispatcher.class), eq(opName), eq(data), anyListOf(String.class)))
      .thenReturn(coordinatorTask);
    // count down the error latch when we get the remote error
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        // pass on the error to the master
        invocation.callRealMethod();
        // then count down the got error latch
        coordinatorReceivedErrorLatch.countDown();
        return null;
      }
    }).when(coordinatorTask).receive(Mockito.any(ForeignException.class));

    // ----------------------------
    // start running the operation
    // ----------------------------

    Procedure task = coordinator.startProcedure(coordinatorTaskErrorMonitor, opName, data, expected);
    assertEquals("Didn't mock coordinator task", coordinatorTask, task);

    // wait for the task to complete
    try {
      task.waitForCompleted();
    } catch (ForeignException fe) {
      // this may get caught or may not
    }

    // -------------
    // verification
    // -------------

    // always expect prepared, never committed, and possible to have cleanup and finish (racy since
    // error case)
    waitAndVerifyProc(coordinatorTask, once, never(), once, atMost(1), true);
    verifyCohortSuccessful(expected, subprocFactory, cohortTasks, once, never(), once,
      once, true);

    // close all the open things
    closeAll(coordinator, coordinatorController, members);
  }

  /**
   * Wait for the coordinator task to complete, and verify all the mocks
   * @param task to wait on
   * @throws Exception on unexpected failure
   */
  private void waitAndVerifyProc(Procedure proc, VerificationMode prepare,
      VerificationMode commit, VerificationMode cleanup, VerificationMode finish, boolean opHasError)
      throws Exception {
    boolean caughtError = false;
    try {
      proc.waitForCompleted();
    } catch (ForeignException fe) {
      caughtError = true;
    }
    // make sure that the task called all the expected phases
    Mockito.verify(proc, prepare).sendGlobalBarrierStart();
    Mockito.verify(proc, commit).sendGlobalBarrierReached();
    Mockito.verify(proc, finish).sendGlobalBarrierComplete();
    assertEquals("Operation error state was unexpected", opHasError, proc.getErrorMonitor()
        .hasException());
    assertEquals("Operation error state was unexpected", opHasError, caughtError);

  }

  /**
   * Wait for the coordinator task to complete, and verify all the mocks
   * @param task to wait on
   * @throws Exception on unexpected failure
   */
  private void waitAndVerifySubproc(Subprocedure op, VerificationMode prepare,
      VerificationMode commit, VerificationMode cleanup, VerificationMode finish, boolean opHasError)
      throws Exception {
    boolean caughtError = false;
    try {
      op.waitForLocallyCompleted();
    } catch (ForeignException fe) {
      caughtError = true;
    }
    // make sure that the task called all the expected phases
    Mockito.verify(op, prepare).acquireBarrier();
    Mockito.verify(op, commit).insideBarrier();
    // We cannot guarantee that cleanup has run so we don't check it.

    assertEquals("Operation error state was unexpected", opHasError, op.getErrorCheckable()
        .hasException());
    assertEquals("Operation error state was unexpected", opHasError, caughtError);

  }

  private void verifyCohortSuccessful(List<String> cohortNames,
      SubprocedureFactory subprocFactory, Iterable<Subprocedure> cohortTasks,
      VerificationMode prepare, VerificationMode commit, VerificationMode cleanup,
      VerificationMode finish, boolean opHasError) throws Exception {

    // make sure we build the correct number of cohort members
    Mockito.verify(subprocFactory, Mockito.times(cohortNames.size())).buildSubprocedure(
      Mockito.eq(opName), (byte[]) Mockito.argThat(new ArrayEquals(data)));
    // verify that we ran each of the operations cleanly
    int j = 0;
    for (Subprocedure op : cohortTasks) {
      LOG.debug("Checking mock:" + (j++));
      waitAndVerifySubproc(op, prepare, commit, cleanup, finish, opHasError);
    }
  }

  private void closeAll(
      ProcedureCoordinator coordinator,
      ZKProcedureCoordinatorRpcs coordinatorController,
      List<Pair<ProcedureMember, ZKProcedureMemberRpcs>> cohort)
      throws IOException {
    // make sure we close all the resources
    for (Pair<ProcedureMember, ZKProcedureMemberRpcs> member : cohort) {
      member.getFirst().close();
      member.getSecond().close();
    }
    coordinator.close();
    coordinatorController.close();
  }
}
