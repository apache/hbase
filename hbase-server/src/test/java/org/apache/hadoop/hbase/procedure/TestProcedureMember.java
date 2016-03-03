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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.TimeoutException;
import org.apache.hadoop.hbase.procedure.Subprocedure.SubprocedureImpl;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test the procedure member, and it's error handling mechanisms.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestProcedureMember {
  private static final long WAKE_FREQUENCY = 100;
  private static final long TIMEOUT = 100000;
  private static final long POOL_KEEP_ALIVE = 1;

  private final String op = "some op";
  private final byte[] data = new byte[0];
  private final ForeignExceptionDispatcher mockListener = Mockito
      .spy(new ForeignExceptionDispatcher());
  private final SubprocedureFactory mockBuilder = mock(SubprocedureFactory.class);
  private final ProcedureMemberRpcs mockMemberComms = Mockito
      .mock(ProcedureMemberRpcs.class);
  private ProcedureMember member;
  private ForeignExceptionDispatcher dispatcher;
  Subprocedure spySub;

  /**
   * Reset all the mock objects
   */
  @After
  public void resetTest() {
    reset(mockListener, mockBuilder, mockMemberComms);
    if (member != null)
      try {
        member.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  /**
   * Build a member using the class level mocks
   * @return member to use for tests
   */
  private ProcedureMember buildCohortMember() {
    String name = "node";
    ThreadPoolExecutor pool = ProcedureMember.defaultPool(name, 1, POOL_KEEP_ALIVE);
    return new ProcedureMember(mockMemberComms, pool, mockBuilder);
  }

  /**
   * Setup a procedure member that returns the spied-upon {@link Subprocedure}.
   */
  private void buildCohortMemberPair() throws IOException {
    dispatcher = new ForeignExceptionDispatcher();
    String name = "node";
    ThreadPoolExecutor pool = ProcedureMember.defaultPool(name, 1, POOL_KEEP_ALIVE);
    member = new ProcedureMember(mockMemberComms, pool, mockBuilder);
    when(mockMemberComms.getMemberName()).thenReturn("membername"); // needed for generating exception
    Subprocedure subproc = new EmptySubprocedure(member, dispatcher);
    spySub = spy(subproc);
    when(mockBuilder.buildSubprocedure(op, data)).thenReturn(spySub);
    addCommitAnswer();
  }


  /**
   * Add a 'in barrier phase' response to the mock controller when it gets a acquired notification
   */
  private void addCommitAnswer() throws IOException {
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        member.receivedReachedGlobalBarrier(op);
        return null;
      }
    }).when(mockMemberComms).sendMemberAcquired(any(Subprocedure.class));
  }

  /**
   * Test the normal sub procedure execution case.
   */
  @Test(timeout = 500)
  public void testSimpleRun() throws Exception {
    member = buildCohortMember();
    EmptySubprocedure subproc = new EmptySubprocedure(member, mockListener);
    EmptySubprocedure spy = spy(subproc);
    when(mockBuilder.buildSubprocedure(op, data)).thenReturn(spy);

    // when we get a prepare, then start the commit phase
    addCommitAnswer();

    // run the operation
    // build a new operation
    Subprocedure subproc1 = member.createSubprocedure(op, data);
    member.submitSubprocedure(subproc1);
    // and wait for it to finish
    subproc.waitForLocallyCompleted();

    // make sure everything ran in order
    InOrder order = inOrder(mockMemberComms, spy);
    order.verify(spy).acquireBarrier();
    order.verify(mockMemberComms).sendMemberAcquired(eq(spy));
    order.verify(spy).insideBarrier();
    order.verify(mockMemberComms).sendMemberCompleted(eq(spy), eq(data));
    order.verify(mockMemberComms, never()).sendMemberAborted(eq(spy),
        any(ForeignException.class));
  }

  /**
   * Make sure we call cleanup etc, when we have an exception during
   * {@link Subprocedure#acquireBarrier()}.
   */
  @Test(timeout = 60000)
  public void testMemberPrepareException() throws Exception {
    buildCohortMemberPair();

    // mock an exception on Subprocedure's prepare
    doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            throw new IOException("Forced IOException in member acquireBarrier");
          }
        }).when(spySub).acquireBarrier();

    // run the operation
    // build a new operation
    Subprocedure subproc = member.createSubprocedure(op, data);
    member.submitSubprocedure(subproc);
    // if the operation doesn't die properly, then this will timeout
    member.closeAndWait(TIMEOUT);

    // make sure everything ran in order
    InOrder order = inOrder(mockMemberComms, spySub);
    order.verify(spySub).acquireBarrier();
    // Later phases not run
    order.verify(mockMemberComms, never()).sendMemberAcquired(eq(spySub));
    order.verify(spySub, never()).insideBarrier();
    order.verify(mockMemberComms, never()).sendMemberCompleted(eq(spySub), eq(data));
    // error recovery path exercised
    order.verify(spySub).cancel(anyString(), any(Exception.class));
    order.verify(spySub).cleanup(any(Exception.class));
  }

  /**
   * Make sure we call cleanup etc, when we have an exception during prepare.
   */
  @Test(timeout = 60000)
  public void testSendMemberAcquiredCommsFailure() throws Exception {
    buildCohortMemberPair();

    // mock an exception on Subprocedure's prepare
    doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            throw new IOException("Forced IOException in memeber prepare");
          }
        }).when(mockMemberComms).sendMemberAcquired(any(Subprocedure.class));

    // run the operation
    // build a new operation
    Subprocedure subproc = member.createSubprocedure(op, data);
    member.submitSubprocedure(subproc);
    // if the operation doesn't die properly, then this will timeout
    member.closeAndWait(TIMEOUT);

    // make sure everything ran in order
    InOrder order = inOrder(mockMemberComms, spySub);
    order.verify(spySub).acquireBarrier();
    order.verify(mockMemberComms).sendMemberAcquired(eq(spySub));

    // Later phases not run
    order.verify(spySub, never()).insideBarrier();
    order.verify(mockMemberComms, never()).sendMemberCompleted(eq(spySub), eq(data));
    // error recovery path exercised
    order.verify(spySub).cancel(anyString(), any(Exception.class));
    order.verify(spySub).cleanup(any(Exception.class));
  }

  /**
   * Fail correctly if coordinator aborts the procedure.  The subprocedure will not interrupt a
   * running {@link Subprocedure#prepare} -- prepare needs to finish first, and the the abort
   * is checked.  Thus, the {@link Subprocedure#prepare} should succeed but later get rolled back
   * via {@link Subprocedure#cleanup}.
   */
  @Test(timeout = 60000)
  public void testCoordinatorAbort() throws Exception {
    buildCohortMemberPair();

    // mock that another node timed out or failed to prepare
    final TimeoutException oate = new TimeoutException("bogus timeout", 1,2,0);
    doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            // inject a remote error (this would have come from an external thread)
            spySub.cancel("bogus message", oate);
            // sleep the wake frequency since that is what we promised
            Thread.sleep(WAKE_FREQUENCY);
            return null;
          }
        }).when(spySub).waitForReachedGlobalBarrier();

    // run the operation
    // build a new operation
    Subprocedure subproc = member.createSubprocedure(op, data);
    member.submitSubprocedure(subproc);
    // if the operation doesn't die properly, then this will timeout
    member.closeAndWait(TIMEOUT);

    // make sure everything ran in order
    InOrder order = inOrder(mockMemberComms, spySub);
    order.verify(spySub).acquireBarrier();
    order.verify(mockMemberComms).sendMemberAcquired(eq(spySub));
    // Later phases not run
    order.verify(spySub, never()).insideBarrier();
    order.verify(mockMemberComms, never()).sendMemberCompleted(eq(spySub), eq(data));
    // error recovery path exercised
    order.verify(spySub).cancel(anyString(), any(Exception.class));
    order.verify(spySub).cleanup(any(Exception.class));
  }

  /**
   * Handle failures if a member's commit phase fails.
   *
   * NOTE: This is the core difference that makes this different from traditional 2PC.  In true
   * 2PC the transaction is committed just before the coordinator sends commit messages to the
   * member.  Members are then responsible for reading its TX log.  This implementation actually
   * rolls back, and thus breaks the normal TX guarantees.
  */
  @Test(timeout = 60000)
  public void testMemberCommitException() throws Exception {
    buildCohortMemberPair();

    // mock an exception on Subprocedure's prepare
    doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            throw new IOException("Forced IOException in memeber prepare");
          }
        }).when(spySub).insideBarrier();

    // run the operation
    // build a new operation
    Subprocedure subproc = member.createSubprocedure(op, data);
    member.submitSubprocedure(subproc);
    // if the operation doesn't die properly, then this will timeout
    member.closeAndWait(TIMEOUT);

    // make sure everything ran in order
    InOrder order = inOrder(mockMemberComms, spySub);
    order.verify(spySub).acquireBarrier();
    order.verify(mockMemberComms).sendMemberAcquired(eq(spySub));
    order.verify(spySub).insideBarrier();

    // Later phases not run
    order.verify(mockMemberComms, never()).sendMemberCompleted(eq(spySub), eq(data));
    // error recovery path exercised
    order.verify(spySub).cancel(anyString(), any(Exception.class));
    order.verify(spySub).cleanup(any(Exception.class));
  }

  /**
   * Handle Failures if a member's commit phase succeeds but notification to coordinator fails
   *
   * NOTE: This is the core difference that makes this different from traditional 2PC.  In true
   * 2PC the transaction is committed just before the coordinator sends commit messages to the
   * member.  Members are then responsible for reading its TX log.  This implementation actually
   * rolls back, and thus breaks the normal TX guarantees.
  */
  @Test(timeout = 60000)
  public void testMemberCommitCommsFailure() throws Exception {
    buildCohortMemberPair();
    final TimeoutException oate = new TimeoutException("bogus timeout",1,2,0);
    doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            // inject a remote error (this would have come from an external thread)
            spySub.cancel("commit comms fail", oate);
            // sleep the wake frequency since that is what we promised
            Thread.sleep(WAKE_FREQUENCY);
            return null;
          }
        }).when(mockMemberComms).sendMemberCompleted(any(Subprocedure.class), eq(data));

    // run the operation
    // build a new operation
    Subprocedure subproc = member.createSubprocedure(op, data);
    member.submitSubprocedure(subproc);
    // if the operation doesn't die properly, then this will timeout
    member.closeAndWait(TIMEOUT);

    // make sure everything ran in order
    InOrder order = inOrder(mockMemberComms, spySub);
    order.verify(spySub).acquireBarrier();
    order.verify(mockMemberComms).sendMemberAcquired(eq(spySub));
    order.verify(spySub).insideBarrier();
    order.verify(mockMemberComms).sendMemberCompleted(eq(spySub), eq(data));
    // error recovery path exercised
    order.verify(spySub).cancel(anyString(), any(Exception.class));
    order.verify(spySub).cleanup(any(Exception.class));
  }

  /**
   * Fail correctly on getting an external error while waiting for the prepared latch
   * @throws Exception on failure
   */
  @Test(timeout = 60000)
  public void testPropagateConnectionErrorBackToManager() throws Exception {
    // setup the operation
    member = buildCohortMember();
    ProcedureMember memberSpy = spy(member);

    // setup the commit and the spy
    final ForeignExceptionDispatcher dispatcher = new ForeignExceptionDispatcher();
    ForeignExceptionDispatcher dispSpy = spy(dispatcher);
    Subprocedure commit = new EmptySubprocedure(member, dispatcher);
    Subprocedure spy = spy(commit);
    when(mockBuilder.buildSubprocedure(op, data)).thenReturn(spy);

    // fail during the prepare phase
    doThrow(new ForeignException("SRC", "prepare exception")).when(spy).acquireBarrier();
    // and throw a connection error when we try to tell the controller about it
    doThrow(new IOException("Controller is down!")).when(mockMemberComms)
        .sendMemberAborted(eq(spy), any(ForeignException.class));


    // run the operation
    // build a new operation
    Subprocedure subproc = memberSpy.createSubprocedure(op, data);
    memberSpy.submitSubprocedure(subproc);
    // if the operation doesn't die properly, then this will timeout
    memberSpy.closeAndWait(TIMEOUT);

    // make sure everything ran in order
    InOrder order = inOrder(mockMemberComms, spy, dispSpy);
    // make sure we acquire.
    order.verify(spy).acquireBarrier();
    order.verify(mockMemberComms, never()).sendMemberAcquired(spy);

    // TODO Need to do another refactor to get this to propagate to the coordinator.
    // make sure we pass a remote exception back the controller
//    order.verify(mockMemberComms).sendMemberAborted(eq(spy),
//      any(ExternalException.class));
//    order.verify(dispSpy).receiveError(anyString(),
//        any(ExternalException.class), any());
  }

  /**
   * Test that the cohort member correctly doesn't attempt to start a task when the builder cannot
   * correctly build a new task for the requested operation
   * @throws Exception on failure
   */
  @Test
  public void testNoTaskToBeRunFromRequest() throws Exception {
    ThreadPoolExecutor pool = mock(ThreadPoolExecutor.class);
    when(mockBuilder.buildSubprocedure(op, data)).thenReturn(null)
      .thenThrow(new IllegalStateException("Wrong state!"), new IllegalArgumentException("can't understand the args"));
    member = new ProcedureMember(mockMemberComms, pool, mockBuilder);
    // builder returns null
    // build a new operation
    Subprocedure subproc = member.createSubprocedure(op, data);
    member.submitSubprocedure(subproc);
    // throws an illegal state exception
    try {
      // build a new operation
      Subprocedure subproc2 = member.createSubprocedure(op, data);
      member.submitSubprocedure(subproc2);
    } catch (IllegalStateException ise) {
    }
    // throws an illegal argument exception
    try {
      // build a new operation
      Subprocedure subproc3 = member.createSubprocedure(op, data);
      member.submitSubprocedure(subproc3);
    } catch (IllegalArgumentException iae) {
    }

    // no request should reach the pool
    verifyZeroInteractions(pool);
    // get two abort requests
    // TODO Need to do another refactor to get this to propagate to the coordinator.
    // verify(mockMemberComms, times(2)).sendMemberAborted(any(Subprocedure.class), any(ExternalException.class));
  }

  /**
   * Helper {@link Procedure} who's phase for each step is just empty
   */
  public class EmptySubprocedure extends SubprocedureImpl {
    public EmptySubprocedure(ProcedureMember member, ForeignExceptionDispatcher dispatcher) {
      super( member, op, dispatcher,
      // TODO 1000000 is an arbitrary number that I picked.
          WAKE_FREQUENCY, TIMEOUT);
    }
  }
}
