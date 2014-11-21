package org.apache.hadoop.hbase.consensus.fsm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import junit.framework.Assert;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.serial.AsyncSerialExecutorServiceImpl;
import org.apache.hadoop.hbase.util.serial.SerialExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIncompleteStates {
  enum States implements StateType {
    S1,
    S2,
    S3,
    S4,
    S5,
    S6
  }

  enum Events implements EventType {
    E1,
    E2,
    E3
  }

  enum Transitions implements TransitionType {
    T1,
    T2,
    T3,
    T4,
    T5
  }

  private class TestableState extends State {
    boolean entered = false;

    public TestableState(StateType stateType) {
      super(stateType);
    }

    @Override
    public void onEntry(Event e) {
      entered = true;
    }

    @Override
    public void onExit(Event e) {
      countDownLatch.countDown();
    }

    public boolean isEntered() {
      return entered;
    }
  }

  /**
   * This type of state mocks the behavior of a state which doesn't complete
   * its onEntry() method's logic with the return of the onEntry() method call.
   */
  private class TestableStateWithAsyncWork extends TestableState {
    boolean isComplete = false;
    SettableFuture<?> pendingFuture;

    public TestableStateWithAsyncWork(StateType stateType) {
      super(stateType);
    }

    @Override
    public boolean isAsyncState() {
      return true;
    }

    @Override
    public void onEntry(Event e) {
      entered = true;
      pendingFuture = SettableFuture.create();
    }

    @Override
    public boolean isComplete() {
      return isComplete;
    }

    @Override
    public ListenableFuture<?> getAsyncCompletion() {
      return pendingFuture;
    }

    public void setComplete() {
      if (entered) {
        isComplete = true;
        pendingFuture.set(null);
      }
    }
  }

  // States
  TestableState S1 = new TestableState(States.S1);
  TestableStateWithAsyncWork S2 = new TestableStateWithAsyncWork(States.S2);
  TestableState S3 = new TestableState(States.S3);
  TestableState S4 = new TestableState(States.S4);
  TestableState S5 = new TestableState(States.S5);
  TestableStateWithAsyncWork S6 = new TestableStateWithAsyncWork(States.S6);

  // Transitions
  Transition T1 = new Transition(Transitions.T1, new OnEvent(Events.E1));
  Transition T2 = new Transition(Transitions.T2, new OnEvent(Events.E2));
  Transition T3 = new Transition(Transitions.T3, new Unconditional());
  Transition T4 = new Transition(Transitions.T4, new Unconditional());
  Transition T5 = new Transition(Transitions.T5, new OnEvent(Events.E3));

  // Events
  Event E1 = new Event(Events.E1);
  Event E2 = new Event(Events.E2);
  Event E3 = new Event(Events.E3);

  CountDownLatch countDownLatch;
  FiniteStateMachine fsm;
  FiniteStateMachineService fsmService;
  boolean multiplexedFSM;

  public TestIncompleteStates(boolean multiplexedFSM) {
    this.multiplexedFSM = multiplexedFSM;
  }

  @SuppressWarnings("serial")
  @Parameterized.Parameters
  public static Collection<Boolean[]> parameters() {
    return new ArrayList<Boolean[]>() {{
      add(new Boolean[]{ true });
      add(new Boolean[]{ false });
    }};
  }

  @Before
  public void setUp() {
    fsm = new FiniteStateMachine("fsm");
    fsm.setStartState(S1);
    if (multiplexedFSM) {
      SerialExecutorService serialService =
          new AsyncSerialExecutorServiceImpl(HConstants.DEFAULT_FSM_MUX_THREADPOOL_SIZE,
              "serialScheduler");
      fsmService = new ConstitutentFSMService(fsm, serialService.createStream());
    } else {
      fsmService = new FiniteStateMachineServiceImpl(fsm);
    }
  }

  public void countDown() {
    try {
      countDownLatch.await(500, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    Assert.assertEquals(0, countDownLatch.getCount());
  }

  public void resetLatch(int expectedCount) {
    countDownLatch = new CountDownLatch(expectedCount);
  }

  @Test
  public void testIncompleteStates() throws InterruptedException {
    fsm.addTransition(S1, S2, T1);
    fsm.addTransition(S2, S3, T2);

    // Offer the event E1.
    resetLatch(1);
    fsmService.offer(E1);
    countDown();

    // Check that the state S1 is complete, since it is a regular state.
    Assert.assertEquals(true, S1.isComplete());

    // Check that we land up in S2.
    Assert.assertEquals(S2, fsmService.getCurrentState());
    Assert.assertEquals(0, fsmService.getNumPendingEvents());

    // Issue an event to transition to S3. Note that S2 mimics a state which
    // has pending async work.
    resetLatch(0);
    fsmService.offer(E2);
    countDown();

    Assert.assertEquals(1, fsmService.getNumPendingEvents());
    Assert.assertEquals(S2, fsmService.getCurrentState());

    // Now set the state to be completed
    resetLatch(1);
    S2.setComplete();
    Assert.assertEquals(true, S2.isComplete());
    countDown();

    Assert.assertEquals(0, fsmService.getNumPendingEvents());
    Assert.assertEquals(S3, fsmService.getCurrentState());
  }

  /**
   * This tests the case when you have a scenario like (S2 is an async state):
   * S2--(Conditional)-->S3--(Conditional)-->S4--(Event E)-->S5.
   *
   * If we are in S2, and the async task is not complete, we should not
   * transition to S3, and then to S4. Only when the task is complete, should we
   * automatically transition to S3, and then to S4.
   *
   * Upon offering the event E, we should transition to S3, and then to S5.
   * E should not be discarded.
   */
  @Test
  public void testStateTransitionFromAsyncStatesWithConditions() {
    fsm.addTransition(S1, S2, T1);
    fsm.addTransition(S2, S3, T3);
    fsm.addTransition(S3, S4, T4);
    fsm.addTransition(S4, S5, T5);

    // Offer the event E1.
    resetLatch(1);
    fsmService.offer(E1);
    countDown();

    // Check that the state S1 is complete, since it is a regular state.
    Assert.assertEquals(true, S1.isComplete());

    // Check that the current state is S2, and NOT S3, even though there is an
    // unconditional transition from S2 to S3.
    Assert.assertEquals(S2, fsm.getCurrentState());

    // Now set the state S2 to be completed.
    resetLatch(2);
    S2.setComplete();
    countDown();

    // We should now be in S4, by taking a conditional transition to S3, and
    // then to S4.
    Assert.assertEquals(S4, fsmService.getCurrentState());

    // Also check that we visited S3.
    Assert.assertTrue(S3.isEntered());


    // Now offer E3, we should transition unconditionally from S2 to S3, and
    // unconditionally from S3 to S4. Then because E3 was offered, we should
    // transition to S5.
    resetLatch(1);
    fsmService.offer(E3);
    countDown();

    Assert.assertEquals(S5, fsm.getCurrentState());
  }

  /**
   * We should not abort an event when we are in an async state.
   */
  @Test
  public void testEventNotAbortedWhenInAsyncState() {
    // S2 and S6 are async states, which transition on E2 and E3 respectively.
    fsm.addTransition(S1, S2, T1);
    fsm.addTransition(S2, S6, T2);
    fsm.addTransition(S6, S3, T5);

    resetLatch(1);
    fsmService.offer(E1);
    countDown();

    // We offer E3 before S2 is complete, so that it is in the event queue.
    // After E2 we transition to S6, but E3 is not applicable yet because the
    // state is not complete. We should not discard E3.
    resetLatch(1);
    fsmService.offer(E2);
    fsmService.offer(E3);
    S2.setComplete();
    countDown();

    resetLatch(1);
    S6.setComplete();
    countDown();

    Assert.assertEquals(S3, fsm.getCurrentState());
  }
}
