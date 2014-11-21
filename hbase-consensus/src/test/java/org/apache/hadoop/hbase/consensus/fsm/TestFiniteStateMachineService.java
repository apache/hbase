package org.apache.hadoop.hbase.consensus.fsm;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.serial.AsyncSerialExecutorServiceImpl;
import org.apache.hadoop.hbase.util.serial.SerialExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFiniteStateMachineService {
  CountDownLatch latch;
  CountDownMachine fsm;
  FiniteStateMachineService service;
  boolean multiplexedFSM;

  public TestFiniteStateMachineService(boolean multiplexedFSM) {
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
    latch = new CountDownLatch(3);
    fsm = new CountDownMachine("CountDownMachine", latch);
    if (multiplexedFSM) {
      SerialExecutorService serialService =
          new AsyncSerialExecutorServiceImpl(HConstants.DEFAULT_FSM_MUX_THREADPOOL_SIZE,
              "serialScheduler");
      service = new ConstitutentFSMService(fsm, serialService.createStream());
    } else {
      service = new FiniteStateMachineServiceImpl(fsm);
    }
  }

  @Test
  public void shouldDrainEventQueueOnShutdown() throws InterruptedException {
    assertFalse("Service should not be shutdown", service.isShutdown());
    for (int i = 0; i < latch.getCount(); i++) {
      assertTrue("Event should be scheduled",
        service.offer(new Event(Events.PUSH)));
    }
    service.shutdown();
    fsm.start();  // Allow the CountDownMachine to handle events.
    assertTrue("Event queue should be drained after shutdown",
      latch.await(100, TimeUnit.MILLISECONDS));
    assertTrue("Service should be terminated",
      service.awaitTermination(100, TimeUnit.MILLISECONDS));
  }

  enum States implements StateType {
    NONE,
    WAIT,
    PUSH,
    MAX
  }

  enum Events implements EventType {
    NONE,
    PUSH,
    MAX
  }

  enum Transitions implements TransitionType {
    NONE,
    UNCONDITIONAL,
    ON_PUSH,
    MAX
  }

  private class CountDownMachine extends FiniteStateMachine {
    CountDownLatch startDelay;

    public CountDownMachine(String name, CountDownLatch latch) {
      super(name);
      this.startDelay = new CountDownLatch(1);

      State WAIT = new CountDownState(States.WAIT, null);
      State PUSH = new CountDownState(States.PUSH, latch);

      addTransition(WAIT, PUSH,
        new Transition(Transitions.ON_PUSH, new OnEvent(Events.PUSH)));
      addTransition(PUSH, WAIT,
        new Transition(Transitions.UNCONDITIONAL, new Unconditional()));
      setStartState(WAIT);
    }

    public void start() {
      startDelay.countDown();
    }

    /**
     * Delay handling events until start has been called. This works around
     * the race condition between adding events to the queue and shutdown
     * of the FSM service.
     * @param e Event to be handled
     */
    @Override
    public void handleEvent(final Event e) {
      try {
        startDelay.await();
        super.handleEvent(e);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }

    private class CountDownState extends State {
      CountDownLatch latch;

      public CountDownState(StateType t, CountDownLatch latch) {
        super(t);
        this.latch = latch;
      }

      @Override
      public void onEntry(Event e) {
        if (latch != null) {
          latch.countDown();
        }
      }

      @Override
      public void onExit(Event e) {}
    }
  }
}
