package org.apache.hadoop.hbase.consensus.quorum;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This test checks if we can run multiple timers at the same time.
 */
public class TestAggregateTimer {
  public static final Log LOG = LogFactory.getLog(TestAggregateTimer.class);

  class TimeoutHandlerWithCounter implements TimeoutEventHandler {
    public AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void onTimeout() {
      counter.incrementAndGet();
    }
  }

  AggregateTimer aggregateTimer;
  TimeoutHandlerWithCounter c1, c2;
  ConstituentTimer timer1, timer2;
  long timeout1 = 100, timeout2 = 200;


  @Before
  public void setUp() {
    aggregateTimer = new AggregateTimer();
    c1 = new TimeoutHandlerWithCounter();
    c2 = new TimeoutHandlerWithCounter();

    // Default values.
    // timer1 is supposed to be faster than timer2.
    timeout1 = 100;
    timeout2 = 200;

    // Create the timers
    timer1 = aggregateTimer.createTimer(
      "timer1",
      timeout1,
      TimeUnit.MILLISECONDS,
      c1
    );

    timer2 = aggregateTimer.createTimer(
      "timer2",
      timeout2,
      TimeUnit.MILLISECONDS,
      c2
    );
  }

  @Test
  public void testNothingFiresWhenTimersAreNotStarted() {
    LOG.debug("Checking for timeouts not firing when the timers weren't started");
    // Sleep for 2 * (timeout1 + timeout2). None of them should have fired
    try {
      Thread.sleep(2 * (timeout1 + timeout2));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    Assert.assertEquals("Timeout for Timer 1 should not have fired",
      0, c1.counter.get());
    Assert.assertEquals("Timeout for Timer 2 should not have fired",
      0, c2.counter.get());
  }

  @Test
  public void testOnlyOneTimerFiresIfOnlyOneWasStarted() {
    // Starting the first timer thread
    timer1.start();

    LOG.debug("Checking for timeouts not firing when only one of the timers were started");

    // Sleep for 2 * (timeout1 + timeout2).
    // Only the callback for timer1 should have fired, since only timer1 was
    // started.
    try {
      Thread.sleep(2 * (timeout1 + timeout2));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Checking that the timeout fires for timer 1, and resetting the counter
    Assert.assertTrue("Timeout for Timer 1 should have fired",
      c1.counter.getAndSet(0) > 0);
    Assert.assertEquals("Timeout for Timer 2 should not have fired",
      0, c2.counter.get());

  }

  @Test
  public void testTimersFireSufficiently() {
    // Start both the timer threads
    timer1.start();
    timer2.start();

    LOG.debug("Checking for timeouts when both timers were started");

    // Sleep for 3 * (timeout1 + timeout2). Both should have fired at least
    // (total / individual timeout - 1). Subtracting one to avoid edge cases.
    try {
      Thread.sleep(3 * (timeout1 + timeout2));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    long totalSleepTime = (3 * (timeout1 + timeout2));
    long targetTimeoutsForTimer1 = (totalSleepTime / timeout1) - 1;
    long targetTimeoutsForTimer2 = (totalSleepTime / timeout2) - 1;
    Assert.assertTrue("Timeout for Timer 1 did not fire enough",
      c1.counter.getAndSet(0) >= targetTimeoutsForTimer1);
    Assert.assertTrue("Timeout for Timer 2 did not fire enough",
      c2.counter.getAndSet(0) >= targetTimeoutsForTimer2);
  }

  @Test
  public void testResettingATimerWorks() {
    // Start both the timer threads
    timer1.start();
    timer2.start();

    LOG.debug("Checking timeouts when we reset one of the timers");
    for (int i = 0; i < 100; i++) {
      try {
        Thread.sleep(timeout2 / 4);
      } catch (InterruptedException e) {
        LOG.error(e);
        Thread.currentThread().interrupt();
      }
      timer2.reset();
    }
    LOG.debug("Done with the reset of the timers");

    // As expected, timeout for timer 2 should have not fired
    Assert.assertEquals(
      "Timeout for Timer 2 should not be firing because we were resetting it",
      0, c2.counter.getAndSet(0));

    // Timer 1 wasn't touched, so it should still have fired
    Assert.assertTrue(
      "Timeout for Timer 1 should have fired, since we did not reset it",
      c1.counter.getAndSet(0) > 0
    );
  }

  @Test
  public void testStartAndStopOfATimer() {
    testTimersFireSufficiently();

    // Stop timer 1 now
    timer1.stop();
    c1.counter.getAndSet(0);

    // Sleep for 3 * (timeout1 + timeout2).
    // Only callback for timer1 should not have fired.
    try {
      Thread.sleep(3 * (timeout1 + timeout2));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // As expected, timeout for timer 1 should have not fired
    Assert.assertEquals(
      "Timeout for Timer 1 should not be firing because we have stopped it",
      0, c1.counter.getAndSet(0));

    // Timer 2 wasn't touched, so it should still have fired
    Assert.assertTrue(
      "Timeout for Timer 2 should have fired, since we did not stop it",
      c2.counter.getAndSet(0) > 0
    );
  }

  @Test
  public void testStopAndStartWithBackoff() {
    // Making the second timer slower to reproduce this case.
    timeout2 = 500;
    timer2.setDelay(timeout2, TimeUnit.MILLISECONDS);

    timer1.start();
    timer2.start();

    // Assuming right now is t=0,
    // The event for timer2 has been queued to occur at t=timeout2,
    // t=2*timeout2, and so on.

    // Stop timer2.
    // The old event should not be processed for some time, since timer1's
    // event would be queued at t=timeout1, which is before t=timeout2.
    // If we change the backoff for timer2, and start it again before the old
    // event for timer2 is processed, the old event should be discarded.
    LOG.debug("Stopping timer2");
    timer2.stop();

    // Set the backoff to a large value
    timer2.backoff(timeout2 * 100, TimeUnit.MILLISECONDS);

    // Now resume the timer
    timer2.start();

    try {
      Thread.sleep(timeout2 * 10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    Assert.assertEquals(
      "The second timer should not have fired, since the backoff is very large",
      0,
      c2.counter.get()
    );
  }
}
