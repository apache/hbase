package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * This test can fail from time to time and it is ok.
 * It tests some race conditions that can happen
 * occasionally, but not every time.
 */
public class TestSizeBasedThrottler {

  private static final int REPEATS = 100;

  private Thread makeThread(final SizeBasedThrottler throttler,
      final AtomicBoolean failed, final int delta,
      final int limit, final CountDownLatch latch) {

    Thread ret = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          latch.await();
          if (throttler.increase(delta) > limit) {
            failed.set(true);
          }
          throttler.decrease(delta);
        } catch (Exception e) {
          failed.set(true);
        }
      }
    });

    ret.start();
    return ret;

  }

  private void runGenericTest(int threshold, int delta, int maxValueAllowed,
      int numberOfThreads, long timeout) {
    SizeBasedThrottler throttler = new SizeBasedThrottler(threshold);
    AtomicBoolean failed = new AtomicBoolean(false);

    ArrayList<Thread> threads = new ArrayList<Thread>(numberOfThreads);
    CountDownLatch latch = new CountDownLatch(1);
    long timeElapsed = 0;

    for (int i = 0; i < numberOfThreads; ++i) {
      threads.add(makeThread(throttler, failed, delta, maxValueAllowed, latch));
    }

    latch.countDown();
    for (Thread t : threads) {
      try {
        long beforeJoin = System.currentTimeMillis();
        t.join(timeout - timeElapsed);
        timeElapsed += System.currentTimeMillis() - beforeJoin;
        if (t.isAlive() || timeElapsed >= timeout) {
          fail("Timeout reached.");
        }
      } catch (InterruptedException e) {
        fail("Got InterruptedException");
      }
    }

    assertFalse(failed.get());
  }

  @Test
  public void testSmallIncreases(){
    for (int i = 0; i < REPEATS; ++i) {
      runGenericTest(
          10, // threshold
          1,  // delta
          15, // fail if throttler's value
              // exceeds 15
          1000, // use 1000 threads
          200 // wait for 200ms
          );
    }
  }

  @Test
  public void testBigIncreases() {
    for (int i = 0; i < REPEATS; ++i) {
      runGenericTest(
          1, // threshold
          2, // delta
          4, // fail if throttler's value
             // exceeds 4
          1000, // use 1000 threads
          200 // wait for 200ms
          );
    }
  }

  @Test
  public void testIncreasesEqualToThreshold(){
    for (int i = 0; i < REPEATS; ++i) {
      runGenericTest(
          1, // threshold
          1, // delta
          2, // fail if throttler's value
             // exceeds 2
          1000, // use 1000 threads
          200 // wait for 200ms
          );
    }
  }

}
