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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.MediumTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This tests some race conditions that can happen
 * occasionally, but not every time.
 */
@Category(MediumTests.class)
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
          500 // wait for 500ms
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
          500 // wait for 500ms
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
          500 // wait for 500ms
          );
    }
  }
}
