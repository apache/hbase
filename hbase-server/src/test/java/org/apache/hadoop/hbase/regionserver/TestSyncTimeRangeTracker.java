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

import static org.junit.Assert.assertTrue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestSyncTimeRangeTracker extends TestSimpleTimeRangeTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSyncTimeRangeTracker.class);

  private static final int NUM_KEYS = 10000000;
  private static final int NUM_OF_THREADS = 20;

  @Override
  protected TimeRangeTracker getTimeRangeTracker() {
    return TimeRangeTracker.create(TimeRangeTracker.Type.SYNC);
  }

  @Override
  protected TimeRangeTracker getTimeRangeTracker(long min, long max) {
    return TimeRangeTracker.create(TimeRangeTracker.Type.SYNC, min, max);
  }

  /**
   * Run a bunch of threads against a single TimeRangeTracker and ensure we arrive
   * at right range.  Here we do ten threads each incrementing over 100k at an offset
   * of the thread index; max is 10 * 10k and min is 0.
   */
  @Test
  public void testArriveAtRightAnswer() throws InterruptedException {
    final TimeRangeTracker trr = getTimeRangeTracker();
    final int threadCount = 10;
    final int calls = 1000 * 1000;
    Thread [] threads = new Thread[threadCount];
    for (int i = 0; i < threads.length; i++) {
      Thread t = new Thread("" + i) {
        @Override
        public void run() {
          int offset = Integer.parseInt(getName());
          boolean even = offset % 2 == 0;
          if (even) {
            for (int i = (offset * calls); i < calls; i++) {
              trr.includeTimestamp(i);
            }
          } else {
            int base = offset * calls;
            for (int i = base + calls; i >= base; i--) {
              trr.includeTimestamp(i);
            }
          }
        }
      };
      t.start();
      threads[i] = t;
    }
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }

    assertTrue(trr.getMax() == calls * threadCount);
    assertTrue(trr.getMin() == 0);
  }

  static class RandomTestData {
    private long[] keys = new long[NUM_KEYS];
    private long min = Long.MAX_VALUE;
    private long max = 0;

    public RandomTestData() {
      if (ThreadLocalRandom.current().nextInt(NUM_OF_THREADS) % 2 == 0) {
        for (int i = 0; i < NUM_KEYS; i++) {
          keys[i] = i + ThreadLocalRandom.current().nextLong(NUM_OF_THREADS);
          if (keys[i] < min) {
            min = keys[i];
          }
          if (keys[i] > max) {
            max = keys[i];
          }
        }
      } else {
        for (int i = NUM_KEYS - 1; i >= 0; i--) {
          keys[i] = i + ThreadLocalRandom.current().nextLong(NUM_OF_THREADS);
          if (keys[i] < min) {
            min = keys[i];
          }
          if (keys[i] > max) {
            max = keys[i];
          }
        }
      }
    }

    public long getMax() {
      return this.max;
    }

    public long getMin() {
      return this.min;
    }
  }

  static class TrtUpdateRunnable implements Runnable {

    private TimeRangeTracker trt;
    private RandomTestData data;
    public TrtUpdateRunnable(final TimeRangeTracker trt, final RandomTestData data) {
      this.trt = trt;
      this.data = data;
    }

    @Override
    public void run() {
      for (long key : data.keys) {
        trt.includeTimestamp(key);
      }
    }
  }

  /**
   * Run a bunch of threads against a single TimeRangeTracker and ensure we arrive
   * at right range.  The data chosen is going to ensure that there are lots collisions, i.e,
   * some other threads may already update the value while one tries to update min/max value.
   */
  @Test
  public void testConcurrentIncludeTimestampCorrectness() {
    RandomTestData[] testData = new RandomTestData[NUM_OF_THREADS];
    long min = Long.MAX_VALUE, max = 0;
    for (int i = 0; i < NUM_OF_THREADS; i ++) {
      testData[i] = new RandomTestData();
      if (testData[i].getMin() < min) {
        min = testData[i].getMin();
      }
      if (testData[i].getMax() > max) {
        max = testData[i].getMax();
      }
    }

    TimeRangeTracker trt = TimeRangeTracker.create(TimeRangeTracker.Type.SYNC);

    Thread[] t = new Thread[NUM_OF_THREADS];
    for (int i = 0; i < NUM_OF_THREADS; i++) {
      t[i] = new Thread(new TrtUpdateRunnable(trt, testData[i]));
      t[i].start();
    }

    for (Thread thread : t) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    assertTrue(min == trt.getMin());
    assertTrue(max == trt.getMax());
  }
}
