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

import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestTimeRangeTracker {
  @Test
  public void testAlwaysDecrementingSetsMaximum() {
    TimeRangeTracker trr = new TimeRangeTracker();
    trr.includeTimestamp(3);
    trr.includeTimestamp(2);
    trr.includeTimestamp(1);
    assertTrue(trr.getMinimumTimestamp() != TimeRangeTracker.INITIAL_MINIMUM_TIMESTAMP);
    assertTrue(trr.getMaximumTimestamp() != -1 /*The initial max value*/);
  }

  @Test
  public void testSimpleInRange() {
    TimeRangeTracker trr = new TimeRangeTracker();
    trr.includeTimestamp(0);
    trr.includeTimestamp(2);
    assertTrue(trr.includesTimeRange(new TimeRange(1)));
  }

  /**
   * Run a bunch of threads against a single TimeRangeTracker and ensure we arrive
   * at right range.  Here we do ten threads each incrementing over 100k at an offset
   * of the thread index; max is 10 * 10k and min is 0.
   * @throws InterruptedException
   */
  @Test
  public void testArriveAtRightAnswer() throws InterruptedException {
    final TimeRangeTracker trr = new TimeRangeTracker();
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
            for (int i = (offset * calls); i < calls; i++) trr.includeTimestamp(i);
          } else {
            int base = offset * calls;
            for (int i = base + calls; i >= base; i--) trr.includeTimestamp(i);
          }
        }
      };
      t.start();
      threads[i] = t;
    }
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
    assertTrue(trr.getMaximumTimestamp() == calls * threadCount);
    assertTrue(trr.getMinimumTimestamp() == 0);
  }

  /**
   * Bit of code to test concurrent access on this class.
   * @param args
   * @throws InterruptedException
   */
  public static void main(String[] args) throws InterruptedException {
    long start = System.currentTimeMillis();
    final TimeRangeTracker trr = new TimeRangeTracker();
    final int threadCount = 5;
    final int calls = 1024 * 1024 * 128;
    Thread [] threads = new Thread[threadCount];
    for (int i = 0; i < threads.length; i++) {
      Thread t = new Thread("" + i) {
        @Override
        public void run() {
          for (int i = 0; i < calls; i++) trr.includeTimestamp(i);
        }
      };
      t.start();
      threads[i] = t;
    }
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
    System.out.println(trr.getMinimumTimestamp() + " " + trr.getMaximumTimestamp() + " " +
      (System.currentTimeMillis() - start));
  }
}