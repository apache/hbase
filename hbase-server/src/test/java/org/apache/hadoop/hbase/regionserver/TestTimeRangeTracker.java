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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestTimeRangeTracker {

  @Test
  public void testAlwaysDecrementingSetsMaximum() {
    TimeRangeTracker trr = new TimeRangeTracker();
    trr.includeTimestamp(3);
    trr.includeTimestamp(2);
    trr.includeTimestamp(1);
    assertTrue(trr.getMin() != TimeRangeTracker.INITIAL_MIN_TIMESTAMP);
    assertTrue(trr.getMax() != -1 /*The initial max value*/);
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
    assertTrue(trr.getMax() == calls * threadCount);
    assertTrue(trr.getMin() == 0);
  }

  @Test
  public void testRangeConstruction() throws IOException {
    TimeRange defaultRange = new TimeRange();
    assertEquals(0L, defaultRange.getMin());
    assertEquals(Long.MAX_VALUE, defaultRange.getMax());
    assertTrue(defaultRange.isAllTime());

    TimeRange oneArgRange = new TimeRange(0L);
    assertEquals(0L, oneArgRange.getMin());
    assertEquals(Long.MAX_VALUE, oneArgRange.getMax());
    assertTrue(oneArgRange.isAllTime());

    TimeRange oneArgRange2 = new TimeRange(1);
    assertEquals(1, oneArgRange2.getMin());
    assertEquals(Long.MAX_VALUE, oneArgRange2.getMax());
    assertFalse(oneArgRange2.isAllTime());

    TimeRange twoArgRange = new TimeRange(0L, Long.MAX_VALUE);
    assertEquals(0L, twoArgRange.getMin());
    assertEquals(Long.MAX_VALUE, twoArgRange.getMax());
    assertTrue(twoArgRange.isAllTime());

    TimeRange twoArgRange2 = new TimeRange(0L, Long.MAX_VALUE - 1);
    assertEquals(0L, twoArgRange2.getMin());
    assertEquals(Long.MAX_VALUE - 1, twoArgRange2.getMax());
    assertFalse(twoArgRange2.isAllTime());

    TimeRange twoArgRange3 = new TimeRange(1, Long.MAX_VALUE);
    assertEquals(1, twoArgRange3.getMin());
    assertEquals(Long.MAX_VALUE, twoArgRange3.getMax());
    assertFalse(twoArgRange3.isAllTime());
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
    System.out.println(trr.getMin() + " " + trr.getMax() + " " +
      (System.currentTimeMillis() - start));
  }
}
