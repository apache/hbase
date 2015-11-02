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

package org.apache.hadoop.hbase.procedure2.util;


import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.procedure2.util.TimeoutBlockingQueue.TimeoutRetriever;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(SmallTests.class)
public class TestTimeoutBlockingQueue {
  private static final Log LOG = LogFactory.getLog(TestTimeoutBlockingQueue.class);

  static class TestObject {
    private long timeout;
    private int seqId;

    public TestObject(int seqId, long timeout) {
      this.timeout = timeout;
      this.seqId = seqId;
    }

    public long getTimeout() {
      return timeout;
    }

    public String toString() {
      return String.format("(%03d, %03d)", seqId, timeout);
    }
  }

  static class TestObjectTimeoutRetriever implements TimeoutRetriever<TestObject> {
    @Override
    public long getTimeout(TestObject obj) {
      return obj.getTimeout();
    }

    @Override
    public TimeUnit getTimeUnit(TestObject obj) {
      return TimeUnit.MILLISECONDS;
    }
  }

  @Test
  public void testOrder() {
    TimeoutBlockingQueue<TestObject> queue =
      new TimeoutBlockingQueue<TestObject>(8, new TestObjectTimeoutRetriever());

    long[] timeouts = new long[] {500, 200, 700, 300, 600, 600, 200, 800, 500};

    for (int i = 0; i < timeouts.length; ++i) {
      for (int j = 0; j <= i; ++j) {
        queue.add(new TestObject(j, timeouts[j]));
        queue.dump();
      }

      long prev = 0;
      for (int j = 0; j <= i; ++j) {
        TestObject obj = queue.poll();
        assertTrue(obj.getTimeout() >= prev);
        prev = obj.getTimeout();
        queue.dump();
      }
    }
  }

  @Test
  public void testTimeoutBlockingQueue() {
    TimeoutBlockingQueue<TestObject> queue;

    int[][] testArray = new int[][] {
      {200, 400, 600},  // append
      {200, 400, 100},  // prepend
      {200, 400, 300},  // insert
    };

    for (int i = 0; i < testArray.length; ++i) {
      int[] sortedArray = Arrays.copyOf(testArray[i], testArray[i].length);
      Arrays.sort(sortedArray);

      // test with head == 0
      queue = new TimeoutBlockingQueue<TestObject>(2, new TestObjectTimeoutRetriever());
      for (int j = 0; j < testArray[i].length; ++j) {
        queue.add(new TestObject(j, testArray[i][j]));
        queue.dump();
      }

      for (int j = 0; !queue.isEmpty(); ++j) {
        assertEquals(sortedArray[j], queue.poll().getTimeout());
      }

      queue = new TimeoutBlockingQueue<TestObject>(2, new TestObjectTimeoutRetriever());
      queue.add(new TestObject(0, 50));
      assertEquals(50, queue.poll().getTimeout());

      // test with head > 0
      for (int j = 0; j < testArray[i].length; ++j) {
        queue.add(new TestObject(j, testArray[i][j]));
        queue.dump();
      }

      for (int j = 0; !queue.isEmpty(); ++j) {
        assertEquals(sortedArray[j], queue.poll().getTimeout());
      }
    }
  }
}
