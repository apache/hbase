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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestBoundedPriorityBlockingQueue {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBoundedPriorityBlockingQueue.class);

  private final static int CAPACITY = 16;

  static class TestObject {
    private final int priority;
    private final int seqId;

    public TestObject(final int priority, final int seqId) {
      this.priority = priority;
      this.seqId = seqId;
    }

    public int getSeqId() {
      return this.seqId;
    }

    public int getPriority() {
      return this.priority;
    }
  }

  static class TestObjectComparator implements Comparator<TestObject> {
    public TestObjectComparator() {}

    @Override
    public int compare(TestObject a, TestObject b) {
      return a.getPriority() - b.getPriority();
    }
  }

  private BoundedPriorityBlockingQueue<TestObject> queue;

  @Before
  public void setUp() throws Exception {
    this.queue = new BoundedPriorityBlockingQueue<>(CAPACITY, new TestObjectComparator());
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void tesAppend() throws Exception {
    // Push
    for (int i = 1; i <= CAPACITY; ++i) {
      assertTrue(queue.offer(new TestObject(i, i)));
      assertEquals(i, queue.size());
      assertEquals(CAPACITY - i, queue.remainingCapacity());
    }
    assertFalse(queue.offer(new TestObject(0, -1), 5, TimeUnit.MILLISECONDS));

    // Pop
    for (int i = 1; i <= CAPACITY; ++i) {
      TestObject obj = queue.poll();
      assertEquals(i, obj.getSeqId());
      assertEquals(CAPACITY - i, queue.size());
      assertEquals(i, queue.remainingCapacity());
    }
    assertEquals(null, queue.poll());
  }

  @Test
  public void tesAppendSamePriority() throws Exception {
    // Push
    for (int i = 1; i <= CAPACITY; ++i) {
      assertTrue(queue.offer(new TestObject(0, i)));
      assertEquals(i, queue.size());
      assertEquals(CAPACITY - i, queue.remainingCapacity());
    }
    assertFalse(queue.offer(new TestObject(0, -1), 5, TimeUnit.MILLISECONDS));

    // Pop
    for (int i = 1; i <= CAPACITY; ++i) {
      TestObject obj = queue.poll();
      assertEquals(i, obj.getSeqId());
      assertEquals(CAPACITY - i, queue.size());
      assertEquals(i, queue.remainingCapacity());
    }
    assertEquals(null, queue.poll());
  }

  @Test
  public void testPrepend() throws Exception {
    // Push
    for (int i = 1; i <= CAPACITY; ++i) {
      assertTrue(queue.offer(new TestObject(CAPACITY - i, i)));
      assertEquals(i, queue.size());
      assertEquals(CAPACITY - i, queue.remainingCapacity());
    }

    // Pop
    for (int i = 1; i <= CAPACITY; ++i) {
      TestObject obj = queue.poll();
      assertEquals(CAPACITY - (i - 1), obj.getSeqId());
      assertEquals(CAPACITY - i, queue.size());
      assertEquals(i, queue.remainingCapacity());
    }
    assertEquals(null, queue.poll());
  }

  @Test
  public void testInsert() throws Exception {
    // Push
    for (int i = 1; i <= CAPACITY; i += 2) {
      assertTrue(queue.offer(new TestObject(i, i)));
      assertEquals((1 + i) / 2, queue.size());
    }
    for (int i = 2; i <= CAPACITY; i += 2) {
      assertTrue(queue.offer(new TestObject(i, i)));
      assertEquals(CAPACITY / 2 + (i / 2), queue.size());
    }
    assertFalse(queue.offer(new TestObject(0, -1), 5, TimeUnit.MILLISECONDS));

    // Pop
    for (int i = 1; i <= CAPACITY; ++i) {
      TestObject obj = queue.poll();
      assertEquals(i, obj.getSeqId());
      assertEquals(CAPACITY - i, queue.size());
      assertEquals(i, queue.remainingCapacity());
    }
    assertEquals(null, queue.poll());
  }

  @Test
  public void testFifoSamePriority() throws Exception {
    assertTrue(CAPACITY >= 6);
    for (int i = 0; i < 6; ++i) {
      assertTrue(queue.offer(new TestObject((1 + (i % 2)) * 10, i)));
    }

    for (int i = 0; i < 6; i += 2) {
      TestObject obj = queue.poll();
      assertEquals(10, obj.getPriority());
      assertEquals(i, obj.getSeqId());
    }

    for (int i = 1; i < 6; i += 2) {
      TestObject obj = queue.poll();
      assertEquals(20, obj.getPriority());
      assertEquals(i, obj.getSeqId());
    }
    assertEquals(null, queue.poll());
  }

  @Test
  public void testPoll() {
    assertNull(queue.poll());
    PriorityQueue<TestObject> testList = new PriorityQueue<>(CAPACITY, new TestObjectComparator());

    for (int i = 0; i < CAPACITY; ++i) {
      TestObject obj = new TestObject(i, i);
      testList.add(obj);
      queue.offer(obj);
    }

    for (int i = 0; i < CAPACITY; ++i) {
      assertEquals(testList.poll(), queue.poll());
    }

    assertNull(null, queue.poll());
  }

  @Test
  public void testPollInExecutor() throws InterruptedException {
    final TestObject testObj = new TestObject(0, 0);

    final CyclicBarrier threadsStarted = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          assertNull(queue.poll(1000, TimeUnit.MILLISECONDS));
          threadsStarted.await();
          assertSame(testObj, queue.poll(1000, TimeUnit.MILLISECONDS));
          assertTrue(queue.isEmpty());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });

    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
            threadsStarted.await();
            queue.offer(testObj);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });

    executor.shutdown();
    assertTrue(executor.awaitTermination(8000, TimeUnit.MILLISECONDS));
  }
}
