/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestBoundedConcurrentLinkedQueue {
  private final static int CAPACITY = 16;

  private BoundedConcurrentLinkedQueue<Long> queue;

  @Before
  public void setUp() throws Exception {
    this.queue = new BoundedConcurrentLinkedQueue<Long>(CAPACITY);
  }

  @Test
  public void testOfferAndPoll() throws Exception {
    // Offer
    for (long i = 1; i <= CAPACITY; ++i) {
      assertTrue(queue.offer(i));
      assertEquals(i, queue.size());
      assertEquals(CAPACITY - i, queue.remainingCapacity());
    }
    assertFalse(queue.offer(0L));

    // Poll
    for (int i = 1; i <= CAPACITY; ++i) {
      long l = queue.poll();
      assertEquals(i, l);
      assertEquals(CAPACITY - i, queue.size());
      assertEquals(i, queue.remainingCapacity());
    }
    assertEquals(null, queue.poll());
  }

  @Test
  public void testDrain() throws Exception {
    // Offer
    for (long i = 1; i <= CAPACITY; ++i) {
      assertTrue(queue.offer(i));
      assertEquals(i, queue.size());
      assertEquals(CAPACITY - i, queue.remainingCapacity());
    }
    assertFalse(queue.offer(0L));

    // Drain
    List<Long> list = new ArrayList<Long>();
    queue.drainTo(list);
    assertEquals(null, queue.poll());
    assertEquals(0, queue.size());
    assertEquals(CAPACITY, queue.remainingCapacity());
  }

  @Test
  public void testClear() {
    // Offer
    for (long i = 1; i <= CAPACITY; ++i) {
      assertTrue(queue.offer(i));
      assertEquals(i, queue.size());
      assertEquals(CAPACITY - i, queue.remainingCapacity());
    }
    assertFalse(queue.offer(0L));

    queue.clear();
    assertEquals(null, queue.poll());
    assertEquals(0, queue.size());
    assertEquals(CAPACITY, queue.remainingCapacity());
  }

  @Test
  public void testMultiThread() throws InterruptedException {
    int offerThreadCount = 10;
    int pollThreadCount = 5;
    int duration = 5000; // ms
    final AtomicBoolean stop = new AtomicBoolean(false);
    Thread[] offerThreads = new Thread[offerThreadCount];
    for (int i = 0; i < offerThreadCount; i++) {
      offerThreads[i] = new Thread("offer-thread-" + i) {

        @Override
        public void run() {
          Random rand = new Random();
          while (!stop.get()) {
            queue.offer(rand.nextLong());
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
            }
          }
        }

      };
    }
    Thread[] pollThreads = new Thread[pollThreadCount];
    for (int i = 0; i < pollThreadCount; i++) {
      pollThreads[i] = new Thread("poll-thread-" + i) {

        @Override
        public void run() {
          while (!stop.get()) {
            queue.poll();
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
            }
          }
        }

      };
    }
    for (Thread t : offerThreads) {
      t.start();
    }
    for (Thread t : pollThreads) {
      t.start();
    }
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < duration) {
      assertTrue(queue.size() <= CAPACITY);
      Thread.yield();
    }
    stop.set(true);
    for (Thread t : offerThreads) {
      t.join();
    }
    for (Thread t : pollThreads) {
      t.join();
    }
    assertTrue(queue.size() <= CAPACITY);
  }
}
