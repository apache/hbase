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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestBoundedConcurrentLinkedQueue {
  private final static int CAPACITY = 16;

  private BoundedConcurrentLinkedQueue<Long> queue;

  @Before
  public void setUp() throws Exception {
    this.queue = new BoundedConcurrentLinkedQueue<Long>(CAPACITY);
  }

  @After
  public void tearDown() throws Exception {
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
}
