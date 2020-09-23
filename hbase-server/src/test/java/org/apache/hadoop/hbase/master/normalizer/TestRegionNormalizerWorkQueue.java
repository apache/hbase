/*
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
package org.apache.hadoop.hbase.master.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests that {@link RegionNormalizerWorkQueue} implements the contract described in its docstring.
 */
@Category({ MasterTests.class, SmallTests.class})
public class TestRegionNormalizerWorkQueue {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionNormalizerWorkQueue.class);

  @Rule
  public TestName testName = new TestName();

  @Test
  public void testElementUniquenessAndFIFO() throws Exception {
    final RegionNormalizerWorkQueue<Integer> queue = new RegionNormalizerWorkQueue<>();
    final List<Integer> content = new LinkedList<>();
    IntStream.of(4, 3, 2, 1, 4, 3, 2, 1)
      .boxed()
      .forEach(queue::put);
    assertEquals(4, queue.size());
    while (queue.size() > 0) {
      content.add(queue.take());
    }
    assertThat(content, contains(4, 3, 2, 1));

    queue.clear();
    queue.putAll(Arrays.asList(4, 3, 2, 1));
    queue.putAll(Arrays.asList(4, 5));
    assertEquals(5, queue.size());
    content.clear();
    while (queue.size() > 0) {
      content.add(queue.take());
    }
    assertThat(content, contains(4, 3, 2, 1, 5));
  }

  @Test
  public void testPriorityAndFIFO() throws Exception {
    final RegionNormalizerWorkQueue<Integer> queue = new RegionNormalizerWorkQueue<>();
    final List<Integer> content = new LinkedList<>();
    queue.putAll(Arrays.asList(4, 3, 2, 1));
    assertEquals(4, queue.size());
    queue.putFirst(0);
    assertEquals(5, queue.size());
    drainTo(queue, content);
    assertThat("putFirst items should jump the queue, preserving existing order",
      content, contains(0, 4, 3, 2, 1));

    queue.clear();
    content.clear();
    queue.putAll(Arrays.asList(4, 3, 2, 1));
    queue.putFirst(1);
    assertEquals(4, queue.size());
    drainTo(queue, content);
    assertThat("existing items re-added with putFirst should jump the queue",
      content, contains(1, 4, 3, 2));

    queue.clear();
    content.clear();
    queue.putAll(Arrays.asList(4, 3, 2, 1));
    queue.putAllFirst(Arrays.asList(2, 3));
    assertEquals(4, queue.size());
    drainTo(queue, content);
    assertThat(
      "existing items re-added with putAllFirst jump the queue AND honor changes in priority",
      content, contains(2, 3, 4, 1));
  }

  private enum Action {
    PUT,
    PUT_FIRST,
    PUT_ALL,
    PUT_ALL_FIRST,
  }

  /**
   * Test that the uniqueness constraint is honored in the face of concurrent modification.
   */
  @Test
  public void testConcurrentPut() throws Exception {
    final RegionNormalizerWorkQueue<Integer> queue = new RegionNormalizerWorkQueue<>();
    final int maxValue = 100;
    final Runnable producer = () -> {
      final Random rand = ThreadLocalRandom.current();
      for (int i = 0; i < 1_000; i++) {
        final Action action = Action.values()[rand.nextInt(Action.values().length)];
        switch (action) {
          case PUT: {
            final int val = rand.nextInt(maxValue);
            queue.put(val);
            break;
          }
          case PUT_FIRST: {
            final int val = rand.nextInt(maxValue);
            queue.putFirst(val);
            break;
          }
          case PUT_ALL: {
            final List<Integer> vals = rand.ints(5, 0, maxValue)
              .boxed()
              .collect(Collectors.toList());
            queue.putAll(vals);
            break;
          }
          case PUT_ALL_FIRST: {
            final List<Integer> vals = rand.ints(5, 0, maxValue)
              .boxed()
              .collect(Collectors.toList());
            queue.putAllFirst(vals);
            break;
          }
          default:
            fail("Unrecognized action " + action);
        }
      }
    };

    final int numThreads = 5;
    final CompletableFuture<?>[] futures = IntStream.range(0, numThreads)
      .mapToObj(val -> CompletableFuture.runAsync(producer))
      .toArray(CompletableFuture<?>[]::new);
    CompletableFuture.allOf(futures).join();

    final List<Integer> content = new ArrayList<>(queue.size());
    drainTo(queue, content);
    assertThat("at most `maxValue` items should be present.",
      content.size(), lessThanOrEqualTo(maxValue));
    assertEquals("all items should be unique.", content.size(), new HashSet<>(content).size());
  }

  /**
   * Test that calls to {@link RegionNormalizerWorkQueue#take()} block the requesting thread. The
   * producing thread places new entries onto the queue following a known schedule. The consuming
   * thread collects a time measurement between calls to {@code take}. Finally, the test makes
   * coarse-grained assertions of the consumer's observations based on the producer's schedule.
   */
  @Test
  public void testTake() throws Exception {
    final RegionNormalizerWorkQueue<Integer> queue = new RegionNormalizerWorkQueue<>();
    final ConcurrentLinkedQueue<Long> takeTimes = new ConcurrentLinkedQueue<>();
    final AtomicBoolean finished = new AtomicBoolean(false);
    final Runnable consumer = () -> {
      try {
        while (!finished.get()) {
          queue.take();
          takeTimes.add(System.nanoTime());
        }
      } catch (InterruptedException e) {
        fail("interrupted.");
      }
    };

    CompletableFuture<Void> worker = CompletableFuture.runAsync(consumer);
    final long testStart = System.nanoTime();
    for (int i = 0; i < 5; i++) {
      Thread.sleep(10);
      queue.put(i);
    }

    // set finished = true and pipe one more value in case the thread needs an extra pass through
    // the loop.
    finished.set(true);
    queue.put(1);
    worker.get(1, TimeUnit.SECONDS);

    final Iterator<Long> times = takeTimes.iterator();
    assertTrue("should have timing information for at least 2 calls to take.",
      takeTimes.size() >= 5);
    for (int i = 0; i < 5; i++) {
      assertThat(
        "Observations collected in takeTimes should increase by roughly 10ms every interval",
        times.next(), greaterThan(testStart + TimeUnit.MILLISECONDS.toNanos(i * 10)));
    }
  }

  private static <E> void drainTo(final RegionNormalizerWorkQueue<E> queue, Collection<E> dest)
    throws InterruptedException {
    assertThat(queue.size(), greaterThan(0));
    while (queue.size() > 0) {
      dest.add(queue.take());
    }
  }
}
