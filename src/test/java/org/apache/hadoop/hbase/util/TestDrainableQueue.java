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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDrainableQueue implements ParamCallable<Integer>{

  private static final Log LOG = LogFactory.getLog(TestDrainableQueue.class);

  private static final int NUM_ATTEMPTS = 10;
  private static final int NUM_PRODUCERS = 20;
  private static final int NUM_EVENTS_PER_BATCH = 1000;

  private CountDownLatch shouldDrain;
  private DrainableQueue<Integer> q;
  private int eventsProcessed;
  private AtomicInteger numEnqueued = new AtomicInteger();

  private class Producer implements Callable<Void> {

    private final int index;

    private Producer(int index) {
      this.index = index;
    }

    @Override
    public Void call() throws Exception {
      LOG.info("Starting producer " + index);
      try {
        Random rand = new Random(982735927437L + index);
        for (int i = 0; i < NUM_EVENTS_PER_BATCH; ++i) {
          if (q.enqueue(rand.nextInt(100000))) {
            numEnqueued.incrementAndGet();
          }
          if (rand.nextBoolean()) {
            Threads.sleep(rand.nextInt(3));
          }
          shouldDrain.countDown();
        }
      } finally {
        LOG.info("Finishing producer " + index);
      }
      return null;
    }
  }

  @Override
  public void call(Integer x) {
    if (x % (NUM_PRODUCERS * 10) == 0) {
      Threads.sleep(x % 3);
    }
    ++eventsProcessed;
  }

  @Test(timeout = 30 * 1000)
  public void testDrainableQueue() throws Exception {
    for (int attempt = 0; attempt < NUM_ATTEMPTS; ++attempt) {
      final int totalEvents = NUM_PRODUCERS * NUM_EVENTS_PER_BATCH;
      final int drainAfterNEvents = totalEvents / 2;
      shouldDrain = new CountDownLatch(drainAfterNEvents);
      numEnqueued.set(0);
      q = new DrainableQueue<Integer>("queue");
      ExecutorService exec = Executors.newFixedThreadPool(NUM_PRODUCERS);
      CompletionService<Void> cs = new ExecutorCompletionService<Void>(exec);
      List<Future<Void>> futures = new ArrayList<Future<Void>>();
      for (int producer = 0; producer < NUM_PRODUCERS; ++producer) {
        futures.add(cs.submit(new Producer(producer)));
      }
      shouldDrain.await();
      eventsProcessed = 0;
      LOG.info("Starting draining the queue");
      q.drain(this);
      LOG.info("Finished draining the queue");
      assertEquals(numEnqueued.get(), eventsProcessed);
      LOG.info("Events processed: " + eventsProcessed + ", drainAfterNEvents: "
          + drainAfterNEvents);
      assertTrue(eventsProcessed >= drainAfterNEvents);
      for (Future<Void> f : futures) {
        try {
          f.get();
        } catch (ExecutionException ex) {
          LOG.error("Exception from producer thread", ex);
          if (ex.getCause() instanceof AssertionError) {
            throw (AssertionError) ex.getCause();
          }
          throw ex;
        }
      }
      exec.shutdown();
      assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

}
