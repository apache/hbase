/**
 *
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

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;


@Category({MiscTests.class, SmallTests.class})
public class TestStealJobQueue {

  StealJobQueue<Integer> stealJobQueue;
  BlockingQueue stealFromQueue;

  @Before
  public void setup() {
    stealJobQueue = new StealJobQueue<>();
    stealFromQueue = stealJobQueue.getStealFromQueue();

  }


  @Test
  public void testTake() throws InterruptedException {
    stealJobQueue.offer(3);
    stealFromQueue.offer(10);
    stealJobQueue.offer(15);
    stealJobQueue.offer(4);
    assertEquals(3, stealJobQueue.take().intValue());
    assertEquals(4, stealJobQueue.take().intValue());
    assertEquals("always take from the main queue before trying to steal", 15,
            stealJobQueue.take().intValue());
    assertEquals(10, stealJobQueue.take().intValue());
    assertTrue(stealFromQueue.isEmpty());
    assertTrue(stealJobQueue.isEmpty());
  }

  @Test
  public void testOfferInStealQueueFromShouldUnblock() throws InterruptedException {
    final AtomicInteger taken = new AtomicInteger();
    Thread consumer = new Thread() {
      @Override
      public void run() {
        try {
          Integer n = stealJobQueue.take();
          taken.set(n);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    consumer.start();
    stealFromQueue.offer(3);
    consumer.join(1000);
    assertEquals(3, taken.get());
    consumer.interrupt(); //Ensure the consumer thread will stop.
  }


  @Test
  public void testOfferInStealJobQueueShouldUnblock() throws InterruptedException {
    final AtomicInteger taken = new AtomicInteger();
    Thread consumer = new Thread() {
      @Override
      public void run() {
        try {
          Integer n = stealJobQueue.take();
          taken.set(n);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    consumer.start();
    stealJobQueue.offer(3);
    consumer.join(1000);
    assertEquals(3, taken.get());
    consumer.interrupt(); //Ensure the consumer thread will stop.
  }


  @Test
  public void testPoll() throws InterruptedException {
    stealJobQueue.offer(3);
    stealFromQueue.offer(10);
    stealJobQueue.offer(15);
    stealJobQueue.offer(4);
    assertEquals(3, stealJobQueue.poll(1, TimeUnit.SECONDS).intValue());
    assertEquals(4, stealJobQueue.poll(1, TimeUnit.SECONDS).intValue());
    assertEquals("always take from the main queue before trying to steal", 15,
            stealJobQueue.poll(1, TimeUnit.SECONDS).intValue());
    assertEquals(10, stealJobQueue.poll(1, TimeUnit.SECONDS).intValue());
    assertTrue(stealFromQueue.isEmpty());
    assertTrue(stealJobQueue.isEmpty());
    assertNull(stealJobQueue.poll(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testPutInStealQueueFromShouldUnblockPoll() throws InterruptedException {
    final AtomicInteger taken = new AtomicInteger();
    Thread consumer = new Thread() {
      @Override
      public void run() {
        try {
          Integer n = stealJobQueue.poll(3, TimeUnit.SECONDS);
          taken.set(n);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    consumer.start();
    stealFromQueue.put(3);
    consumer.join(1000);
    assertEquals(3, taken.get());
    consumer.interrupt(); //Ensure the consumer thread will stop.

  }


  @Test
  public void testAddInStealJobQueueShouldUnblockPoll() throws InterruptedException {
    final AtomicInteger taken = new AtomicInteger();
    Thread consumer = new Thread() {
      @Override
      public void run() {
        try {
          Integer n = stealJobQueue.poll(3, TimeUnit.SECONDS);
          taken.set(n);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    consumer.start();
    stealJobQueue.add(3);
    consumer.join(1000);
    assertEquals(3, taken.get());
    consumer.interrupt(); //Ensure the consumer thread will stop.
  }


  @Test
  public void testInteractWithThreadPool() throws InterruptedException {
    StealJobQueue<Runnable> stealTasksQueue = new StealJobQueue<>();
    final CountDownLatch stealJobCountDown = new CountDownLatch(3);
    final CountDownLatch stealFromCountDown = new CountDownLatch(3);
    ThreadPoolExecutor stealPool = new ThreadPoolExecutor(3, 3, 1, TimeUnit.DAYS, stealTasksQueue) {
      @Override
      protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        stealJobCountDown.countDown();
      }

    };

    //This is necessary otherwise no worker will be running and stealing job
    stealPool.prestartAllCoreThreads();

    ThreadPoolExecutor stealFromPool = new ThreadPoolExecutor(3, 3, 1, TimeUnit.DAYS,
            stealTasksQueue.getStealFromQueue()) {
      @Override
      protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        stealFromCountDown.countDown();
      }
    };

    for (int i = 0; i < 4; i++) {
      TestTask task = new TestTask();
      stealFromPool.execute(task);
    }

    for (int i = 0; i < 2; i++) {
      TestTask task = new TestTask();
      stealPool.execute(task);
    }

    stealJobCountDown.await(1, TimeUnit.SECONDS);
    stealFromCountDown.await(1, TimeUnit.SECONDS);
    assertEquals(0, stealFromCountDown.getCount());
    assertEquals(0, stealJobCountDown.getCount());
  }

  class TestTask extends Thread implements Comparable<TestTask> {
    @Override
    public int compareTo(TestTask o) {
      return 0;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}