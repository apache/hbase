package org.apache.hadoop.hbase.consensus.quorum;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

class TimeoutWithLatchForConstituentTimer implements TimeoutEventHandler {
  private AtomicInteger count = new AtomicInteger(0);
  private final TestConstituentTimer test;

  public TimeoutWithLatchForConstituentTimer(final TestConstituentTimer test) {
    this.test = test;
  }

  public int getCount() {
    return count.get();
  }

  @Override
  public void onTimeout() {
    count.incrementAndGet();
    test.latch.countDown();
  }
}

/**
 * Test that the ConstituentTimer behaves like a regular RepeatingTimer.
 */
public class TestConstituentTimer {
  public CountDownLatch latch;
  public ConstituentTimer timer;
  public TimeoutWithLatchForConstituentTimer callback;

  @Before
  public void setUp() {
    latch = new CountDownLatch(1);
    callback = new TimeoutWithLatchForConstituentTimer(this);
    timer = new AggregateTimer().createTimer("timer", 100,
      TimeUnit.MILLISECONDS, callback);
  }

  @Test(timeout=1000)
  public void shouldFireOnTimeout() throws InterruptedException {
    timer.start();
    latch.await();
    assertEquals(1, callback.getCount());
  }

  @Test(timeout=1000)
  public void startShouldBeIdempotent() throws InterruptedException {
    latch = new CountDownLatch(3);
    timer.start();
    timer.start();
    timer.start();
    // Wait 50 ms to make sure the task is running.
    assertFalse("Timeout expected", latch.await(50, TimeUnit.MILLISECONDS));
    timer.stop();
    // Wait several cycles to pass.
    assertFalse("Latch should not reach zero",
      latch.await(500, TimeUnit.MILLISECONDS));
    assertEquals(0, callback.getCount());
  }

  @Test(timeout=1000)
  public void shouldNotFireOnStop() throws InterruptedException {
    timer.start();
    latch.await(50, TimeUnit.MILLISECONDS); // Make sure the timer is running.
    timer.stop();
    assertFalse("Latch should not reach zero",
      latch.await(200, TimeUnit.MILLISECONDS));
    assertEquals(0, callback.getCount());
  }

  @Test(timeout=1000)
  public void shouldFireRepeatedlyIfNotStopped() throws InterruptedException {
    latch = new CountDownLatch(3);
    timer.start();
    assertTrue("Latch should reach zero",
      latch.await(500, TimeUnit.MILLISECONDS));
    assertEquals(3, callback.getCount());
  }

  @Test(timeout=1000)
  public void shouldDelayCallbackOnReset() throws InterruptedException {
    long begin = System.currentTimeMillis();
    timer.start();
    latch.await(50, TimeUnit.MILLISECONDS);
    timer.reset();
    latch.await();
    long end = System.currentTimeMillis();
    // Elapsed time should be >=150 milliseconds by now.
    assertTrue(end - begin >= 150);
    assertEquals(1, callback.getCount());
  }

  @Test(timeout=1000)
  public void shouldNotFireOnShutdown() throws InterruptedException {
    timer.start();
    timer.shutdown();
    assertFalse("Latch should not reach zero",
      latch.await(200, TimeUnit.MILLISECONDS));
    assertEquals(0, callback.getCount());
  }

  @Test(timeout=1000)
  public void shouldRunTheUsualFollowerPattern() throws InterruptedException {
    timer.start();
    assertFalse("Timeout expected", latch.await(50, TimeUnit.MILLISECONDS));
    timer.reset();
    assertFalse("Timeout expected", latch.await(50, TimeUnit.MILLISECONDS));
    timer.stop();
    assertFalse("Timeout expected", latch.await(50, TimeUnit.MILLISECONDS));
    assertEquals("onTimeout handler should not have executed yet", 0,
      callback.getCount());
    timer.start();
    latch.await();
    timer.stop();
    synchronized (this) {
      this.wait(300);  // Wait for three more timer cycles.
    }
    assertEquals(1, callback.getCount());
  }

  @Test
  public void shouldBackoff() throws InterruptedException {
    long delay = 200;
    timer.setDelay(delay, TimeUnit.MILLISECONDS);
    timer.start();
    timer.stop();
    timer.backoff(delay * 10, TimeUnit.MILLISECONDS);
    timer.start();
    synchronized (this) {
      this.wait(delay * 5);
    }
    // We started and stopped the timer, followed by starting it with a large
    // backoff again. The timer shouldn't have fired before the back off
    // duration.
    assertEquals(0, callback.getCount());
  }
}
