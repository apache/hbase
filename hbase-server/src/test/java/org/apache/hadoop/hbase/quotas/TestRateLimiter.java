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

package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verify the behaviour of the Rate Limiter.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestRateLimiter {
  @Test
  public void testWaitIntervalTimeUnitSeconds() {
    testWaitInterval(TimeUnit.SECONDS, 10, 100);
  }

  @Test
  public void testWaitIntervalTimeUnitMinutes() {
    testWaitInterval(TimeUnit.MINUTES, 10, 6000);
  }

  @Test
  public void testWaitIntervalTimeUnitHours() {
    testWaitInterval(TimeUnit.HOURS, 10, 360000);
  }

  @Test
  public void testWaitIntervalTimeUnitDays() {
    testWaitInterval(TimeUnit.DAYS, 10, 8640000);
  }

  private void testWaitInterval(final TimeUnit timeUnit, final long limit,
      final long expectedWaitInterval) {
    RateLimiter limiter = new AverageIntervalRateLimiter();
    limiter.set(limit, timeUnit);

    long nowTs = 0;
    // consume all the available resources, one request at the time.
    // the wait interval should be 0
    for (int i = 0; i < (limit - 1); ++i) {
      assertTrue(limiter.canExecute());
      limiter.consume();
      long waitInterval = limiter.waitInterval();
      assertEquals(0, waitInterval);
    }

    for (int i = 0; i < (limit * 4); ++i) {
      // There is one resource available, so we should be able to
      // consume it without waiting.
      limiter.setNextRefillTime(limiter.getNextRefillTime() - nowTs);
      assertTrue(limiter.canExecute());
      assertEquals(0, limiter.waitInterval());
      limiter.consume();
      // No more resources are available, we should wait for at least an interval.
      long waitInterval = limiter.waitInterval();
      assertEquals(expectedWaitInterval, waitInterval);

      // set the nowTs to be the exact time when resources should be available again.
      nowTs = waitInterval;

      // artificially go into the past to prove that when too early we should fail.
      long temp = nowTs + 500;
      limiter.setNextRefillTime(limiter.getNextRefillTime() + temp);
      assertFalse(limiter.canExecute());
      //Roll back the nextRefillTime set to continue further testing
      limiter.setNextRefillTime(limiter.getNextRefillTime() - temp);
    }
  }

  @Test
  public void testOverconsumptionAverageIntervalRefillStrategy() {
    RateLimiter limiter = new AverageIntervalRateLimiter();
    limiter.set(10, TimeUnit.SECONDS);

    // 10 resources are available, but we need to consume 20 resources
    // Verify that we have to wait at least 1.1sec to have 1 resource available
    assertTrue(limiter.canExecute());
    limiter.consume(20);
    // To consume 1 resource wait for 100ms
    assertEquals(100, limiter.waitInterval(1));
    // To consume 10 resource wait for 1000ms
    assertEquals(1000, limiter.waitInterval(10));

    limiter.setNextRefillTime(limiter.getNextRefillTime() - 900);
    // Verify that after 1sec the 1 resource is available
    assertTrue(limiter.canExecute(1));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 100);
    // Verify that after 1sec the 10 resource is available
    assertTrue(limiter.canExecute());
    assertEquals(0, limiter.waitInterval());
  }

  @Test
  public void testOverconsumptionFixedIntervalRefillStrategy() throws InterruptedException {
    RateLimiter limiter = new FixedIntervalRateLimiter();
    limiter.set(10, TimeUnit.SECONDS);

    // 10 resources are available, but we need to consume 20 resources
    // Verify that we have to wait at least 1.1sec to have 1 resource available
    assertTrue(limiter.canExecute());
    limiter.consume(20);
    // To consume 1 resource also wait for 1000ms
    assertEquals(1000, limiter.waitInterval(1));
    // To consume 10 resource wait for 100ms
    assertEquals(1000, limiter.waitInterval(10));

    limiter.setNextRefillTime(limiter.getNextRefillTime() - 900);
    // Verify that after 1sec also no resource should be available
    assertFalse(limiter.canExecute(1));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 100);

    // Verify that after 1sec the 10 resource is available
    assertTrue(limiter.canExecute());
    assertEquals(0, limiter.waitInterval());
  }

  @Test
  public void testFixedIntervalResourceAvailability() throws Exception {
    RateLimiter limiter = new FixedIntervalRateLimiter();
    limiter.set(10, TimeUnit.SECONDS);

    assertTrue(limiter.canExecute(10));
    limiter.consume(3);
    assertEquals(7, limiter.getAvailable());
    assertFalse(limiter.canExecute(10));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertTrue(limiter.canExecute(10));
    assertEquals(10, limiter.getAvailable());
  }

  @Test
  public void testLimiterBySmallerRate() throws InterruptedException {
    // set limiter is 10 resources per seconds
    RateLimiter limiter = new FixedIntervalRateLimiter();
    limiter.set(10, TimeUnit.SECONDS);

    int count = 0; // control the test count
    while ((count++) < 10) {
      // test will get 3 resources per 0.5 sec. so it will get 6 resources per sec.
      limiter.setNextRefillTime(limiter.getNextRefillTime() - 500);
      for (int i = 0; i < 3; i++) {
        // 6 resources/sec < limit, so limiter.canExecute(nowTs, lastTs) should be true
        assertEquals(true, limiter.canExecute());
        limiter.consume();
      }
    }
  }

  @Test
  public void testCanExecuteOfAverageIntervalRateLimiter() throws InterruptedException {
    RateLimiter limiter = new AverageIntervalRateLimiter();
    // when set limit is 100 per sec, this AverageIntervalRateLimiter will support at max 200 per sec
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(50, testCanExecuteByRate(limiter, 50));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(100, testCanExecuteByRate(limiter, 100));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(200, testCanExecuteByRate(limiter, 200));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(200, testCanExecuteByRate(limiter, 500));
  }

  @Test
  public void testCanExecuteOfFixedIntervalRateLimiter() throws InterruptedException {
    RateLimiter limiter = new FixedIntervalRateLimiter();
    // when set limit is 100 per sec, this FixedIntervalRateLimiter will support at max 100 per sec
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(50, testCanExecuteByRate(limiter, 50));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(100, testCanExecuteByRate(limiter, 100));

    // refill the avail to limit
    limiter.set(100, TimeUnit.SECONDS);
    limiter.setNextRefillTime(EnvironmentEdgeManager.currentTime());
    assertEquals(100, testCanExecuteByRate(limiter, 200));
  }

  public int testCanExecuteByRate(RateLimiter limiter, int rate) {
    int request = 0;
    int count = 0;
    while ((request++) < rate) {
      limiter.setNextRefillTime(limiter.getNextRefillTime() - limiter.getTimeUnitInMillis() / rate);
      if (limiter.canExecute()) {
        count++;
        limiter.consume();
      }
    }
    return count;
  }

  @Test
  public void testRefillOfAverageIntervalRateLimiter() throws InterruptedException {
    RateLimiter limiter = new AverageIntervalRateLimiter();
    limiter.set(60, TimeUnit.SECONDS);
    assertEquals(60, limiter.getAvailable());
    // first refill, will return the number same with limit
    assertEquals(60, limiter.refill(limiter.getLimit()));

    limiter.consume(30);

    // after 0.2 sec, refill should return 12
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 200);
    assertEquals(12, limiter.refill(limiter.getLimit()));

    // after 0.5 sec, refill should return 30
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 500);
    assertEquals(30, limiter.refill(limiter.getLimit()));

    // after 1 sec, refill should return 60
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertEquals(60, limiter.refill(limiter.getLimit()));

    // after more than 1 sec, refill should return at max 60
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 3000);
    assertEquals(60, limiter.refill(limiter.getLimit()));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 5000);
    assertEquals(60, limiter.refill(limiter.getLimit()));
  }

  @Test
  public void testRefillOfFixedIntervalRateLimiter() throws InterruptedException {
    RateLimiter limiter = new FixedIntervalRateLimiter();
    limiter.set(60, TimeUnit.SECONDS);
    assertEquals(60, limiter.getAvailable());
    // first refill, will return the number same with limit
    assertEquals(60, limiter.refill(limiter.getLimit()));

    limiter.consume(30);

    // after 0.2 sec, refill should return 0
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 200);
    assertEquals(0, limiter.refill(limiter.getLimit()));

    // after 0.5 sec, refill should return 0
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 500);
    assertEquals(0, limiter.refill(limiter.getLimit()));

    // after 1 sec, refill should return 60
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 1000);
    assertEquals(60, limiter.refill(limiter.getLimit()));

    // after more than 1 sec, refill should return at max 60
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 3000);
    assertEquals(60, limiter.refill(limiter.getLimit()));
    limiter.setNextRefillTime(limiter.getNextRefillTime() - 5000);
    assertEquals(60, limiter.refill(limiter.getLimit()));
  }
}
