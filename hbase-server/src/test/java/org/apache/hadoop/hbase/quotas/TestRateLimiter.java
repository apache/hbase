/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verify the behaviour of the Rate Limiter.
 */
@Category({ SmallTests.class })
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
    RateLimiter limiter = new RateLimiter();
    limiter.set(limit, timeUnit);

    long nowTs = 0;
    long lastTs = 0;

    // consume all the available resources, one request at the time.
    // the wait interval should be 0
    for (int i = 0; i < (limit - 1); ++i) {
      assertTrue(limiter.canExecute(nowTs, lastTs));
      limiter.consume();
      long waitInterval = limiter.waitInterval();
      assertEquals(0, waitInterval);
    }

    for (int i = 0; i < (limit * 4); ++i) {
      // There is one resource available, so we should be able to
      // consume it without waiting.
      assertTrue(limiter.canExecute(nowTs, lastTs));
      assertEquals(0, limiter.waitInterval());
      limiter.consume();
      lastTs = nowTs;

      // No more resources are available, we should wait for at least an interval.
      long waitInterval = limiter.waitInterval();
      assertEquals(expectedWaitInterval, waitInterval);

      // set the nowTs to be the exact time when resources should be available again.
      nowTs += waitInterval;

      // artificially go into the past to prove that when too early we should fail.
      assertFalse(limiter.canExecute(nowTs - 500, lastTs));
    }
  }

  @Test
  public void testOverconsumption() {
    RateLimiter limiter = new RateLimiter();
    limiter.set(10, TimeUnit.SECONDS);

    // 10 resources are available, but we need to consume 20 resources
    // Verify that we have to wait at least 1.1sec to have 1 resource available
    assertTrue(limiter.canExecute(0, 0));
    limiter.consume(20);
    assertEquals(1100, limiter.waitInterval());

    // Verify that after 1sec we need to wait for another 0.1sec to get a resource available
    assertFalse(limiter.canExecute(1000, 0));
    assertEquals(100, limiter.waitInterval());

    // Verify that after 1.1sec the resource is available
    assertTrue(limiter.canExecute(1100, 0));
    assertEquals(0, limiter.waitInterval());
  }
}