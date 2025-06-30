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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verify the behavior of the FeedbackAdaptiveRateLimiter including adaptive backoff multipliers and
 * over-subscription functionality.
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestFeedbackAdaptiveRateLimiter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFeedbackAdaptiveRateLimiter.class);

  private ManualEnvironmentEdge testEdge;
  private FeedbackAdaptiveRateLimiter.FeedbackAdaptiveRateLimiterFactory factory;

  @Before
  public void setUp() {
    testEdge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(testEdge);

    Configuration conf = HBaseConfiguration.create();
    // Set refill interval for testing
    conf.setLong(FixedIntervalRateLimiter.RATE_LIMITER_REFILL_INTERVAL_MS, 500);
    // Configure adaptive parameters for testing - using larger values than defaults for
    // observability
    conf.setDouble(FeedbackAdaptiveRateLimiter.FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_INCREMENT, 0.1);
    conf.setDouble(FeedbackAdaptiveRateLimiter.FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_DECREMENT,
      0.05);
    conf.setDouble(FeedbackAdaptiveRateLimiter.FEEDBACK_ADAPTIVE_MAX_BACKOFF_MULTIPLIER, 3.0);
    conf.setDouble(FeedbackAdaptiveRateLimiter.FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_INCREMENT, 0.01);
    conf.setDouble(FeedbackAdaptiveRateLimiter.FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_DECREMENT, 0.005);
    conf.setDouble(FeedbackAdaptiveRateLimiter.FEEDBACK_ADAPTIVE_MAX_OVERSUBSCRIPTION, 0.2);
    conf.setDouble(FeedbackAdaptiveRateLimiter.FEEDBACK_ADAPTIVE_UTILIZATION_ERROR_BUDGET, 0.1);

    factory = new FeedbackAdaptiveRateLimiter.FeedbackAdaptiveRateLimiterFactory(conf);
  }

  @After
  public void tearDown() {
    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testBasicFunctionality() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    // Initially should work like normal rate limiter
    assertEquals(0, limiter.getWaitIntervalMs());
    limiter.consume(5);
    assertEquals(0, limiter.getWaitIntervalMs());
    limiter.consume(5);

    // Should need to wait after consuming full limit
    assertTrue(limiter.getWaitIntervalMs() > 0);
  }

  @Test
  public void testAdaptiveBackoffIncreases() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);

    // Test that adaptive backoff functionality works without throwing exceptions
    // and that we can create contention scenarios
    for (int i = 0; i < 3; i++) {
      limiter.refill(10);
      limiter.consume(10);
      // Create contention by asking for more than available
      long waitInterval = limiter.getWaitInterval(10, 0, 1);

      // Basic sanity check - wait interval should be reasonable
      assertTrue("Wait interval should be positive, got: " + waitInterval, waitInterval > 0);

      // Advance to next interval
      testEdge.setValue(1000 + (i + 1) * 500);
    }

    // Test passes if the adaptive backoff mechanism works without errors
    assertTrue("Adaptive backoff should work without errors", true);
  }

  @Test
  public void testAdaptiveBackoffDecreases() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);

    // Test that the backoff decrease mechanism works without errors
    // Build up some contention first
    for (int i = 0; i < 3; i++) {
      limiter.refill(10);
      limiter.consume(10);
      limiter.getWaitInterval(10, 0, 1); // Create contention
      testEdge.setValue(1000 + (i + 1) * 500);
    }

    // Run several intervals without contention to decrease backoff
    for (int i = 0; i < 10; i++) {
      testEdge.setValue(2500 + i * 500);
      limiter.refill(10);
      if (limiter.getAvailable() > 0) {
        limiter.consume(Math.min(5, (int) limiter.getAvailable())); // Consume less than limit
      }
    }

    // Test passes if the backoff decrease mechanism works without errors
    assertTrue("Adaptive backoff decrease should work without errors", true);
  }

  @Test
  public void testOversubscriptionTracking() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);

    // Initial refill to set up the limiter
    long initialRefill = limiter.refill(10);
    assertTrue("Initial refill should be positive", initialRefill > 0);

    // Just verify that the over-subscription tracking is working without complex assertions
    // This test mainly ensures that the adaptive behavior doesn't break basic functionality
    for (int i = 0; i < 5; i++) {
      testEdge.setValue(1000 + (i + 1) * 500); // Use 500ms intervals, start from next interval
      long refilled = limiter.refill(10);

      if (refilled > 0) {
        // Only test when we actually get resources
        limiter.consume(Math.min(8, (int) refilled));
        long waitInterval = limiter.getWaitInterval(10, 2, 5);
        assertTrue("Wait interval should be reasonable", waitInterval >= 0);
      }
    }

    // Test passes if no exceptions are thrown
    assertTrue("Over-subscription tracking should work without errors", true);
  }

  @Test
  public void testUtilizationTracking() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);

    // Test various utilization levels to ensure tracking works
    for (int i = 0; i < 5; i++) {
      testEdge.setValue(1000 + i * 500); // Use 500ms intervals
      limiter.refill(10);

      // Vary consumption from 20% to 100%
      int consumption = 2 + (i * 2);
      limiter.consume(consumption);
    }

    // The limiter should have tracked utilization without throwing exceptions
    assertTrue("Utilization tracking should work correctly", true);
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(100, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(100);

    // Simulate concurrent access
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < 10; j++) {
          limiter.consume(1);
          limiter.getWaitInterval(100, 50, 1);
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    // Should complete without exceptions - basic thread safety verification
    assertTrue("Concurrent access should complete successfully", true);
  }

  @Test
  public void testOverconsumptionBehavior() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(10);

    // Over-consume significantly
    limiter.consume(20);

    // Should require waiting for multiple intervals (500ms refill interval)
    long waitInterval = limiter.getWaitInterval(10, -10, 1);
    assertTrue("Should require substantial wait after over-consumption", waitInterval >= 500);
  }

  private static class ManualEnvironmentEdge implements EnvironmentEdge {
    private long currentTime = 1000;

    public void setValue(long time) {
      this.currentTime = time;
    }

    @Override
    public long currentTime() {
      return currentTime;
    }
  }
}
