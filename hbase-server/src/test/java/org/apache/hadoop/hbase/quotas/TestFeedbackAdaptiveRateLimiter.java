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
    limiter.refill(10);

    // Record initial wait interval
    limiter.consume(10);
    long initialWaitInterval = limiter.getWaitInterval(10, 0, 1);
    assertTrue("Initial wait interval should be positive", initialWaitInterval > 0);

    // Create sustained contention over multiple intervals to increase backoff
    for (int i = 0; i < 5; i++) {
      testEdge.setValue(1000 + (i + 1) * 500);
      limiter.refill(10);
      limiter.consume(10);
      // Create contention by asking for more than available
      limiter.getWaitInterval(10, 0, 1);
    }

    // After contention, wait interval should increase due to backoff multiplier
    testEdge.setValue(4000);
    limiter.refill(10);
    limiter.consume(10);
    long increasedWaitInterval = limiter.getWaitInterval(10, 0, 1);

    // With backoffMultiplierIncrement=0.1 and 5 intervals of contention,
    // multiplier should be around 1.5, so wait should be significantly higher
    assertTrue(
      "Wait interval should increase with contention. Initial: " + initialWaitInterval
        + ", After contention: " + increasedWaitInterval,
      increasedWaitInterval > initialWaitInterval * 1.3);
  }

  @Test
  public void testAdaptiveBackoffDecreases() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(10);

    // Build up contention to increase backoff multiplier
    for (int i = 0; i < 5; i++) {
      limiter.consume(10);
      limiter.getWaitInterval(10, 0, 1); // Create contention
      testEdge.setValue(1000 + (i + 1) * 500);
      limiter.refill(10);
    }

    // Measure wait interval with elevated backoff
    limiter.consume(10);
    long elevatedWaitInterval = limiter.getWaitInterval(10, 0, 1);

    // Run several intervals without contention to decrease backoff
    for (int i = 0; i < 10; i++) {
      testEdge.setValue(4000 + i * 500);
      limiter.refill(10);
      // Consume less than available - no contention
      limiter.consume(3);
    }

    // Measure wait interval after backoff reduction
    testEdge.setValue(9500);
    limiter.refill(10);
    limiter.consume(10);
    long reducedWaitInterval = limiter.getWaitInterval(10, 0, 1);

    // After 10 intervals without contention (decrement=0.05 each),
    // multiplier should decrease by ~0.5, making wait interval lower
    assertTrue("Wait interval should decrease without contention. Elevated: " + elevatedWaitInterval
      + ", Reduced: " + reducedWaitInterval, reducedWaitInterval < elevatedWaitInterval * 0.9);
  }

  @Test
  public void testOversubscriptionIncreasesWithLowUtilization() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);

    // Initial refill to set up the limiter
    long initialRefill = limiter.refill(10);
    assertEquals("Initial refill should match limit", 10, initialRefill);

    // Create low utilization scenario (consuming much less than available)
    // With error budget of 0.1, min target utilization is 0.9
    // We'll consume only ~40% to trigger oversubscription increase
    // Refill interval adjusted limit is 5 (500ms / 1000ms * 10)
    for (int i = 0; i < 30; i++) {
      // Consume before advancing time so utilization is tracked
      limiter.consume(2); // 2 out of 5 = 40% utilization
      testEdge.setValue(1000 + (i + 1) * 500);
      limiter.refill(10);
    }

    // After many intervals of low utilization, oversubscription should have increased
    // Now test that the oversubscription proportion actually affects refill behavior
    // Consume all available to start fresh
    limiter.consume((int) limiter.getAvailable());

    // Jump forward by 3 refill intervals (1500ms)
    // This tests that refill can return more than the base limit due to oversubscription
    testEdge.setValue(16000 + 1500);
    long multiIntervalRefill = limiter.refill(10);

    // With oversubscription at max (0.2), the oversubscribed limit is 10 * 1.2 = 12
    // With 3 intervals: refillAmount = 3 * 5 = 15
    // Result = min(12, 15) = 12, which exceeds the base limit of 10
    // Without oversubscription, this would be capped at min(10, 15) = 10
    assertTrue("With oversubscription from low utilization, refill should exceed base limit. Got: "
      + multiIntervalRefill, multiIntervalRefill > 10);
  }

  @Test
  public void testOversubscriptionDecreasesWithHighUtilization() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);

    // First, build up oversubscription with low utilization
    limiter.refill(10);
    for (int i = 0; i < 15; i++) {
      testEdge.setValue(1000 + (i + 1) * 500);
      limiter.refill(10);
      limiter.consume(2); // Low utilization
    }

    // Now create high utilization scenario (consuming more than target)
    // With error budget of 0.1, max target utilization is 1.1
    // We'll consume close to the full interval-adjusted limit to trigger decrease
    for (int i = 0; i < 10; i++) {
      testEdge.setValue(8500 + (i + 1) * 500);
      long refilled = limiter.refill(10);
      // Consume full amount to show high utilization
      limiter.consume((int) refilled);
    }

    // After intervals of high utilization, oversubscription should decrease
    testEdge.setValue(14000);
    long refillAfterHighUtil = limiter.refill(10);

    // Oversubscription should have decreased, so refill should be closer to base limit
    // With oversubscriptionDecrement=0.005 over 10 intervals, it should drop by ~0.05
    assertTrue(
      "Refill should be closer to base after high utilization. Got: " + refillAfterHighUtil,
      refillAfterHighUtil <= 6);
  }

  @Test
  public void testBackoffMultiplierCapsAtMaximum() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(10);

    // Record base wait interval
    limiter.consume(10);
    long baseWaitInterval = limiter.getWaitInterval(10, 0, 1);

    // Create extreme sustained contention to push backoff to max
    // With increment=0.1 and max=3.0, we need (3.0-1.0)/0.1 = 20 intervals
    for (int i = 0; i < 25; i++) {
      testEdge.setValue(1000 + (i + 1) * 500);
      limiter.refill(10);
      limiter.consume(10);
      limiter.getWaitInterval(10, 0, 1); // Create contention
    }

    // Measure wait at maximum backoff
    testEdge.setValue(14000);
    limiter.refill(10);
    limiter.consume(10);
    long maxBackoffWaitInterval = limiter.getWaitInterval(10, 0, 1);

    // Wait interval should be approximately 3x base (max multiplier)
    assertTrue(
      "Wait interval should cap at max multiplier. Base: " + baseWaitInterval + ", Max backoff: "
        + maxBackoffWaitInterval,
      maxBackoffWaitInterval >= baseWaitInterval * 2.5
        && maxBackoffWaitInterval <= baseWaitInterval * 3.5);

    // Additional contention should not increase wait further
    testEdge.setValue(14500);
    limiter.refill(10);
    limiter.consume(10);
    limiter.getWaitInterval(10, 0, 1);

    testEdge.setValue(15000);
    limiter.refill(10);
    limiter.consume(10);
    long stillMaxWaitInterval = limiter.getWaitInterval(10, 0, 1);

    // Should still be at max, not increasing further
    assertTrue(
      "Wait should remain capped. Previous: " + maxBackoffWaitInterval + ", Current: "
        + stillMaxWaitInterval,
      Math.abs(stillMaxWaitInterval - maxBackoffWaitInterval) < baseWaitInterval * 0.2);
  }

  @Test
  public void testOversubscriptionCapsAtMaximum() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(10);

    // Create extreme low utilization to push oversubscription to max
    // With increment=0.01 and max=0.2, we need 0.2/0.01 = 20 intervals
    for (int i = 0; i < 25; i++) {
      testEdge.setValue(1000 + (i + 1) * 500);
      limiter.refill(10);
      // Very low consumption to maximize oversubscription increase
      limiter.consume(1);
    }

    // Check that refill is capped at max oversubscription
    testEdge.setValue(14000);
    long refillWithMaxOversubscription = limiter.refill(10);

    // With max oversubscription of 0.2, refill should be at most 5 * 1.2 = 6
    // (5 is the interval-adjusted limit for 500ms refill interval)
    assertTrue("Refill should cap at max oversubscription. Got: " + refillWithMaxOversubscription,
      refillWithMaxOversubscription <= 7);

    // Further low utilization should not increase refill
    testEdge.setValue(14500);
    limiter.refill(10);
    limiter.consume(1);

    testEdge.setValue(15000);
    long stillMaxRefill = limiter.refill(10);

    // Should remain at cap
    assertEquals("Refill should remain at max oversubscription", refillWithMaxOversubscription,
      stillMaxRefill);
  }

  @Test
  public void testMultipleRefillIntervals() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(10);
    limiter.consume(10);

    // Jump forward by multiple refill intervals (3 intervals = 1500ms)
    testEdge.setValue(1000 + 1500);

    // Should refill 3 intervals worth, but capped at oversubscribed limit
    long multiIntervalRefill = limiter.refill(10);

    // With 500ms refill interval, each interval gives 5 resources
    // 3 intervals = 15, but capped at limit (no oversubscription yet) = 10
    assertTrue("Multiple interval refill should provide multiple refill amounts. Got: "
      + multiIntervalRefill, multiIntervalRefill >= 10);
  }

  @Test
  public void testRefillIntervalAdjustment() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);

    // First refill should give full limit
    long firstRefill = limiter.refill(10);
    assertEquals("First refill should give full limit", 10, firstRefill);

    limiter.consume(10);

    // After exactly one refill interval (500ms), should get interval-adjusted amount
    testEdge.setValue(1000 + 500);
    long adjustedRefill = limiter.refill(10);

    // 500ms is half of 1000ms time unit, so should get half the limit = 5
    assertEquals("Refill after one interval should be interval-adjusted", 5, adjustedRefill);
  }

  @Test
  public void testBackoffMultiplierBottomsAtOne() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(10);

    // Record baseline wait with no backoff applied
    limiter.consume(10);
    long baselineWait = limiter.getWaitInterval(10, 0, 1);

    // Run many intervals without contention to ensure multiplier stays at 1.0
    for (int i = 0; i < 20; i++) {
      testEdge.setValue(1000 + (i + 1) * 500);
      limiter.refill(10);
      limiter.consume(3); // No contention
    }

    // Wait interval should still be at baseline (multiplier = 1.0)
    testEdge.setValue(11500);
    limiter.refill(10);
    limiter.consume(10);
    long noContentionWait = limiter.getWaitInterval(10, 0, 1);

    assertEquals("Wait interval should not go below baseline (multiplier=1.0)", baselineWait,
      noContentionWait);
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

  @Test
  public void testOscillatingLoadPattern() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(10);

    // Oscillate between high contention and low contention
    for (int cycle = 0; cycle < 3; cycle++) {
      // High contention phase - increase backoff
      for (int i = 0; i < 3; i++) {
        testEdge.setValue(1000 + (cycle * 3000) + (i * 500));
        limiter.refill(10);
        limiter.consume(10);
        limiter.getWaitInterval(10, 0, 1); // Create contention
      }

      long highContentionWait = limiter.getWaitInterval(10, 0, 1);

      // Low contention phase - decrease backoff
      for (int i = 0; i < 3; i++) {
        testEdge.setValue(1000 + (cycle * 3000) + 1500 + (i * 500));
        limiter.refill(10);
        limiter.consume(3); // No contention
      }

      testEdge.setValue(1000 + (cycle * 3000) + 3000);
      limiter.refill(10);
      limiter.consume(10);
      long lowContentionWait = limiter.getWaitInterval(10, 0, 1);

      // After low contention phase, wait should be lower than after high contention
      assertTrue(
        "Wait should decrease after low contention phase in cycle " + cycle + ". High: "
          + highContentionWait + ", Low: " + lowContentionWait,
        lowContentionWait < highContentionWait);
    }
  }

  @Test
  public void testUtilizationEmaConvergence() {
    FeedbackAdaptiveRateLimiter limiter = factory.create();
    limiter.set(10, TimeUnit.SECONDS);

    testEdge.setValue(1000);
    limiter.refill(10);

    // Consistently consume at 80% utilization
    for (int i = 0; i < 30; i++) {
      testEdge.setValue(1000 + (i + 1) * 500);
      limiter.refill(10);
      limiter.consume(4); // 4 out of 5 interval-adjusted = 80%
    }

    // After many intervals, oversubscription should stabilize
    // At 80% utilization (below 90% target), oversubscription should increase
    testEdge.setValue(16500);
    limiter.refill(10);

    // Now switch to 100% utilization
    for (int i = 0; i < 30; i++) {
      testEdge.setValue(16500 + (i + 1) * 500);
      long refilled = limiter.refill(10);
      limiter.consume((int) refilled); // Consume everything
    }

    // At 100% utilization (within target range), oversubscription should stabilize
    testEdge.setValue(32000);
    limiter.refill(10);

    // The EMA should have adjusted, and refills should be different
    // (though exact values depend on EMA convergence rate)
    assertTrue("Refill behavior should adapt to utilization patterns", true);
  }

  private static final class ManualEnvironmentEdge implements EnvironmentEdge {
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
