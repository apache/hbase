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
package org.apache.hadoop.hbase.regionserver.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestMetricsThrottleExceptions {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetricsThrottleExceptions.class);

  private MetricRegistry testRegistry;
  private MetricsThrottleExceptions throttleMetrics;

  @After
  public void cleanup() {
    // Clean up global registries after each test to avoid interference
    MetricRegistries.global().clear();
  }

  @Test
  public void testBasicThrottleMetricsRecording() {
    setupTestMetrics();

    // Record a throttle exception
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.NumRequestsExceeded,
      "alice", "users");

    // Verify the counter exists and has correct value
    Optional<Metric> metric =
      testRegistry.get("RpcThrottlingException_Type_NumRequestsExceeded_User_alice_Table_users");
    assertTrue("Counter metric should be present", metric.isPresent());
    assertTrue("Metric should be a counter", metric.get() instanceof Counter);

    Counter counter = (Counter) metric.get();
    assertEquals("Counter should have count of 1", 1, counter.getCount());
  }

  @Test
  public void testMultipleThrottleTypes() {
    setupTestMetrics();

    // Record different types of throttle exceptions
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.NumRequestsExceeded,
      "alice", "users");
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.WriteSizeExceeded, "bob",
      "logs");
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.ReadSizeExceeded, "charlie",
      "metadata");

    // Verify all three counters were created
    verifyCounter(testRegistry,
      "RpcThrottlingException_Type_NumRequestsExceeded_User_alice_Table_users", 1);
    verifyCounter(testRegistry, "RpcThrottlingException_Type_WriteSizeExceeded_User_bob_Table_logs",
      1);
    verifyCounter(testRegistry,
      "RpcThrottlingException_Type_ReadSizeExceeded_User_charlie_Table_metadata", 1);
  }

  @Test
  public void testCounterIncrement() {
    setupTestMetrics();

    // Record the same throttle exception multiple times
    String metricName = "RpcThrottlingException_Type_NumRequestsExceeded_User_alice_Table_users";
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.NumRequestsExceeded,
      "alice", "users");
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.NumRequestsExceeded,
      "alice", "users");
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.NumRequestsExceeded,
      "alice", "users");

    // Verify the counter incremented correctly
    verifyCounter(testRegistry, metricName, 3);
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    setupTestMetrics();

    int numThreads = 10;
    int incrementsPerThread = 100;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicInteger exceptions = new AtomicInteger(0);

    // Create multiple threads that increment the same counter concurrently
    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < incrementsPerThread; j++) {
            throttleMetrics.recordThrottleException(RpcThrottlingException.Type.NumRequestsExceeded,
              "alice", "users");
          }
        } catch (Exception e) {
          exceptions.incrementAndGet();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    // Start all threads at once
    startLatch.countDown();

    // Wait for all threads to complete
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    assertTrue("All threads should complete within timeout", completed);
    assertEquals("No exceptions should occur during concurrent access", 0, exceptions.get());

    // Verify the final counter value
    verifyCounter(testRegistry,
      "RpcThrottlingException_Type_NumRequestsExceeded_User_alice_Table_users",
      numThreads * incrementsPerThread);

    executor.shutdown();
  }

  @Test
  public void testCommonTableNamePatterns() {
    setupTestMetrics();

    // Test common HBase table name patterns that should be preserved
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.NumRequestsExceeded,
      "service-user", "my-app-logs");
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.WriteSizeExceeded,
      "batch.process", "namespace:table-name");
    throttleMetrics.recordThrottleException(RpcThrottlingException.Type.ReadSizeExceeded,
      "user_123", "test_table_v2");

    verifyCounter(testRegistry,
      "RpcThrottlingException_Type_NumRequestsExceeded_User_service-user_Table_my-app-logs", 1);
    verifyCounter(testRegistry,
      "RpcThrottlingException_Type_WriteSizeExceeded_User_batch.process_Table_namespace:table-name",
      1);
    verifyCounter(testRegistry,
      "RpcThrottlingException_Type_ReadSizeExceeded_User_user_123_Table_test_table_v2", 1);
  }

  @Test
  public void testAllThrottleExceptionTypes() {
    setupTestMetrics();

    // Test all 13 throttle exception types from RpcThrottlingException.Type enum
    RpcThrottlingException.Type[] throttleTypes = RpcThrottlingException.Type.values();

    // Record one exception for each type
    for (RpcThrottlingException.Type throttleType : throttleTypes) {
      throttleMetrics.recordThrottleException(throttleType, "testuser", "testtable");
    }

    // Verify all counters were created with correct values
    for (RpcThrottlingException.Type throttleType : throttleTypes) {
      String expectedMetricName =
        "RpcThrottlingException_Type_" + throttleType.name() + "_User_testuser_Table_testtable";
      verifyCounter(testRegistry, expectedMetricName, 1);
    }
  }

  @Test
  public void testMultipleInstances() {
    setupTestMetrics();

    // Test that multiple instances of MetricsThrottleExceptions work with the same registry
    MetricsThrottleExceptions metrics1 = new MetricsThrottleExceptions(testRegistry);
    MetricsThrottleExceptions metrics2 = new MetricsThrottleExceptions(testRegistry);

    // Record different exceptions on each instance
    metrics1.recordThrottleException(RpcThrottlingException.Type.NumRequestsExceeded, "alice",
      "table1");
    metrics2.recordThrottleException(RpcThrottlingException.Type.WriteSizeExceeded, "bob",
      "table2");

    // Verify both counters exist in the shared registry
    verifyCounter(testRegistry,
      "RpcThrottlingException_Type_NumRequestsExceeded_User_alice_Table_table1", 1);
    verifyCounter(testRegistry,
      "RpcThrottlingException_Type_WriteSizeExceeded_User_bob_Table_table2", 1);
  }

  /**
   * Helper method to set up test metrics registry and instance
   */
  private void setupTestMetrics() {
    MetricRegistryInfo registryInfo = getRegistryInfo();
    testRegistry = MetricRegistries.global().create(registryInfo);
    throttleMetrics = new MetricsThrottleExceptions(testRegistry);
  }

  /**
   * Helper method to verify a counter exists and has the expected value
   */
  private void verifyCounter(MetricRegistry registry, String metricName, long expectedCount) {
    Optional<Metric> metric = registry.get(metricName);
    assertTrue("Counter metric '" + metricName + "' should be present", metric.isPresent());
    assertTrue("Metric should be a counter", metric.get() instanceof Counter);

    Counter counter = (Counter) metric.get();
    assertEquals("Counter '" + metricName + "' should have expected count", expectedCount,
      counter.getCount());
  }

  /**
   * Helper method to create the expected MetricRegistryInfo for ThrottleExceptions
   */
  private MetricRegistryInfo getRegistryInfo() {
    return new MetricRegistryInfo("ThrottleExceptions", "Metrics about RPC throttling exceptions",
      "RegionServer,sub=ThrottleExceptions", "regionserver", false);
  }
}
