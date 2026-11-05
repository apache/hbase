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
package org.apache.hadoop.hbase.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.MetricsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
@Tag(MiscTests.TAG)
public class TestMetricsIO {

  public MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @BeforeEach
  public void setUp() {
    // Several tests register the same source, which the metrics system only allows in this mode.
    HELPER.init();
    MetricsIO.resetForTesting();
  }

  @AfterEach
  public void tearDown() {
    MetricsIO.resetForTesting();
  }

  @Test
  public void testDisabledByDefault() {
    assertFalse(MetricsIO.isEnabled());
    MetricsIO metrics = MetricsIO.getInstance();
    assertInstanceOf(MetricsIO.NoOpMetricsIOSource.class, metrics.getMetricsSource());
    assertSame(metrics, MetricsIO.getInstance());
    // Updates must be silently discarded while disabled.
    metrics.updateFsReadTime(100);
    metrics.updateFsPreadTime(200);
    metrics.updateFsWriteTime(300);
  }

  @Test
  public void testEnable() {
    MetricsIO disabled = MetricsIO.getInstance();
    MetricsIO.enable();
    assertTrue(MetricsIO.isEnabled());
    MetricsIO enabled = MetricsIO.getInstance();
    assertNotSame(disabled, enabled);
    assertFalse(enabled.getMetricsSource() instanceof MetricsIO.NoOpMetricsIOSource);
    assertSame(enabled, MetricsIO.getInstance());

    enabled.updateFsWriteTime(100);
    HELPER.assertCounter("fsWriteTime_numOps", 1, enabled.getMetricsSource());
  }

  @Test
  public void testMetrics() {
    MetricsIO metrics = new MetricsIO(new MetricsIOWrapper() {
      @Override
      public long getChecksumFailures() {
        return 40;
      }
    });

    metrics.updateFsReadTime(100);
    metrics.updateFsReadTime(200);

    metrics.updateFsPreadTime(300);

    metrics.updateFsWriteTime(400);
    metrics.updateFsWriteTime(500);
    metrics.updateFsWriteTime(600);

    HELPER.assertCounter("fsChecksumFailureCount", 40, metrics.getMetricsSource());

    HELPER.assertCounter("fsReadTime_numOps", 2, metrics.getMetricsSource());
    HELPER.assertCounter("fsPReadTime_numOps", 1, metrics.getMetricsSource());
    HELPER.assertCounter("fsWriteTime_numOps", 3, metrics.getMetricsSource());
  }

  @Test
  public void testCreateInstanceUsesSupplierResult() {
    MetricsIOWrapper wrapper = () -> 0;
    MetricsIO expected = new MetricsIO(new MetricsIO.NoOpMetricsIOSource(), wrapper);
    assertSame(expected, MetricsIO.createInstance(() -> expected, wrapper));
  }

  @Test
  public void testFallbackToNoOpSourceOnRuntimeException() {
    MetricsIOWrapper wrapper = () -> 0;
    MetricsIO metrics = MetricsIO.createInstance(() -> {
      // What a second registration attempt of the source fails with, see HBASE-30231
      throw new MetricsException("Metrics source RegionServer,sub=IO already exists!");
    }, wrapper);
    assertFallback(metrics, wrapper);
  }

  @Test
  public void testFallbackToNoOpSourceOnLinkageError() {
    MetricsIOWrapper wrapper = () -> 0;
    MetricsIO metrics = MetricsIO.createInstance(() -> {
      throw new ExceptionInInitializerError("simulated static initializer failure");
    }, wrapper);
    assertFallback(metrics, wrapper);
  }

  private static void assertFallback(MetricsIO metrics, MetricsIOWrapper wrapper) {
    assertInstanceOf(MetricsIO.NoOpMetricsIOSource.class, metrics.getMetricsSource());
    assertSame(wrapper, metrics.getWrapper());
    assertEquals(MetricsIOSource.METRICS_JMX_CONTEXT,
      metrics.getMetricsSource().getMetricsJmxContext());
    // Updates must be silently discarded rather than failing the caller's HFile read or write.
    metrics.updateFsReadTime(100);
    metrics.updateFsPreadTime(200);
    metrics.updateFsWriteTime(300);
  }
}
