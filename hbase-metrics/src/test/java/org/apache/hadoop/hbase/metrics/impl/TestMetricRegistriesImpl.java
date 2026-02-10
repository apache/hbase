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
package org.apache.hadoop.hbase.metrics.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link MetricRegistries}.
 */
@Tag(SmallTests.TAG)
public class TestMetricRegistriesImpl {

  @Test
  public void testMetricsRegistriesRemoveRef() {
    MetricRegistryInfo registryInfo =
      new MetricRegistryInfo("testMetrics", null, null, null, false);
    MetricRegistries.global().create(registryInfo);
    Optional<MetricRegistry> registry1 = MetricRegistries.global().get(registryInfo);
    assertTrue(registry1.isPresent());

    MetricRegistries.global().create(registryInfo);
    Optional<MetricRegistry> registry2 = MetricRegistries.global().get(registryInfo);
    assertTrue(registry2.isPresent());

    MetricRegistries.global().remove(registryInfo);
    Optional<MetricRegistry> registry3 = MetricRegistries.global().get(registryInfo);
    assertTrue(registry3.isPresent());

    MetricRegistries.global().remove(registryInfo);
    Optional<MetricRegistry> registry4 = MetricRegistries.global().get(registryInfo);
    assertFalse(registry4.isPresent());
  }
}
