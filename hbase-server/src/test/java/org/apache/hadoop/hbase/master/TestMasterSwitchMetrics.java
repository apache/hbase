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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.SnapshotCleanupTracker;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestMasterSwitchMetrics {
  private static final MetricsAssertHelper METRICS_HELPER =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private HMaster master;
  private MetricsMasterWrapperImpl wrapper;
  private MetricsMasterSourceImpl source;

  @BeforeEach
  public void setUp() {
    master = mock(HMaster.class);
    when(master.isActiveMaster()).thenReturn(true);
    when(master.isInitialized()).thenReturn(true);
    master.snapshotCleanupTracker = mock(SnapshotCleanupTracker.class);
    wrapper = new MetricsMasterWrapperImpl(master);
    source = new MetricsMasterSourceImpl(wrapper);
  }

  @AfterEach
  public void tearDown() {
    DefaultMetricsSystem.instance().unregisterSource(MetricsMasterSource.METRICS_JMX_CONTEXT);
    MetricRegistries.global().remove(source.getMetricRegistryInfo());
  }

  @Test
  public void testEachSwitchChangesItsGauge() {
    Map<String, Consumer<Boolean>> switches = new HashMap<>();
    switches.put(MetricsMasterSource.BALANCER_ENABLED_NAME,
      enabled -> when(master.isBalancerOn()).thenReturn(enabled));
    switches.put(MetricsMasterSource.NORMALIZER_ENABLED_NAME,
      enabled -> when(master.isNormalizerOn()).thenReturn(enabled));
    switches.put(MetricsMasterSource.SPLIT_ENABLED_NAME,
      enabled -> when(master.isSplitOrMergeEnabled(MasterSwitchType.SPLIT)).thenReturn(enabled));
    switches.put(MetricsMasterSource.MERGE_ENABLED_NAME,
      enabled -> when(master.isSplitOrMergeEnabled(MasterSwitchType.MERGE)).thenReturn(enabled));
    switches.put(MetricsMasterSource.CLEANER_CHORE_ENABLED_NAME,
      enabled -> when(master.isCleanerChoreEnabled()).thenReturn(enabled));
    switches.put(MetricsMasterSource.CATALOG_JANITOR_ENABLED_NAME,
      enabled -> when(master.isCatalogJanitorEnabled()).thenReturn(enabled));
    switches.put(MetricsMasterSource.SNAPSHOT_CLEANUP_ENABLED_NAME,
      enabled -> when(master.snapshotCleanupTracker.isSnapshotCleanupEnabled())
        .thenReturn(enabled));
    Map<String, Integer> expected = new HashMap<>();
    switches.keySet().forEach(name -> expected.put(name, 0));
    for (Map.Entry<String, Consumer<Boolean>> entry : switches.entrySet()) {
      for (boolean enabled : new boolean[] { true, false }) {
        entry.getValue().accept(enabled);
        expected.put(entry.getKey(), enabled ? 1 : 0);
        assertEquals(expected, wrapper.getSwitchStates());
        expected
          .forEach((name, state) -> METRICS_HELPER.assertGauge(name, state.longValue(), source));
      }
    }
  }

  @Test
  public void testOnlyActiveInitializedMasterExposesSwitches() {
    Set<String> metricNames = wrapper.getSwitchStates().keySet();
    when(master.isInitialized()).thenReturn(false);
    assertTrue(wrapper.getSwitchStates().isEmpty());
    metricNames.forEach(name -> assertFalse(METRICS_HELPER.checkGaugeExists(name, source)));
    when(master.isInitialized()).thenReturn(true);
    when(master.isActiveMaster()).thenReturn(false);
    assertTrue(wrapper.getSwitchStates().isEmpty());
    metricNames.forEach(name -> assertFalse(METRICS_HELPER.checkGaugeExists(name, source)));
  }
}
