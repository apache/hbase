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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit test version of rs metrics tests.
 */
@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestMetricsHeapMemoryManager {

  public static MetricsAssertHelper HELPER =
    CompatibilitySingletonFactory.getInstance(MetricsAssertHelper.class);

  private MetricsHeapMemoryManager hmm;
  private MetricsHeapMemoryManagerSource source;

  @BeforeEach
  public void setUp() {
    hmm = new MetricsHeapMemoryManager();
    source = hmm.getMetricsSource();
  }

  @Test
  public void testConstuctor() {
    assertNotNull(source, "There should be a hadoop1/hadoop2 metrics source");
  }

  @Test
  public void testCounter() {
    for (int i = 0; i < 10; i++) {
      hmm.increaseAboveHeapOccupancyLowWatermarkCounter();
    }
    for (int i = 0; i < 11; i++) {
      hmm.increaseTunerDoNothingCounter();
    }

    HELPER.assertCounter("aboveHeapOccupancyLowWaterMarkCounter", 10L, source);
    HELPER.assertCounter("tunerDoNothingCounter", 11L, source);
  }

  @Test
  public void testGauge() {
    hmm.updateBlockedFlushCount(200);
    hmm.updateUnblockedFlushCount(50);
    hmm.setCurMemStoreSizeGauge(256 * 1024 * 1024);
    hmm.setCurMemStoreOnHeapSizeGauge(512 * 1024 * 1024);
    hmm.setCurMemStoreOffHeapSizeGauge(128 * 1024 * 1024);
    hmm.setCurBlockCacheSizeGauge(100 * 1024 * 1024);

    HELPER.assertGauge("blockedFlushGauge", 200, source);
    HELPER.assertGauge("unblockedFlushGauge", 50, source);
    HELPER.assertGauge("memStoreSize", 256 * 1024 * 1024, source);
    HELPER.assertGauge("memStoreOnHeapSize", 512 * 1024 * 1024, source);
    HELPER.assertGauge("memStoreOffHeapSize", 128 * 1024 * 1024, source);
    HELPER.assertGauge("blockCacheSize", 100 * 1024 * 1024, source);
  }
}
