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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;

/**
 * Unit test version of rs metrics tests.
 */
@Category(SmallTests.class)
public class TestMetricsRegionServer {
  public static MetricsAssertHelper HELPER =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private MetricsRegionServerWrapperStub wrapper;
  private MetricsRegionServer rsm;
  private MetricsRegionServerSource serverSource;

  @BeforeClass
  public static void classSetUp() {
    HELPER.init();
  }

  @Before
  public void setUp() {
    wrapper = new MetricsRegionServerWrapperStub();
    rsm = new MetricsRegionServer(wrapper);
    serverSource = rsm.getMetricsSource();
  }

  @Test
  public void testWrapperSource() {
    HELPER.assertTag("serverName", "test", serverSource);
    HELPER.assertTag("clusterId", "tClusterId", serverSource);
    HELPER.assertTag("zookeeperQuorum", "zk", serverSource);
    HELPER.assertGauge("regionServerStartTime", 100, serverSource);
    HELPER.assertGauge("regionCount", 101, serverSource);
    HELPER.assertGauge("storeCount", 2, serverSource);
    HELPER.assertGauge("maxStoreFileAge", 2, serverSource);
    HELPER.assertGauge("minStoreFileAge", 2, serverSource);
    HELPER.assertGauge("avgStoreFileAge", 2, serverSource);
    HELPER.assertGauge("numReferenceFiles", 2, serverSource);
    HELPER.assertGauge("hlogFileCount", 10, serverSource);
    HELPER.assertGauge("hlogFileSize", 1024000, serverSource);
    HELPER.assertGauge("storeFileCount", 300, serverSource);
    HELPER.assertGauge("memstoreSize", 1025, serverSource);
    HELPER.assertGauge("storeFileSize", 1900, serverSource);
    HELPER.assertCounter("totalRequestCount", 899, serverSource);
    HELPER.assertCounter("readRequestCount", 997, serverSource);
    HELPER.assertCounter("writeRequestCount", 707, serverSource);
    HELPER.assertCounter("checkMutateFailedCount", 401, serverSource);
    HELPER.assertCounter("checkMutatePassedCount", 405, serverSource);
    HELPER.assertGauge("storeFileIndexSize", 406, serverSource);
    HELPER.assertGauge("staticIndexSize", 407, serverSource);
    HELPER.assertGauge("staticBloomSize", 408, serverSource);
    HELPER.assertGauge("mutationsWithoutWALCount", 409, serverSource);
    HELPER.assertGauge("mutationsWithoutWALSize", 410, serverSource);
    HELPER.assertGauge("percentFilesLocal", 99, serverSource);
    HELPER.assertGauge("compactionQueueLength", 411, serverSource);
    HELPER.assertGauge("flushQueueLength", 412, serverSource);
    HELPER.assertGauge("blockCacheFreeSize", 413, serverSource);
    HELPER.assertGauge("blockCacheCount", 414, serverSource);
    HELPER.assertGauge("blockCacheSize", 415, serverSource);
    HELPER.assertCounter("blockCacheHitCount", 416, serverSource);
    HELPER.assertCounter("blockCacheMissCount", 417, serverSource);
    HELPER.assertCounter("blockCacheEvictionCount", 418, serverSource);
    HELPER.assertGauge("blockCountHitPercent", 98, serverSource);
    HELPER.assertGauge("blockCacheExpressHitPercent", 97, serverSource);
    HELPER.assertCounter("blockCacheFailedInsertionCount", 36, serverSource);
    HELPER.assertCounter("updatesBlockedTime", 419, serverSource);
  }

  @Test
  public void testConstuctor() {
    assertNotNull("There should be a hadoop1/hadoop2 metrics source", rsm.getMetricsSource() );
    assertNotNull("The RegionServerMetricsWrapper should be accessable", rsm.getRegionServerWrapper());
  }

  @Test
  public void testSlowCount() {
    for (int i=0; i < 12; i ++) {
      rsm.updateAppend(12);
      rsm.updateAppend(1002);
    }
    for (int i=0; i < 13; i ++) {
      rsm.updateDelete(13);
      rsm.updateDelete(1003);
    }
    for (int i=0; i < 14; i ++) {
      rsm.updateGet(14);
      rsm.updateGet(1004);
    }
    for (int i=0; i < 15; i ++) {
      rsm.updateIncrement(15);
      rsm.updateIncrement(1005);
    }
    for (int i=0; i < 16; i ++) {
      rsm.updatePut(16);
      rsm.updatePut(1006);
    }

    HELPER.assertCounter("appendNumOps", 24, serverSource);
    HELPER.assertCounter("deleteNumOps", 26, serverSource);
    HELPER.assertCounter("getNumOps", 28, serverSource);
    HELPER.assertCounter("incrementNumOps", 30, serverSource);
    HELPER.assertCounter("mutateNumOps", 32, serverSource);


    HELPER.assertCounter("slowAppendCount", 12, serverSource);
    HELPER.assertCounter("slowDeleteCount", 13, serverSource);
    HELPER.assertCounter("slowGetCount", 14, serverSource);
    HELPER.assertCounter("slowIncrementCount", 15, serverSource);
    HELPER.assertCounter("slowPutCount", 16, serverSource);
  }

  @Test
  public void testPauseMonitor() {
    Configuration conf = new Configuration();
    conf.setLong(JvmPauseMonitor.INFO_THRESHOLD_KEY, 1000L);
    conf.setLong(JvmPauseMonitor.WARN_THRESHOLD_KEY, 10000L);
    JvmPauseMonitor monitor = new JvmPauseMonitor(conf, serverSource);
    monitor.updateMetrics(1500, false);
    HELPER.assertCounter("pauseInfoThresholdExceeded", 1, serverSource);
    HELPER.assertCounter("pauseWarnThresholdExceeded", 0, serverSource);
    HELPER.assertCounter("pauseTimeWithoutGc_num_ops", 1, serverSource);
    HELPER.assertCounter("pauseTimeWithGc_num_ops", 0, serverSource);
    monitor.updateMetrics(15000, true);
    HELPER.assertCounter("pauseInfoThresholdExceeded", 1, serverSource);
    HELPER.assertCounter("pauseWarnThresholdExceeded", 1, serverSource);
    HELPER.assertCounter("pauseTimeWithoutGc_num_ops", 1, serverSource);
    HELPER.assertCounter("pauseTimeWithGc_num_ops", 1, serverSource);
  }
}

