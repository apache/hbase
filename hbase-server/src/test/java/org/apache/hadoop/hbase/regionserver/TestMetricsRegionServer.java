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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit test version of rs metrics tests.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestMetricsRegionServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsRegionServer.class);

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
    Configuration conf = new Configuration(false);
    conf.setBoolean(MetricsRegionServer.RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY, false);
    rsm = new MetricsRegionServer(wrapper, conf, null);
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
    HELPER.assertCounter("totalRowActionRequestCount",
      HELPER.getCounter("readRequestCount", serverSource)
          + HELPER.getCounter("writeRequestCount", serverSource),
      serverSource);
    HELPER.assertCounter("readRequestCount", 997, serverSource);
    HELPER.assertCounter("filteredReadRequestCount", 1997, serverSource);
    HELPER.assertCounter("writeRequestCount", 707, serverSource);
    HELPER.assertCounter("checkMutateFailedCount", 401, serverSource);
    HELPER.assertCounter("checkMutatePassedCount", 405, serverSource);
    HELPER.assertGauge("storeFileIndexSize", 406, serverSource);
    HELPER.assertGauge("staticIndexSize", 407, serverSource);
    HELPER.assertGauge("staticBloomSize", 408, serverSource);
    HELPER.assertGauge("mutationsWithoutWALCount", 409, serverSource);
    HELPER.assertGauge("mutationsWithoutWALSize", 410, serverSource);
    HELPER.assertGauge("percentFilesLocal", 99, serverSource);
    HELPER.assertGauge("percentFilesLocalSecondaryRegions", 99, serverSource);
    HELPER.assertGauge("compactionQueueLength", 411, serverSource);
    HELPER.assertGauge("flushQueueLength", 412, serverSource);
    HELPER.assertGauge("blockCacheFreeSize", 413, serverSource);
    HELPER.assertGauge("blockCacheCount", 414, serverSource);
    HELPER.assertGauge("blockCacheSize", 415, serverSource);
    HELPER.assertCounter("blockCacheHitCount", 416, serverSource);
    HELPER.assertCounter("blockCacheMissCount", 417, serverSource);
    HELPER.assertCounter("blockCacheEvictionCount", 418, serverSource);
    HELPER.assertGauge("blockCacheCountHitPercent", 98, serverSource);
    HELPER.assertGauge("blockCacheExpressHitPercent", 97, serverSource);
    HELPER.assertCounter("blockCacheFailedInsertionCount", 36, serverSource);
    HELPER.assertGauge("l1CacheHitCount", 200, serverSource);
    HELPER.assertGauge("l1CacheMissCount", 100, serverSource);
    HELPER.assertGauge("l1CacheHitRatio", 80, serverSource);
    HELPER.assertGauge("l1CacheMissRatio", 20, serverSource);
    HELPER.assertGauge("l2CacheHitCount", 800, serverSource);
    HELPER.assertGauge("l2CacheMissCount", 200, serverSource);
    HELPER.assertGauge("l2CacheHitRatio", 90, serverSource);
    HELPER.assertGauge("l2CacheMissRatio", 10, serverSource);
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
      rsm.updateAppend(null, 12);
      rsm.updateAppend(null, 1002);
    }
    for (int i=0; i < 13; i ++) {
      rsm.updateDeleteBatch(null, 13);
      rsm.updateDeleteBatch(null, 1003);
    }
    for (int i=0; i < 14; i ++) {
      rsm.updateGet(null, 14);
      rsm.updateGet(null, 1004);
    }
    for (int i=0; i < 15; i ++) {
      rsm.updateIncrement(null, 15);
      rsm.updateIncrement(null, 1005);
    }
    for (int i=0; i < 16; i ++) {
      rsm.updatePutBatch(null, 16);
      rsm.updatePutBatch(null, 1006);
    }

    for (int i=0; i < 17; i ++) {
      rsm.updatePut(null, 17);
      rsm.updateDelete(null, 17);
      rsm.updatePut(null, 1006);
      rsm.updateDelete(null, 1003);
      rsm.updateCheckAndDelete(null, 17);
      rsm.updateCheckAndPut(null, 17);
      rsm.updateCheckAndMutate(null, 17);
    }

    HELPER.assertCounter("appendNumOps", 24, serverSource);
    HELPER.assertCounter("deleteBatchNumOps", 26, serverSource);
    HELPER.assertCounter("getNumOps", 28, serverSource);
    HELPER.assertCounter("incrementNumOps", 30, serverSource);
    HELPER.assertCounter("putBatchNumOps", 32, serverSource);
    HELPER.assertCounter("putNumOps", 34, serverSource);
    HELPER.assertCounter("deleteNumOps", 34, serverSource);
    HELPER.assertCounter("checkAndDeleteNumOps", 17, serverSource);
    HELPER.assertCounter("checkAndPutNumOps", 17, serverSource);
    HELPER.assertCounter("checkAndMutateNumOps", 17, serverSource);

    HELPER.assertCounter("slowAppendCount", 12, serverSource);
    HELPER.assertCounter("slowDeleteCount", 17, serverSource);
    HELPER.assertCounter("slowGetCount", 14, serverSource);
    HELPER.assertCounter("slowIncrementCount", 15, serverSource);
    HELPER.assertCounter("slowPutCount", 17, serverSource);
  }

  @Test
  public void testFlush() {
    rsm.updateFlush(null, 1, 2, 3);
    HELPER.assertCounter("flushTime_num_ops", 1, serverSource);
    HELPER.assertCounter("flushMemstoreSize_num_ops", 1, serverSource);
    HELPER.assertCounter("flushOutputSize_num_ops", 1, serverSource);
    HELPER.assertCounter("flushedMemstoreBytes", 2, serverSource);
    HELPER.assertCounter("flushedOutputBytes", 3, serverSource);

    rsm.updateFlush(null, 10, 20, 30);
    HELPER.assertCounter("flushTimeNumOps", 2, serverSource);
    HELPER.assertCounter("flushMemstoreSize_num_ops", 2, serverSource);
    HELPER.assertCounter("flushOutputSize_num_ops", 2, serverSource);
    HELPER.assertCounter("flushedMemstoreBytes", 22, serverSource);
    HELPER.assertCounter("flushedOutputBytes", 33, serverSource);
  }

  @Test
  public void testCompaction() {
    rsm.updateCompaction(null, false, 1, 2, 3, 4, 5);
    HELPER.assertCounter("compactionTime_num_ops", 1, serverSource);
    HELPER.assertCounter("compactionInputFileCount_num_ops", 1, serverSource);
    HELPER.assertCounter("compactionInputSize_num_ops", 1, serverSource);
    HELPER.assertCounter("compactionOutputFileCount_num_ops", 1, serverSource);
    HELPER.assertCounter("compactedInputBytes", 4, serverSource);
    HELPER.assertCounter("compactedoutputBytes", 5, serverSource);

    rsm.updateCompaction(null, false, 10, 20, 30, 40, 50);
    HELPER.assertCounter("compactionTime_num_ops", 2, serverSource);
    HELPER.assertCounter("compactionInputFileCount_num_ops", 2, serverSource);
    HELPER.assertCounter("compactionInputSize_num_ops", 2, serverSource);
    HELPER.assertCounter("compactionOutputFileCount_num_ops", 2, serverSource);
    HELPER.assertCounter("compactedInputBytes", 44, serverSource);
    HELPER.assertCounter("compactedoutputBytes", 55, serverSource);

    // do major compaction
    rsm.updateCompaction(null, true, 100, 200, 300, 400, 500);

    HELPER.assertCounter("compactionTime_num_ops", 3, serverSource);
    HELPER.assertCounter("compactionInputFileCount_num_ops", 3, serverSource);
    HELPER.assertCounter("compactionInputSize_num_ops", 3, serverSource);
    HELPER.assertCounter("compactionOutputFileCount_num_ops", 3, serverSource);
    HELPER.assertCounter("compactedInputBytes", 444, serverSource);
    HELPER.assertCounter("compactedoutputBytes", 555, serverSource);

    HELPER.assertCounter("majorCompactionTime_num_ops", 1, serverSource);
    HELPER.assertCounter("majorCompactionInputFileCount_num_ops", 1, serverSource);
    HELPER.assertCounter("majorCompactionInputSize_num_ops", 1, serverSource);
    HELPER.assertCounter("majorCompactionOutputFileCount_num_ops", 1, serverSource);
    HELPER.assertCounter("majorCompactedInputBytes", 400, serverSource);
    HELPER.assertCounter("majorCompactedoutputBytes", 500, serverSource);
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

  @Test
  public void testServerQueryMeterSwitch() {
    TableName tn1 = TableName.valueOf("table1");
    // has been set disable in setUp()
    rsm.updateReadQueryMeter(tn1, 500L);
    assertFalse(HELPER.checkGaugeExists("ServerReadQueryPerSecond_count", serverSource));
    rsm.updateWriteQueryMeter(tn1, 500L);
    assertFalse(HELPER.checkGaugeExists("ServerWriteQueryPerSecond_count", serverSource));

    // enable
    Configuration conf = new Configuration(false);
    conf.setBoolean(MetricsRegionServer.RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY, true);
    rsm = new MetricsRegionServer(wrapper, conf, null);
    serverSource = rsm.getMetricsSource();
    rsm.updateReadQueryMeter(tn1, 500L);
    assertTrue(HELPER.checkGaugeExists("ServerWriteQueryPerSecond_count", serverSource));
    HELPER.assertGauge("ServerReadQueryPerSecond_count", 500L, serverSource);
    assertTrue(HELPER.checkGaugeExists("ServerWriteQueryPerSecond_count", serverSource));
    rsm.updateWriteQueryMeter(tn1, 500L);
    HELPER.assertGauge("ServerWriteQueryPerSecond_count", 500L, serverSource);
  }
}

