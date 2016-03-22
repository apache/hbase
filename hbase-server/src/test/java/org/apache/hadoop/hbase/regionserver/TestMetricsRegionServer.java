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

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;

/**
 * Unit test version of rs metrics tests.
 */
@Category({RegionServerTests.class, SmallTests.class})
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

  String FLUSH_TIME = "flushTime";
  String FLUSH_TIME_DESC = "Histogram for the time in millis for memstore flush";
  String FLUSH_MEMSTORE_SIZE = "flushMemstoreSize";
  String FLUSH_MEMSTORE_SIZE_DESC = "Histogram for number of bytes in the memstore for a flush";
  String FLUSH_FILE_SIZE = "flushFileSize";
  String FLUSH_FILE_SIZE_DESC = "Histogram for number of bytes in the resulting file for a flush";
  String FLUSHED_OUTPUT_BYTES = "flushedOutputBytes";
  String FLUSHED_OUTPUT_BYTES_DESC = "Total number of bytes written from flush";
  String FLUSHED_MEMSTORE_BYTES = "flushedMemstoreBytes";
  String FLUSHED_MEMSTORE_BYTES_DESC = "Total number of bytes of cells in memstore from flush";

  @Test
  public void testFlush() {
    rsm.updateFlush(1, 2, 3);
    HELPER.assertCounter("flushTime_num_ops", 1, serverSource);
    HELPER.assertCounter("flushMemstoreSize_num_ops", 1, serverSource);
    HELPER.assertCounter("flushOutputSize_num_ops", 1, serverSource);
    HELPER.assertCounter("flushedMemstoreBytes", 2, serverSource);
    HELPER.assertCounter("flushedOutputBytes", 3, serverSource);

    rsm.updateFlush(10, 20, 30);
    HELPER.assertCounter("flushTimeNumOps", 2, serverSource);
    HELPER.assertCounter("flushMemstoreSize_num_ops", 2, serverSource);
    HELPER.assertCounter("flushOutputSize_num_ops", 2, serverSource);
    HELPER.assertCounter("flushedMemstoreBytes", 22, serverSource);
    HELPER.assertCounter("flushedOutputBytes", 33, serverSource);
  }

  @Test
  public void testCompaction() {
    rsm.updateCompaction(false, 1, 2, 3, 4, 5);
    HELPER.assertCounter("compactionTime_num_ops", 1, serverSource);
    HELPER.assertCounter("compactionInputFileCount_num_ops", 1, serverSource);
    HELPER.assertCounter("compactionInputSize_num_ops", 1, serverSource);
    HELPER.assertCounter("compactionOutputFileCount_num_ops", 1, serverSource);
    HELPER.assertCounter("compactedInputBytes", 4, serverSource);
    HELPER.assertCounter("compactedoutputBytes", 5, serverSource);

    rsm.updateCompaction(false, 10, 20, 30, 40, 50);
    HELPER.assertCounter("compactionTime_num_ops", 2, serverSource);
    HELPER.assertCounter("compactionInputFileCount_num_ops", 2, serverSource);
    HELPER.assertCounter("compactionInputSize_num_ops", 2, serverSource);
    HELPER.assertCounter("compactionOutputFileCount_num_ops", 2, serverSource);
    HELPER.assertCounter("compactedInputBytes", 44, serverSource);
    HELPER.assertCounter("compactedoutputBytes", 55, serverSource);

    // do major compaction
    rsm.updateCompaction(true, 100, 200, 300, 400, 500);

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
}

