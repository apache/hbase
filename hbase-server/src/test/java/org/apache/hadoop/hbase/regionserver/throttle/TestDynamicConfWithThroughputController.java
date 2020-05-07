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
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestDynamicConfWithThroughputController {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule
          .forClass(TestDynamicConfWithThroughputController.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDynamicConfWithThroughputController.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final double EPSILON = 1E-6;

  long throughputLimit = 1024L * 1024;

  long newMaxThroughputUpperBound = throughputLimit * 6;

  long newMaxThroughputLowerBound = throughputLimit * 3;

  long newOffpeakLimit = throughputLimit * 4;

  @Test
  public void testThroughputTuning() throws Exception {

    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY,
        DefaultStoreEngine.class.getName());
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 200);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    conf.set(
        CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
        PressureAwareCompactionThroughputController.class.getName());
    int tunePeriod = 5000;
    conf.setInt(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD,
        tunePeriod);

    TEST_UTIL.startMiniCluster(1);
    HRegionServer regionServer =
        TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);

    PressureAwareCompactionThroughputController throughputController =
        (PressureAwareCompactionThroughputController) regionServer.compactSplitThread
            .getCompactionThroughputController();

    throughputLimit = 50L * 1024 * 1024;
    assertEquals(throughputLimit, throughputController.getMaxThroughput(),
        EPSILON);
    // change Controller to NoLimitThroughputController
    conf.set(
        CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
        NoLimitThroughputController.class.getName());
    regionServer.compactSplitThread.onConfigurationChange(conf);
    ThroughputController throughputController02 =
        regionServer.compactSplitThread.getCompactionThroughputController();
    assertEquals(NoLimitThroughputController.class,
        throughputController02.getClass());

    // change Controller to PressureAwareCompactionThroughputController
    conf.set(
        CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
        PressureAwareCompactionThroughputController.class.getName());
    conf.setInt(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD,
        tunePeriod);
    conf.setLong(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
        newMaxThroughputUpperBound);
    conf.setLong(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
        newMaxThroughputLowerBound);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_START_HOUR, 0);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_END_HOUR, 6);
    conf.setLong(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK,
        newOffpeakLimit);
    regionServer.compactSplitThread.onConfigurationChange(conf);

    throughputController =
        (PressureAwareCompactionThroughputController) regionServer.compactSplitThread
            .getCompactionThroughputController();
    assertEquals(newMaxThroughputUpperBound,
        throughputController.getMaxThroughputUpperBound(), EPSILON);

    assertEquals(newMaxThroughputLowerBound,
        throughputController.getMaxThroughputLowerBound(), EPSILON);

    Thread.sleep(tunePeriod + 3000);
    if (throughputController.getOffPeakHours().isOffPeakHour()) {
      assertEquals(newOffpeakLimit, throughputController.getMaxThroughput(),
          EPSILON);
    } else {
      assertEquals(newMaxThroughputLowerBound,
          throughputController.getMaxThroughput(), EPSILON);
    }

    // change Controller to PressureAwareCompactionThroughputController
    conf.set(
        CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
        PressureAwareCompactionThroughputController.class.getName());
    conf.setInt(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD,
        tunePeriod);
    conf.setLong(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
        2 * newMaxThroughputUpperBound);
    conf.setLong(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
        2 * newMaxThroughputLowerBound);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_START_HOUR, 0);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_END_HOUR, 6);
    conf.setLong(
        PressureAwareCompactionThroughputController
            .HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK,
        2 * newOffpeakLimit);
    regionServer.compactSplitThread.onConfigurationChange(conf);

    throughputController =
        (PressureAwareCompactionThroughputController) regionServer.compactSplitThread
            .getCompactionThroughputController();
    assertEquals(2 * newMaxThroughputUpperBound,
        throughputController.getMaxThroughputUpperBound(), EPSILON);

    assertEquals(2 * newMaxThroughputLowerBound,
        throughputController.getMaxThroughputLowerBound(), EPSILON);

    Thread.sleep(tunePeriod + 3000);
    if (throughputController.getOffPeakHours().isOffPeakHour()) {
      assertEquals(2 * newOffpeakLimit, throughputController.getMaxThroughput(),
          EPSILON);
    } else {
      assertEquals(2 * newMaxThroughputLowerBound,
          throughputController.getMaxThroughput(), EPSILON);
    }

  }
}
