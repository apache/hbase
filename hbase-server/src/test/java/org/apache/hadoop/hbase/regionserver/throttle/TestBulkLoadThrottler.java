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
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, SmallTests.class })
public class TestBulkLoadThrottler {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadThrottler.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBulkLoadThrottler.class);

  /**
   * unit test for throttling
   */
  @Test
  public void testThrottling() {
    LOG.info("testBulkLoadThrottling");
    Configuration conf1 = new Configuration();
    Configuration conf2 = new Configuration();

    // throttle bandwidth is 100 and 10 bytes/cycle respectively
    BulkLoadThrottler throttler1 = new BulkLoadThrottler(10, conf1);
    BulkLoadThrottler throttler2 = new BulkLoadThrottler(5, conf2);

    long throttler1StartTime = EnvironmentEdgeManager.currentTime();
    for (int i = 1; i < 21; i++) {
      throttler1.limitNextBytes(1);
    }
    long throttler1EndTime = EnvironmentEdgeManager.currentTime();
    long throttler1Interval = throttler1EndTime - throttler1StartTime;
    // twice throttle
    assertTrue(throttler1Interval >= 1900 && throttler1Interval < 2100);

    long throttler2StartTime = EnvironmentEdgeManager.currentTime();
    for (int i = 1; i < 21; i++) {
      throttler2.limitNextBytes(1);
    }
    long throttler2EndTime = EnvironmentEdgeManager.currentTime();
    long throttler2Interval = throttler2EndTime - throttler2StartTime;
    // Four times throttle
    assertTrue(throttler2Interval >= 3900 && throttler2Interval < 4100);

    assertEquals(10, throttler1.getBandwidth());
    // update_all_config or update_config
    conf1.set("hbase.regionserver.bulkload.node.bandwidth", "15");
    throttler1.checkBulkLoadBandwidthChangeAndResetThrottler();
    assertEquals(15, throttler1.getBandwidth());

    long throttler3StartTime = EnvironmentEdgeManager.currentTime();
    for (int i = 1; i < 21; i++) {
      throttler1.limitNextBytes(1);
    }
    long throttler3EndTime = EnvironmentEdgeManager.currentTime();
    long throttler3Interval = throttler3EndTime - throttler3StartTime;
    // once throttle
    assertTrue(throttler3Interval >= 900 && throttler3Interval < 1100);

    assertEquals(5, throttler2.getBandwidth());
    // update_all_config or update_config
    conf2.set("hbase.regionserver.bulkload.node.bandwidth", "10");
    throttler2.checkBulkLoadBandwidthChangeAndResetThrottler();
    assertEquals(10, throttler2.getBandwidth());

    long throttler4StartTime = EnvironmentEdgeManager.currentTime();
    for (int i = 1; i < 21; i++) {
      throttler2.limitNextBytes(1);
    }
    long throttler4EndTime = EnvironmentEdgeManager.currentTime();
    long throttler4Interval = throttler4EndTime - throttler4StartTime;
    // twice throttle
    assertTrue(throttler4Interval >= 1900 && throttler4Interval < 2100);
  }
}
