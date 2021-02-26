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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ClientTests.class, SmallTests.class})
public class TestResultStatsUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestResultStatsUtil.class);

  private static final RegionLoadStats regionLoadStats = new RegionLoadStats(100,
          10,90);
  private static final byte[] regionName = {80};
  private static final ServerName server = ServerName.parseServerName("3.1.yg.n,50,1");

  @Test
  public void testUpdateStats() {
    // Create a tracker
    ServerStatisticTracker serverStatisticTracker = new ServerStatisticTracker();

    // Pass in the tracker for update
    ResultStatsUtil.updateStats(serverStatisticTracker, server, regionName, regionLoadStats);

    // Check that the tracker was updated as expected
    ServerStatistics stats = serverStatisticTracker.getStats(server);

    assertEquals(regionLoadStats.memstoreLoad, stats.getStatsForRegion(regionName)
            .getMemStoreLoadPercent());
    assertEquals(regionLoadStats.compactionPressure, stats.getStatsForRegion(regionName)
            .getCompactionPressure());
    assertEquals(regionLoadStats.heapOccupancy, stats.getStatsForRegion(regionName)
            .getHeapOccupancyPercent());
  }

  @Test
  public void testUpdateStatsRegionNameNull() {
    ServerStatisticTracker serverStatisticTracker = new ServerStatisticTracker();

    ResultStatsUtil.updateStats(serverStatisticTracker, server, null, regionLoadStats);

    ServerStatistics stats = serverStatisticTracker.getStats(server);
    assertNull(stats);
  }

  @Test
  public void testUpdateStatsStatsNull() {
    ServerStatisticTracker serverStatisticTracker = new ServerStatisticTracker();

    ResultStatsUtil.updateStats(serverStatisticTracker, server, regionName, null);

    ServerStatistics stats = serverStatisticTracker.getStats(server);
    assertNull(stats);
  }

  @Test
  public void testUpdateStatsTrackerNull() {
    ServerStatisticTracker serverStatisticTracker = new ServerStatisticTracker();

    ResultStatsUtil.updateStats(null, server, regionName, regionLoadStats);

    ServerStatistics stats = serverStatisticTracker.getStats(server);
    assertNull(stats);
  }
}
