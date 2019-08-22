package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, RegionServerTests.class})
public class TestResultStatsUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestGet.class);

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

    Assert.assertEquals(stats.getStatsForRegion(regionName).getMemStoreLoadPercent(),
            regionLoadStats.memstoreLoad);
    Assert.assertEquals(stats.getStatsForRegion(regionName).getCompactionPressure(),
            regionLoadStats.compactionPressure);
    Assert.assertEquals(stats.getStatsForRegion(regionName).getHeapOccupancyPercent(),
            regionLoadStats.heapOccupancy);
  }

  @Test
  public void testUpdateStatsRegionNameNull() {
    ServerStatisticTracker serverStatisticTracker = new ServerStatisticTracker();

    ResultStatsUtil.updateStats(serverStatisticTracker, server, null, regionLoadStats);

    ServerStatistics stats = serverStatisticTracker.getStats(server);
    Assert.assertEquals(stats,null);
  }

  @Test
  public void testUpdateStatsStatsNull() {
    ServerStatisticTracker serverStatisticTracker = new ServerStatisticTracker();

    ResultStatsUtil.updateStats(serverStatisticTracker, server, regionName, null);

    ServerStatistics stats = serverStatisticTracker.getStats(server);
    Assert.assertEquals(stats,null);
  }

  @Test
  public void testUpdateStatsTrackerNull() {
    ServerStatisticTracker serverStatisticTracker = new ServerStatisticTracker();

    ResultStatsUtil.updateStats(null, server, regionName, regionLoadStats);

    ServerStatistics stats = serverStatisticTracker.getStats(server);
    Assert.assertEquals(stats,null);
  }
}