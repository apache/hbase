package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.RegionChecker.RegionAvailabilityInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRegionChecker {
  final static Log LOG = LogFactory.getLog(TestRegionChecker.class);
  private final static RegionMovementTestHelper TEST_UTIL = new RegionMovementTestHelper();
  private final static int SLAVES = 4;
  private static int REGION_NUM = 10;
  private static RegionChecker regionChecker;
  private static MiniHBaseCluster cluster;
  private static final String TABLE_NAME_BASE = "testRegionAssignment";

  /*
    EPS is small enough and fits for comparing availabilities
    before and after some RegionChecker events:

    we have 2 availabilities - numbers like [0, 1]:
    a - availability before
    b = availability after
    after killing region and after it's being unavailable for 1 sec
    b will be = a - 1000/timeDif
    timeDif is 7*24*60*60*1000 for week => b = a-1.65344*1e-6
    timeDif is 24*60*60*1000 for day => b = a-11.57408*1e-6;
  */
  private final double EPS = 1e-9;

  /**
   * Set up the cluster.  This will start a mini cluster and enable or disable the region checker
   * @param enableRegionChecker if the region checker should run on the master.
   * @throws Exception
   */
  public static void init(boolean enableRegionChecker) throws Exception
  {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Enable the favored nodes based load balancer
    conf.set("hbase.loadbalancer.impl",
      "org.apache.hadoop.hbase.master.RegionManager$AssignmentLoadBalancer");

    conf.setInt("hbase.master.meta.thread.rescanfrequency", 5000);
    conf.setInt("hbase.regionserver.msginterval", 1000);
    conf.setLong("hbase.regionserver.transientAssignment.regionHoldPeriod", 2000);
    conf.setBoolean("hbase.master.regionchecker.enabled", enableRegionChecker);

    TEST_UTIL.startMiniCluster(SLAVES);

    cluster = TEST_UTIL.getHBaseCluster();
    regionChecker = cluster.getActiveMaster().getServerManager().getRegionChecker();
  }

  @After
  public void cleanUp() throws Exception {
    TEST_UTIL.resetLastOpenedRegionCount();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testDisabledRegionChecker() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    init(false);

    assertEquals(-1.0, regionChecker.getLastDayAvailability(), EPS);
    assertEquals(-1.0, regionChecker.getLastWeekAvailability(), EPS);
    assertTrue(regionChecker.getDetailedLastDayAvailability().isEmpty());
    assertTrue(regionChecker.getDetailedLastWeekAvailability().isEmpty());
  }

  @Test(timeout = 180000)
  public void testAvailabilityGoesDownWithRegionFail() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    init(true);

    // Create a table with REGION_NUM regions.
    String tableName = TABLE_NAME_BASE + "testAvailabilityGoesDownWithRegionFail";
    TEST_UTIL.createTable(tableName, REGION_NUM);
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Set<HRegionInfo> allRegions = ht.getRegionsInfo().keySet();
    final int regionsMove = 1;

    int serverId = this.getRegionServerId();
    HRegionInfo regionToKill = null;
    List<String> regionsToKill = new ArrayList<String> ();
    for (HRegion region : cluster.getRegionServer(serverId).getOnlineRegionsAsArray()) {
      if (!region.getRegionInfo().isMetaRegion() && !region.getRegionInfo().isRootRegion()) {
        regionToKill = region.getRegionInfo();
        regionsToKill.add(regionToKill.getRegionNameAsString());
        break;
      }
    }

    TEST_UTIL.waitOnStableRegionMovement();
    TEST_UTIL.resetLastOpenedRegionCount();

    LOG.debug("killing '" + regionToKill.getRegionNameAsString() + "' region");
    cluster.getRegionServer(serverId).closeRegion(regionToKill, true);
    TEST_UTIL.verifyRegionMovementNum(regionsMove);
    LOG.debug("killed '" + regionToKill.getRegionNameAsString() + "' region");

    check(allRegions, regionsToKill);

    TEST_UTIL.deleteTable(Bytes.toBytes(tableName));
    TEST_UTIL.resetLastOpenedRegionCount();
  }

  @Test(timeout = 360000)
  public void testAvailabilityGoesDownWithRegionServerCleanFail() throws Exception {
    testAvailabilityGoesDownWithRegionServerFail(true);
  }

  @Test(timeout = 360000)
  public void testAvailabilityGoesDownWithRegionServerUncleanFail() throws Exception {
    testAvailabilityGoesDownWithRegionServerFail(false);
  }

  public void testAvailabilityGoesDownWithRegionServerFail(boolean isFailClean) throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    init(true);

    // Create a table with REGION_NUM regions.
    String tableName = TABLE_NAME_BASE + "testAvailabilityGoesDownWithRegionServerFail" + isFailClean;
    TEST_UTIL.createTable(tableName, REGION_NUM);
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Set<HRegionInfo> allRegions = ht.getRegionsInfo().keySet();

    int serverId = this.getRegionServerId();

    List<String> regionsToKill = new ArrayList<String>();
    for (HRegionInfo info : cluster.getRegionServer(serverId).getRegionsAssignment()) {
      if (!info.isMetaRegion() && !info.isRootRegion())
        regionsToKill.add(info.getRegionNameAsString());
    }

    TEST_UTIL.resetLastOpenedRegionCount();

    int regionCnt = cluster.getRegionServer(serverId).getOnlineRegions().size();

    if(isFailClean) {
      LOG.debug("killing regionServer clean");
      cluster.stopRegionServer(serverId);
      LOG.debug("killed regionServer clean");
    }
    else {
      LOG.debug("killing regionServer unclean");
      cluster.getRegionServer(serverId).kill();
      LOG.debug("killed regionServer unclean");
    }

    TEST_UTIL.verifyRegionMovementNum(regionCnt);

    check(allRegions, regionsToKill);

    TEST_UTIL.deleteTable(Bytes.toBytes(tableName));
    TEST_UTIL.resetLastOpenedRegionCount();
  }

  private void check(Set<HRegionInfo> allRegions, List<String> regionsToKill)
  {
    double avDayBefore = 1.0;
    double avWeekBefore = 1.0;
    double avDayAfter = regionChecker.getLastDayAvailability();
    double avWeekAfter = regionChecker.getLastWeekAvailability();
    Map<String, RegionAvailabilityInfo> avDetDayAfter = regionChecker.getDetailedLastDayAvailability();
    Map<String, RegionAvailabilityInfo> avDetWeekAfter = regionChecker.getDetailedLastWeekAvailability();

    LOG.debug("avDayBefore " + avDayBefore);
    LOG.debug("avDayAfter " + avDayAfter);
    LOG.debug("avWeekBefore " + avWeekBefore);
    LOG.debug("avWeekAfter " + avWeekAfter);

    // check that after killing some server dayAvailability and weekAvailability decreases
    assertTrue(avDayBefore - avDayAfter > this.EPS);
    assertTrue(avWeekBefore - avWeekAfter > this.EPS);

    // server regions avDetDay:
    for(String region : regionsToKill) {
      if(!avDetDayAfter.containsKey(region)) {
        fail("Day detailed info must contain availability info about region '" + region + "', because it was closed");
      }
      assert (1.0 - avDetDayAfter.get(region).getAvailability() > this.EPS);
    }

    // server regions avWeekDay:
    for(String region : regionsToKill) {
      if(!avDetWeekAfter.containsKey(region)) {
        fail("Week detailed info must contain availability info about region '" + region + "', because it was closed");
      }
      assert (1.0 - avDetWeekAfter.get(region).getAvailability() > this.EPS);
    }

    // not server regions avDetDay:
    for (HRegionInfo info : allRegions) {
      String region = info.getRegionNameAsString();
      if (avDetDayAfter.containsKey(region) && !regionsToKill.contains(region)) {
        fail("Detailed availability map shouldn't contain such a key " + region + ", because this region wasn't killed");
      }
    }

    // not server regions avWeekDay:
    for (HRegionInfo info : allRegions) {
      String region = info.getRegionNameAsString();
      if (avDetWeekAfter.containsKey(region) && !regionsToKill.contains(region)) {
        fail("Detailed availability map shouldn't contain such a key " + region + ", because this region wasn't killed");
      }
    }
  }

  /**
   * Get a region server currently hosting a user region
   */
  private int getRegionServerId() throws IOException {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    for (int i = 0; i < SLAVES; i++) {
      // Find a region server with more than 2 regions.
      // 2 because of root and meta.
      if (cluster.getRegionServer(i).getRegionsAssignment().length > 2) {
        return i;
      }
    }
    return -1;
  }
}
