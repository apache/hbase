package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.TagRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;

@RunWith(TagRunner.class)
public class TestRegionPlacementDestructive extends RegionPlacementTestBase {

  // Need more RS's as twp of the them will be killed.
  private final static int SLAVES = 6;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    setUpCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void cleanUpTables() throws IOException, InterruptedException {
    cleanUp();
  }

  // Test Case: Kill the region server with META region and verify the
  // region movements and region on primary region server are expected.
  // And
  // Test Case: Kill the region sever with ROOT and verify the
  // region movements and region on primary region server are expected.
  @Test(timeout = 360000)
  public void testRegionPlacementKillMeta() throws Exception {
    // Create a table with REGION_NUM regions.
    final String tableName = "testRegionPlacementKillMeta";
    createTable(tableName, REGION_NUM);

    AssignmentPlan currentPlan = rp.getExistingAssignmentPlan();

    waitOnStableRegionMovement();

    // Check to make sure the new table is on it's primaries.
    verifyRegionOnPrimaryRS(REGION_NUM);

    resetLastOpenedRegionCount();
    resetLastRegionOnPrimary();

    HRegionServer meta = this.getRegionServerWithMETA();

    // Get the expected the num of the regions on the its primary region server
    Collection<HRegion> regionOnMetaRegionServer = meta.getOnlineRegions();
    int expectedRegionOnPrimaryRS = lastRegionOnPrimaryRSCount
        - regionOnMetaRegionServer.size()
        + META_REGION_OVERHEAD;

    verifyKillRegionServerWithMetaOrRoot(meta, expectedRegionOnPrimaryRS);
    RegionPlacement.printAssignmentPlan(currentPlan);

    waitOnTable(tableName);
    waitOnStableRegionMovement();

    // Start Root RS Kill Test
    HRegionServer root = this.getRegionServerWithROOT();
    // Get the expected the num of the regions on the its primary region server
    Collection<HRegion> regionOnRootRegionServer = root.getOnlineRegions();
    expectedRegionOnPrimaryRS = lastRegionOnPrimaryRSCount
        - regionOnRootRegionServer.size()
        + ROOT_REGION_OVERHEAD;

    // Adjust the number by removing the regions just moved to the ROOT region server
    for (HRegion region : regionOnRootRegionServer) {
      if (regionOnMetaRegionServer.contains(region))
        expectedRegionOnPrimaryRS++;
    }

    verifyKillRegionServerWithMetaOrRoot(root, expectedRegionOnPrimaryRS);
  }
}
