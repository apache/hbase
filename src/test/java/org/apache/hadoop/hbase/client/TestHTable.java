package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHTable {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  private static int SLAVES = 3;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(HBaseTestingUtility.FS_TYPE_KEY,
        HBaseTestingUtility.FS_TYPE_LFS);

    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCachedHRegionLocations() throws Exception {
    final int NUM_REGIONS = 20;
    byte[] tableName = Bytes.toBytes("testCachedHRegionLocations");
    HTable table = TEST_UTIL.createTable(tableName, new byte[][]{FAMILY},
      3, Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);

    NavigableMap<HRegionInfo, HServerAddress> allRegionsInfoMap =  table.getRegionsInfo();
    Collection<HRegionLocation> regionLocations = table.getCachedHRegionLocations(false);
    Collection<HRegionLocation> regionLocations2 = table.getCachedHRegionLocations(true);
    assertEquals(regionLocations.size(), regionLocations2.size());

    // Make sure allRegions and regionLocations containing the same information
    verifyOnlineRegionsAndRegionLocations(allRegionsInfoMap, regionLocations, NUM_REGIONS);

    HRegionServer killedServer =
        TEST_UTIL.getRSForFirstRegionInTable(tableName);
    Collection<HRegion> regionsToBeMoved = killedServer.getOnlineRegions();
    List<HRegionInfo> testRegionInfos = new ArrayList<>();

    LOG.info(killedServer + " at "
        + killedServer.getServerInfo().getServerAddress()
        + " is going to be killed");

    for (HRegion region : regionsToBeMoved) {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (Bytes.equals(regionInfo.getTableDesc().getName(), tableName)) {
        testRegionInfos.add(regionInfo);
      }
    }
    Assert.assertTrue("testRegionInfos should be non-empty",
        testRegionInfos.size() > 0);

    HServerAddress killedAddress = killedServer.getHServerInfo().getServerAddress();
    killedServer.stop("Testing"); // Stop this region server

    TEST_UTIL.waitForOnlineRegionsToBeAssigned(NUM_REGIONS);
    TEST_UTIL.waitForTableConsistent();

    Assert.assertEquals("Killed server online region count", 0,
        killedServer.getOnlineRegions().size());

    regionLocations = table.getCachedHRegionLocations(false);
    // Make sure allRegions and regionLocations containing the same information
    verifyOnlineRegionsAndRegionLocations(allRegionsInfoMap, regionLocations,
        NUM_REGIONS);
    // Also, it has the stale information about the region location
    for (HRegionInfo info : testRegionInfos) {
      Assert.assertTrue(allRegionsInfoMap.get(info).equals(killedAddress));
    }

    // Update the allRegionsInfoMap and regionLocations by fetching the META table again
    allRegionsInfoMap = table.getRegionsInfo();
    regionLocations = table.getCachedHRegionLocations(true);
    // Make sure allRegions and regionLocations containing the same information
    verifyOnlineRegionsAndRegionLocations(allRegionsInfoMap, regionLocations,
        NUM_REGIONS);

    Assert.assertEquals("killedServer.getOnlineRegions.size", 0,
        killedServer.getOnlineRegions().size());
    // Verify the new allRegionsInfoMap has been updated without the stale information
    for (HRegionInfo info : testRegionInfos) {
      Assert.assertFalse("Region " + info + " is still in killed address "
          + killedAddress, allRegionsInfoMap.get(info).equals(killedAddress));
    }
  }

  private void verifyOnlineRegionsAndRegionLocations(
      NavigableMap<HRegionInfo, HServerAddress> allRegions,
      Collection<HRegionLocation> regionLocation, int numRegions) {
    Assert.assertEquals(numRegions, allRegions.size());
    Assert.assertEquals(numRegions, regionLocation.size());
    for (HRegionLocation location : regionLocation) {
      HRegionInfo regionInfo = location.getRegionInfo();
      HServerAddress address = location.getServerAddress();
      HServerAddress address2 = allRegions.get(regionInfo);
      Assert.assertNotNull("Address of " + location + " in allRegions",
          address2);
      Assert.assertEquals("Address of " + regionInfo + " in allRegions",
          address, address2);
    }
  }

  @Test
  public void testHTableMultiPutThreadPool() throws Exception {
    byte [] TABLE = Bytes.toBytes("testHTableMultiputThreadPool");
    final int NUM_REGIONS = 10;
    HTable ht = TEST_UTIL.createTable(TABLE, new byte[][]{FAMILY},
        3, Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
    byte [][] ROWS = ht.getStartKeys();
    ThreadPoolExecutor pool = (ThreadPoolExecutor)HTable.multiActionThreadPool;
    int previousPoolSize = pool.getPoolSize();
    int previousLargestPoolSize = pool.getLargestPoolSize();
    long previousCompletedTaskCount = pool.getCompletedTaskCount();

    for (int i = 0; i < NUM_REGIONS; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      ht.flushCommits();
    }

    // verify that HTable does NOT use thread pool for single put requests
    assertEquals(1, pool.getCorePoolSize());
    assertEquals(previousPoolSize, pool.getPoolSize());
    assertEquals(previousLargestPoolSize, pool.getLargestPoolSize());
    assertEquals(previousCompletedTaskCount, pool.getCompletedTaskCount());

    ArrayList<Put> multiput = new ArrayList<Put>();
    for (int i = 0; i < NUM_REGIONS; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIER, VALUE);
      multiput.add(put);
    }
    ht.put(multiput);
    ht.flushCommits();

    // verify that HTable does use thread pool for multi put requests.
    assertTrue((SLAVES >= pool.getLargestPoolSize())
      && (pool.getLargestPoolSize() >= previousLargestPoolSize));
    assertEquals(SLAVES,
        (pool.getCompletedTaskCount() - previousCompletedTaskCount));
  }

  /**
   * Test that when a table could not be found, a TableNotFoundException is
   * thrown.
   *
   * @throws Exception
   */
  @Test(expected = TableNotFoundException.class)
  public void testTableNotFound() throws Exception {
    // Let's search for a non-existing table, and get a TableNotFoundException.
    HConnection connection =
      HConnectionManager.getConnection(TEST_UTIL.getConfiguration());
    connection.getRegionLocation(new StringBytes("foo"),
                                 Bytes.toBytes("r1"), false);
  }
}
