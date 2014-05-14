package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.UnitTestRunner;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(MediumTests.class)
public class TestNumReadsWritesPerRs {
  private String[] FAMILIES = new String[] { "cf1", "cf2", "anotherCF" };
  private static final int MAX_VERSIONS = 1;
  private static final int NUM_COLS_PER_ROW = 15;
  private static final int NUM_FLUSHES = 3;
  private static final int NUM_REGIONS = 4;

  private final HBaseTestingUtility testUtil =
      new HBaseTestingUtility();
  private Map<String, Long> startingMetrics;

  @Before
  public void setUp() throws Exception {
    SchemaMetrics.setUseTableNameInTest(true);
    startingMetrics = SchemaMetrics.getMetricsSnapshot();
    testUtil.startMiniCluster();
  }

  @After
  public void tearDown() throws IOException {
    testUtil.shutdownMiniCluster();
    SchemaMetrics.validateMetricChanges(startingMetrics);
  }

  @Test
  public void testNumReadsAndWrites() throws IOException, InterruptedException{
    testUtil.createRandomTable("NumReadsWritesTest", Arrays.asList(FAMILIES),
        MAX_VERSIONS, NUM_COLS_PER_ROW, NUM_FLUSHES, NUM_REGIONS, 1000);
    List<RegionServerThread> threads = testUtil.getMiniHBaseCluster()
        .getLiveRegionServerThreads();
    HRegionServer rs = threads.get(0).getRegionServer();
    HRegionServer.runMetrics = false;
    long preNumRead = 0;
    long preNumWrite = 0;
    for (HRegion region : rs.getOnlineRegions()) {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (!regionInfo.isMetaRegion() && !regionInfo.isRootRegion()) {
        preNumRead += region.rowReadCnt.get();
        preNumWrite += region.rowUpdateCnt.get();
      }
    }
    HRegion[] regions = rs.getOnlineRegionsAsArray();
    for (int i = 0; i < regions.length; i++) {
      Get g = new Get(Bytes.toBytes("row" + i));
      regions[i].get(g, null);
    }
    long numRead = 0;
    long numWrite = 0;
    for (HRegion region : rs.getOnlineRegions()) {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (!regionInfo.isMetaRegion() && !regionInfo.isRootRegion()) {
        numRead += region.rowReadCnt.get();
        numWrite += region.rowUpdateCnt.get();
      }
    }
    assertEquals(regions.length - 2, numRead - preNumRead);
    assertEquals(0, numWrite - preNumWrite);
    HRegionServer.runMetrics = true;
  }
}
