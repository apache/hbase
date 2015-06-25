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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Verify that the Online config Changes on the HRegionServer side are actually
 * happening. We should add tests for important configurations which will be
 * changed online.
 */

@Category({MediumTests.class})
public class TestRegionServerOnlineConfigChange {
  private static final Log LOG =
          LogFactory.getLog(TestRegionServerOnlineConfigChange.class.getName());
  private static HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility();
  private static Configuration conf = null;

  private static Table t1 = null;
  private static HRegionServer rs1 = null;
  private static byte[] r1name = null;
  private static Region r1 = null;

  private final static String table1Str = "table1";
  private final static String columnFamily1Str = "columnFamily1";
  private final static TableName TABLE1 = TableName.valueOf(table1Str);
  private final static byte[] COLUMN_FAMILY1 = Bytes.toBytes(columnFamily1Str);


  @BeforeClass
  public static void setUp() throws Exception {
    conf = hbaseTestingUtility.getConfiguration();
    hbaseTestingUtility.startMiniCluster(1,1);
    t1 = hbaseTestingUtility.createTable(TABLE1, COLUMN_FAMILY1);
    try (RegionLocator locator = hbaseTestingUtility.getConnection().getRegionLocator(TABLE1)) {
      HRegionInfo firstHRI = locator.getAllRegionLocations().get(0).getRegionInfo();
      r1name = firstHRI.getRegionName();
      rs1 = hbaseTestingUtility.getHBaseCluster().getRegionServer(
          hbaseTestingUtility.getHBaseCluster().getServerWith(r1name));
      r1 = rs1.getRegion(r1name);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    hbaseTestingUtility.shutdownMiniCluster();
  }

  /**
   * Check if the number of compaction threads changes online
   * @throws IOException
   */
  @Test
  public void testNumCompactionThreadsOnlineChange() throws IOException {
    assertTrue(rs1.compactSplitThread != null);
    int newNumSmallThreads =
            rs1.compactSplitThread.getSmallCompactionThreadNum() + 1;
    int newNumLargeThreads =
            rs1.compactSplitThread.getLargeCompactionThreadNum() + 1;

    conf.setInt("hbase.regionserver.thread.compaction.small",
            newNumSmallThreads);
    conf.setInt("hbase.regionserver.thread.compaction.large",
            newNumLargeThreads);
    rs1.getConfigurationManager().notifyAllObservers(conf);

    assertEquals(newNumSmallThreads,
                  rs1.compactSplitThread.getSmallCompactionThreadNum());
    assertEquals(newNumLargeThreads,
                  rs1.compactSplitThread.getLargeCompactionThreadNum());
  }

  /**
   * Test that the configurations in the CompactionConfiguration class change
   * properly.
   *
   * @throws IOException
   */
  @Test
  public void testCompactionConfigurationOnlineChange() throws IOException {
    String strPrefix = "hbase.hstore.compaction.";
    Store s = r1.getStore(COLUMN_FAMILY1);
    if (!(s instanceof HStore)) {
      LOG.error("Can't test the compaction configuration of HStore class. "
          + "Got a different implementation other than HStore");
      return;
    }
    HStore hstore = (HStore)s;

    // Set the new compaction ratio to a different value.
    double newCompactionRatio = 
            hstore.getStoreEngine().getCompactionPolicy().getConf().getCompactionRatio() + 0.1;
    conf.setFloat(strPrefix + "ratio", (float)newCompactionRatio);

    // Notify all the observers, which includes the Store object.
    rs1.getConfigurationManager().notifyAllObservers(conf);

    // Check if the compaction ratio got updated in the Compaction Configuration
    assertEquals(newCompactionRatio,
                 hstore.getStoreEngine().getCompactionPolicy().getConf().getCompactionRatio(),
                 0.00001);

    // Check if the off peak compaction ratio gets updated.
    double newOffPeakCompactionRatio =
        hstore.getStoreEngine().getCompactionPolicy().getConf().getCompactionRatioOffPeak() + 0.1;
    conf.setFloat(strPrefix + "ratio.offpeak",
            (float)newOffPeakCompactionRatio);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newOffPeakCompactionRatio,
        hstore.getStoreEngine().getCompactionPolicy().getConf().getCompactionRatioOffPeak(),
                 0.00001);

    // Check if the throttle point gets updated.
    long newThrottlePoint =
        hstore.getStoreEngine().getCompactionPolicy().getConf().getThrottlePoint() + 10;
    conf.setLong("hbase.regionserver.thread.compaction.throttle",
                  newThrottlePoint);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newThrottlePoint,
        hstore.getStoreEngine().getCompactionPolicy().getConf().getThrottlePoint());

    // Check if the minFilesToCompact gets updated.
    int newMinFilesToCompact =
            hstore.getStoreEngine().getCompactionPolicy().getConf().getMinFilesToCompact() + 1;
    conf.setLong(strPrefix + "min", newMinFilesToCompact);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newMinFilesToCompact,
        hstore.getStoreEngine().getCompactionPolicy().getConf().getMinFilesToCompact());

    // Check if the maxFilesToCompact gets updated.
    int newMaxFilesToCompact =
            hstore.getStoreEngine().getCompactionPolicy().getConf().getMaxFilesToCompact() + 1;
    conf.setLong(strPrefix + "max", newMaxFilesToCompact);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newMaxFilesToCompact,
        hstore.getStoreEngine().getCompactionPolicy().getConf().getMaxFilesToCompact());

    // Check OffPeak hours is updated in an online fashion.
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_START_HOUR, 6);
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_END_HOUR, 7);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertFalse(hstore.getOffPeakHours().isOffPeakHour(4));

    // Check if the minCompactSize gets updated.
    long newMinCompactSize =
            hstore.getStoreEngine().getCompactionPolicy().getConf().getMinCompactSize() + 1;
    conf.setLong(strPrefix + "min.size", newMinCompactSize);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newMinCompactSize,
                 hstore.getStoreEngine().getCompactionPolicy().getConf().getMinCompactSize());

    // Check if the maxCompactSize gets updated.
    long newMaxCompactSize =
            hstore.getStoreEngine().getCompactionPolicy().getConf().getMaxCompactSize() - 1;
    conf.setLong(strPrefix + "max.size", newMaxCompactSize);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newMaxCompactSize,
                 hstore.getStoreEngine().getCompactionPolicy().getConf().getMaxCompactSize());

    // Check if majorCompactionPeriod gets updated.
    long newMajorCompactionPeriod =
            hstore.getStoreEngine().getCompactionPolicy().getConf().getMajorCompactionPeriod() + 10;
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, newMajorCompactionPeriod);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newMajorCompactionPeriod,
            hstore.getStoreEngine().getCompactionPolicy().getConf().getMajorCompactionPeriod());

    // Check if majorCompactionJitter gets updated.
    float newMajorCompactionJitter =
        hstore.getStoreEngine().getCompactionPolicy().getConf().getMajorCompactionJitter() + 0.02F;
    conf.setFloat("hbase.hregion.majorcompaction.jitter",
                  newMajorCompactionJitter);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newMajorCompactionJitter,
      hstore.getStoreEngine().getCompactionPolicy().getConf().getMajorCompactionJitter(), 0.00001);
  }
}