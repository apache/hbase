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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.JMXListener;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that the Online config Changes on the HRegionServer side are actually
 * happening. We should add tests for important configurations which will be
 * changed online.
 */

@Category({MediumTests.class})
public class TestRegionServerOnlineConfigChange {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerOnlineConfigChange.class);

  private static final Logger LOG =
          LoggerFactory.getLogger(TestRegionServerOnlineConfigChange.class.getName());
  private static final long WAIT_TIMEOUT = TimeUnit.MINUTES.toMillis(2);
  private static HBaseTestingUtil hbaseTestingUtility = new HBaseTestingUtil();
  private static Configuration conf = null;

  private static Table t1 = null;
  private static HRegionServer rs1 = null;
  private static HMaster hMaster = null;
  private static byte[] r1name = null;
  private static Region r1 = null;

  private final static String table1Str = "table1";
  private final static String columnFamily1Str = "columnFamily1";
  private final static TableName TABLE1 = TableName.valueOf(table1Str);
  private final static byte[] COLUMN_FAMILY1 = Bytes.toBytes(columnFamily1Str);
  private final static long MAX_FILE_SIZE = 20 * 1024 * 1024L;


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = hbaseTestingUtility.getConfiguration();
    hbaseTestingUtility.startMiniCluster(2);
    t1 = hbaseTestingUtility.createTable(
      TableDescriptorBuilder.newBuilder(TABLE1).setMaxFileSize(MAX_FILE_SIZE).build(),
      new byte[][] { COLUMN_FAMILY1 }, conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    hbaseTestingUtility.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    try (RegionLocator locator = hbaseTestingUtility.getConnection().getRegionLocator(TABLE1)) {
      RegionInfo firstHRI = locator.getAllRegionLocations().get(0).getRegion();
      r1name = firstHRI.getRegionName();
      rs1 = hbaseTestingUtility.getHBaseCluster().getRegionServer(
        hbaseTestingUtility.getHBaseCluster().getServerWith(r1name));
      r1 = rs1.getRegion(r1name);
      hMaster = hbaseTestingUtility.getHBaseCluster().getMaster();
    }
  }

  /**
   * Check if the number of compaction threads changes online
   */
  @Test
  public void testNumCompactionThreadsOnlineChange() {
    assertNotNull(rs1.getCompactSplitThread());
    int newNumSmallThreads =
      rs1.getCompactSplitThread().getSmallCompactionThreadNum() + 1;
    int newNumLargeThreads =
      rs1.getCompactSplitThread().getLargeCompactionThreadNum() + 1;

    conf.setInt("hbase.regionserver.thread.compaction.small",
      newNumSmallThreads);
    conf.setInt("hbase.regionserver.thread.compaction.large",
      newNumLargeThreads);
    rs1.getConfigurationManager().notifyAllObservers(conf);

    assertEquals(newNumSmallThreads,
      rs1.getCompactSplitThread().getSmallCompactionThreadNum());
    assertEquals(newNumLargeThreads,
      rs1.getCompactSplitThread().getLargeCompactionThreadNum());
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
    // Check if the offPeakMaxCompactSize gets updated.
    long newOffpeakMaxCompactSize =
            hstore.getStoreEngine().getCompactionPolicy().getConf().getOffPeakMaxCompactSize() - 1;
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_SIZE_OFFPEAK_KEY,
      newOffpeakMaxCompactSize);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(newOffpeakMaxCompactSize,
                 hstore.getStoreEngine().getCompactionPolicy().getConf().getOffPeakMaxCompactSize());

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

  @Test
  public void removeClosedRegionFromConfigurationManager() throws Exception {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      Admin admin = connection.getAdmin();
      assertTrue("The open region doesn't register as a ConfigurationObserver",
        rs1.getConfigurationManager().containsObserver(r1));
      admin.move(r1name);
      hbaseTestingUtility.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          return rs1.getOnlineRegion(r1name) == null;
        }
      });
      assertFalse("The closed region is not removed from ConfigurationManager",
        rs1.getConfigurationManager().containsObserver(r1));
      admin.move(r1name, rs1.getServerName());
      hbaseTestingUtility.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          return rs1.getOnlineRegion(r1name) != null;
        }
      });
    }
  }

  @Test
  public void testStoreConfigurationOnlineChange() {
    rs1.getConfigurationManager().notifyAllObservers(conf);
    long actualMaxFileSize = r1.getStore(COLUMN_FAMILY1).getReadOnlyConfiguration()
        .getLong(TableDescriptorBuilder.MAX_FILESIZE, -1);
    assertEquals(MAX_FILE_SIZE, actualMaxFileSize);
  }

  @Test
  public void testCoprocessorConfigurationOnlineChange() {
    assertNull(rs1.getRegionServerCoprocessorHost().findCoprocessor(JMXListener.class.getName()));
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, JMXListener.class.getName());
    rs1.getConfigurationManager().notifyAllObservers(conf);
    assertNotNull(
      rs1.getRegionServerCoprocessorHost().findCoprocessor(JMXListener.class.getName()));
  }

  @Test
  public void testCoprocessorConfigurationOnlineChangeOnMaster() {
    assertNull(hMaster.getMasterCoprocessorHost().findCoprocessor(JMXListener.class.getName()));
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, JMXListener.class.getName());
    assertFalse(hMaster.isInMaintenanceMode());
    hMaster.getConfigurationManager().notifyAllObservers(conf);
    assertNotNull(
      hMaster.getMasterCoprocessorHost().findCoprocessor(JMXListener.class.getName()));
  }

}
