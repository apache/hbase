/**
 * Copyright 2013 The Apache Software Foundation
 *
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

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Verify that the Online Config Changes on the HRegionServer side are actually
 * happening. We should add tests for important configurations which will be
 * changed online.
 */
public class TestRegionServerOnlineConfigChange extends TestCase {
  static final Log LOG =
          LogFactory.getLog(TestRegionServerOnlineConfigChange.class.getName());
  HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility();
  Configuration conf = null;

  HTable t1 = null;
  HRegionServer rs1 = null;
  byte[] r1name = null;
  HRegion r1 = null;

  final String table1Str = "table1";
  final String columnFamily1Str = "columnFamily1";
  final byte[] TABLE1 = Bytes.toBytes(table1Str);
  final byte[] COLUMN_FAMILY1 = Bytes.toBytes(columnFamily1Str);


  @Override
  public void setUp() throws Exception {
    conf = hbaseTestingUtility.getConfiguration();
    hbaseTestingUtility.startMiniCluster(1,1);
    t1 = hbaseTestingUtility.createTable(TABLE1, COLUMN_FAMILY1);
    HRegionInfo firstHRI = t1.getRegionsInfo().keySet().iterator().next();
    r1name = firstHRI.getRegionName();
    rs1 = hbaseTestingUtility.getRSWithRegion(r1name);
    r1 = rs1.getRegion(r1name);
  }

  @Override
  public void tearDown() throws Exception {
    hbaseTestingUtility.shutdownMiniCluster();
  }

  /**
   * Check if the number of compaction threads changes online
   * @throws IOException
   */
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
    HRegionServer.configurationManager.notifyAllObservers(conf);

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
  public void testCompactionConfigurationOnlineChange() throws IOException {
    String strPrefix = HConstants.HSTORE_COMPACTION_PREFIX;
    Store s = r1.getStore(COLUMN_FAMILY1);

    // Set the new compaction ratio to a different value.
    double newCompactionRatio =
            s.compactionManager.comConf.getCompactionRatio() + 0.1;
    conf.setFloat(strPrefix + "ratio", (float)newCompactionRatio);

    // Notify all the observers, which includes the Store object.
    HRegionServer.configurationManager.notifyAllObservers(conf);

    // Check if the compaction ratio got updated in the Compaction Configuration
    assertEquals(newCompactionRatio,
                 s.compactionManager.comConf.getCompactionRatio(),
                 0.00001);

    // Check if the off peak compaction ratio gets updated.
    double newOffPeakCompactionRatio =
            s.compactionManager.comConf.getCompactionRatioOffPeak() + 0.1;
    conf.setFloat(strPrefix + "ratio.offpeak",
            (float)newOffPeakCompactionRatio);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newOffPeakCompactionRatio,
                 s.compactionManager.comConf.getCompactionRatioOffPeak(),
                 0.00001);

    // Check if the throttle point gets updated.
    long newThrottlePoint = s.compactionManager.comConf.getThrottlePoint() + 10;
    conf.setLong("hbase.regionserver.thread.compaction.throttle",
                  newThrottlePoint);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newThrottlePoint,
                 s.compactionManager.comConf.getThrottlePoint());

    // Check if the minFilesToCompact gets updated.
    int newMinFilesToCompact =
            s.compactionManager.comConf.getMinFilesToCompact() + 1;
    conf.setLong(strPrefix + "min", newMinFilesToCompact);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newMinFilesToCompact,
                 s.compactionManager.comConf.getMinFilesToCompact());

    // Check if the maxFilesToCompact gets updated.
    int newMaxFilesToCompact =
            s.compactionManager.comConf.getMaxFilesToCompact() + 1;
    conf.setLong(strPrefix + "max", newMaxFilesToCompact);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newMaxFilesToCompact,
                 s.compactionManager.comConf.getMaxFilesToCompact());

    // Check if the Off peak start hour gets updated.
    int newOffPeakStartHour =
            (s.compactionManager.comConf.getOffPeakStartHour() + 1) % 24;
    conf.setLong("hbase.offpeak.start.hour", newOffPeakStartHour);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newOffPeakStartHour,
            s.compactionManager.comConf.getOffPeakStartHour());

    // Check if the Off peak end hour gets updated.
    int newOffPeakEndHour =
            (s.compactionManager.comConf.getOffPeakEndHour() + 1) % 24;
    conf.setLong("hbase.offpeak.end.hour", newOffPeakEndHour);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newOffPeakEndHour,
            s.compactionManager.comConf.getOffPeakEndHour());

    // Check if the minCompactSize gets updated.
    long newMinCompactSize =
            s.compactionManager.comConf.getMinCompactSize() + 1;
    conf.setLong(strPrefix + "min.size", newMinCompactSize);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newMinCompactSize,
                 s.compactionManager.comConf.getMinCompactSize());

    // Check if the maxCompactSize gets updated.
    long newMaxCompactSize =
            s.compactionManager.comConf.getMaxCompactSize() - 1;
    conf.setLong(strPrefix + "max.size", newMaxCompactSize);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newMaxCompactSize,
                 s.compactionManager.comConf.getMaxCompactSize());

    // Check if shouldExcludeBulk gets updated.
    boolean newShouldExcludeBulk =
            !s.compactionManager.comConf.shouldExcludeBulk();
    conf.setBoolean(strPrefix + "exclude.bulk", newShouldExcludeBulk);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newShouldExcludeBulk,
            s.compactionManager.comConf.shouldExcludeBulk());

    // Check if shouldDeleteExpired gets updated.
    boolean newShouldDeleteExpired =
            !s.compactionManager.comConf.shouldDeleteExpired();
    conf.setBoolean("hbase.store.delete.expired.storefile",
            newShouldDeleteExpired);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newShouldDeleteExpired,
            s.compactionManager.comConf.shouldDeleteExpired());

    // Check if majorCompactionPeriod gets updated.
    long newMajorCompactionPeriod =
            s.compactionManager.comConf.getMajorCompactionPeriod() + 10;
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, newMajorCompactionPeriod);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newMajorCompactionPeriod,
            s.compactionManager.comConf.getMajorCompactionPeriod());

    // Check if majorCompactionJitter gets updated.
    float newMajorCompactionJitter =
            s.compactionManager.comConf.getMajorCompactionJitter() + 0.02F;
    conf.setFloat("hbase.hregion.majorcompaction.jitter",
                  newMajorCompactionJitter);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(newMajorCompactionJitter,
            s.compactionManager.comConf.getMajorCompactionJitter(), 0.00001);
  }

  /**
   * Check if quorum read settings change online properly
   */
  public void testQuorumReadConfigurationChange() {
    int threads = conf.getInt(
        HConstants.HDFS_QUORUM_READ_THREADS_MAX, 0);
    int timeout = conf.getInt(
        HConstants.HDFS_QUORUM_READ_TIMEOUT_MILLIS, 0);
    threads += 1;
    timeout += 1;
    conf.setInt(HConstants.HDFS_QUORUM_READ_THREADS_MAX, threads);
    conf.setInt(HConstants.HDFS_QUORUM_READ_TIMEOUT_MILLIS, timeout);
    HRegionServer.configurationManager.notifyAllObservers(conf);
    assertEquals(threads,
        conf.getInt(HConstants.HDFS_QUORUM_READ_THREADS_MAX, 0));
    assertEquals(timeout,
        conf.getInt(HConstants.HDFS_QUORUM_READ_TIMEOUT_MILLIS, 0));
  }
}

