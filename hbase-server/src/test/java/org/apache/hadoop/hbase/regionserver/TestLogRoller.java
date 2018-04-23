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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestLogRoller {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogRoller.class);

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int logRollPeriod = 20 * 1000;

  @Before
  public void setup() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.logroll.period", logRollPeriod);
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRemoveClosedWAL() throws Exception {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    Configuration conf = rs.getConfiguration();
    LogRoller logRoller = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getWalRoller();
    int originalSize = logRoller.getWalNeedsRoll().size();
    FSHLog wal1 = new FSHLog(rs.getWALFileSystem(), rs.getWALRootDir(),
        AbstractFSWALProvider.getWALDirectoryName(rs.getServerName().getServerName()), conf);
    logRoller.addWAL(wal1);
    FSHLog wal2 = new FSHLog(rs.getWALFileSystem(), rs.getWALRootDir(),
      AbstractFSWALProvider.getWALDirectoryName(rs.getServerName().getServerName()), conf);
    logRoller.addWAL(wal2);
    FSHLog wal3 = new FSHLog(rs.getWALFileSystem(), rs.getWALRootDir(),
      AbstractFSWALProvider.getWALDirectoryName(rs.getServerName().getServerName()), conf);
    logRoller.addWAL(wal3);

    assertEquals(originalSize + 3, logRoller.getWalNeedsRoll().size());
    assertTrue(logRoller.getWalNeedsRoll().containsKey(wal1));

    wal1.close();
    Thread.sleep(2 * logRollPeriod);

    assertEquals(originalSize + 2, logRoller.getWalNeedsRoll().size());
    assertFalse(logRoller.getWalNeedsRoll().containsKey(wal1));

    wal2.close();
    wal3.close();
    Thread.sleep(2 * logRollPeriod);

    assertEquals(originalSize, logRoller.getWalNeedsRoll().size());
  }
}
