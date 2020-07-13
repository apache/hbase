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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Category({RegionServerTests.class, MediumTests.class})
public class TestLogRoller {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogRoller.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int logRollPeriod = 20 * 1000;

  @Before
  public void setup() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.logroll.period", logRollPeriod);
    TEST_UTIL.startMiniCluster(1);
    TableName name = TableName.valueOf("Test");
    TEST_UTIL.createTable(name, Bytes.toBytes("cf"));
    TEST_UTIL.waitTableAvailable(name);
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

  /**
   * verify that each wal roll separately
   */
  @Test
  public void testRequestRollWithMultiWal() throws Exception {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    Configuration conf = rs.getConfiguration();
    LogRoller logRoller = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getWalRoller();
    FileSystem fs = rs.getFileSystem();
    // add multiple wal
    Map<FSHLog, Path> wals = new HashMap<>();
    for (int i = 1; i <= 3; i++) {
      FSHLog wal = new FSHLog(fs, rs.getWALRootDir(),
        AbstractFSWALProvider.getWALDirectoryName(rs.getServerName().getServerName()),
        AbstractFSWALProvider.getWALArchiveDirectoryName(conf, rs.getServerName().getServerName()),
        conf, null, true, "wal-test", "." + i);
      wal.init();
      wals.put(wal, wal.getCurrentFileName());
      logRoller.addWAL(wal);
      Thread.sleep(3000);
    }

    // request roll
    Iterator<Map.Entry<FSHLog, Path>> it = wals.entrySet().iterator();
    Map.Entry<FSHLog, Path> walEntry = it.next();
    walEntry.getKey().requestLogRoll();
    Thread.sleep(5000);
    assertNotEquals(walEntry.getValue(), walEntry.getKey().getCurrentFileName());
    walEntry.setValue(walEntry.getKey().getCurrentFileName());
    while (it.hasNext()) {
      walEntry = it.next();
      assertEquals(walEntry.getValue(), walEntry.getKey().getCurrentFileName());
    }

    // period roll
    Thread.sleep(logRollPeriod + 5000);
    for (Map.Entry<FSHLog, Path> entry : wals.entrySet()) {
      assertNotEquals(entry.getValue(), entry.getKey().getCurrentFileName());
      entry.getKey().close();
    }
  }
}
