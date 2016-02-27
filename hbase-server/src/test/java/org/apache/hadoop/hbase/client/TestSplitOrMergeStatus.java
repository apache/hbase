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
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MediumTests.class, ClientTests.class})
public class TestSplitOrMergeStatus {

  private static final Log LOG = LogFactory.getLog(TestSplitOrMergeStatus.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSplitSwitch() throws Exception {
    TableName name = TableName.valueOf("testSplitSwitch");
    Table t = TEST_UTIL.createTable(name, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(t.getName());
    int orignalCount = locator.getAllRegionLocations().size();

    Admin admin = TEST_UTIL.getAdmin();
    initSwitchStatus(admin);
    boolean[] results = admin.setSplitOrMergeEnabled(false, false, Admin.MasterSwitchType.SPLIT);
    assertEquals(results.length, 1);
    assertTrue(results[0]);
    admin.split(t.getName());
    int count = waitOnSplitOrMerge(t).size();
    assertTrue(orignalCount == count);

    results = admin.setSplitOrMergeEnabled(true, false, Admin.MasterSwitchType.SPLIT);
    assertEquals(results.length, 1);
    assertFalse(results[0]);
    admin.split(t.getName());
    count = waitOnSplitOrMerge(t).size();
    assertTrue(orignalCount<count);
    admin.close();
  }


  @Test
  public void testMergeSwitch() throws Exception {
    TableName name = TableName.valueOf("testMergeSwitch");
    Table t = TEST_UTIL.createTable(name, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(t.getName());

    Admin admin = TEST_UTIL.getAdmin();
    initSwitchStatus(admin);
    admin.split(t.getName());
    waitOnSplitOrMerge(t); //Split the table to ensure we have two regions at least.

    waitForMergable(admin, name);
    int orignalCount = locator.getAllRegionLocations().size();
    boolean[] results = admin.setSplitOrMergeEnabled(false, false, Admin.MasterSwitchType.MERGE);
    assertEquals(results.length, 1);
    assertTrue(results[0]);
    List<HRegionInfo> regions = admin.getTableRegions(t.getName());
    assertTrue(regions.size() > 1);
    admin.mergeRegions(regions.get(0).getEncodedNameAsBytes(),
      regions.get(1).getEncodedNameAsBytes(), true);
    int count = waitOnSplitOrMerge(t).size();
    assertTrue(orignalCount == count);

    waitForMergable(admin, name);
    results = admin.setSplitOrMergeEnabled(true, false, Admin.MasterSwitchType.MERGE);
    assertEquals(results.length, 1);
    assertFalse(results[0]);
    admin.mergeRegions(regions.get(0).getEncodedNameAsBytes(),
      regions.get(1).getEncodedNameAsBytes(), true);
    count = waitOnSplitOrMerge(t).size();
    assertTrue(orignalCount>count);
    admin.close();
  }

  @Test
  public void testMultiSwitches() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    boolean[] switches = admin.setSplitOrMergeEnabled(false, false,
      Admin.MasterSwitchType.SPLIT, Admin.MasterSwitchType.MERGE);
    for (boolean s : switches){
      assertTrue(s);
    }
    assertFalse(admin.isSplitOrMergeEnabled(Admin.MasterSwitchType.SPLIT));
    assertFalse(admin.isSplitOrMergeEnabled(Admin.MasterSwitchType.MERGE));
    admin.close();
  }

  private void initSwitchStatus(Admin admin) throws IOException {
    if (!admin.isSplitOrMergeEnabled(Admin.MasterSwitchType.SPLIT)) {
      admin.setSplitOrMergeEnabled(true, false, Admin.MasterSwitchType.SPLIT);
    }
    if (!admin.isSplitOrMergeEnabled(Admin.MasterSwitchType.MERGE)) {
      admin.setSplitOrMergeEnabled(true, false, Admin.MasterSwitchType.MERGE);
    }
    assertTrue(admin.isSplitOrMergeEnabled(Admin.MasterSwitchType.SPLIT));
    assertTrue(admin.isSplitOrMergeEnabled(Admin.MasterSwitchType.MERGE));
  }

  private void waitForMergable(Admin admin, TableName t) throws InterruptedException, IOException {
    // Wait for the Regions to be mergeable
    MiniHBaseCluster miniCluster = TEST_UTIL.getMiniHBaseCluster();
    int mergeable = 0;
    while (mergeable < 2) {
      Thread.sleep(100);
      admin.majorCompact(t);
      mergeable = 0;
      for (JVMClusterUtil.RegionServerThread regionThread: miniCluster.getRegionServerThreads()) {
        for (Region region: regionThread.getRegionServer().getOnlineRegions(t)) {
          mergeable += ((HRegion)region).isMergeable() ? 1 : 0;
        }
      }
    }
  }

  /*
   * Wait on table split.  May return because we waited long enough on the split
   * and it didn't happen.  Caller should check.
   * @param t
   * @return Map of table regions; caller needs to check table actually split.
   */
  private List<HRegionLocation> waitOnSplitOrMerge(final Table t)
    throws IOException {
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(t.getName())) {
      List<HRegionLocation> regions = locator.getAllRegionLocations();
      int originalCount = regions.size();
      for (int i = 0; i < TEST_UTIL.getConfiguration().getInt("hbase.test.retries", 10); i++) {
        Thread.currentThread();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        regions = locator.getAllRegionLocations();
        if (regions.size() !=  originalCount)
          break;
      }
      return regions;
    }
  }

}
