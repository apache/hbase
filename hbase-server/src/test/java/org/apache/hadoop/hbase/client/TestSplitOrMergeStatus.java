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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class, ClientTests.class})
public class TestSplitOrMergeStatus {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitOrMergeStatus.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSplitSwitch() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table t = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(t.getName());
    int originalCount = locator.getAllRegionLocations().size();

    Admin admin = TEST_UTIL.getAdmin();
    initSwitchStatus(admin);
    boolean result = admin.splitSwitch(false, false);
    assertTrue(result);
    try {
      admin.split(t.getName());
      fail();
    } catch (IOException e) {
      // expected
    }
    int count = admin.getRegions(tableName).size();
    assertTrue(originalCount == count);
    result = admin.splitSwitch(true, false);
    assertFalse(result);
    admin.split(t.getName());
    while ((count = admin.getRegions(tableName).size()) == originalCount) {
      Threads.sleep(1);
    }
    count = admin.getRegions(tableName).size();
    assertTrue(originalCount < count);
    admin.close();
  }


  @Ignore @Test
  public void testMergeSwitch() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table t = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    Admin admin = TEST_UTIL.getAdmin();
    int originalCount = admin.getRegions(tableName).size();
    initSwitchStatus(admin);
    admin.split(t.getName());
    int postSplitCount = -1;
    while ((postSplitCount = admin.getRegions(tableName).size()) == originalCount) {
      Threads.sleep(1);
    }
    assertTrue("originalCount=" + originalCount + ", newCount=" + postSplitCount,
        originalCount != postSplitCount);

    // Merge switch is off so merge should NOT succeed.
    boolean result = admin.mergeSwitch(false, false);
    assertTrue(result);
    List<RegionInfo> regions = admin.getRegions(t.getName());
    assertTrue(regions.size() > 1);
    Future<?> f = admin.mergeRegionsAsync(regions.get(0).getEncodedNameAsBytes(),
      regions.get(1).getEncodedNameAsBytes(), true);
    try {
      f.get(10, TimeUnit.SECONDS);
      fail("Should not get here.");
    } catch (ExecutionException ee) {
      // Expected.
    }
    int count = admin.getRegions(tableName).size();
    assertTrue("newCount=" + postSplitCount + ", count=" + count, postSplitCount == count);

    result = admin.mergeSwitch(true, false);
    regions = admin.getRegions(t.getName());
    assertFalse(result);
    f = admin.mergeRegionsAsync(regions.get(0).getEncodedNameAsBytes(),
      regions.get(1).getEncodedNameAsBytes(), true);
    f.get(10, TimeUnit.SECONDS);
    count = admin.getRegions(tableName).size();
    assertTrue((postSplitCount / 2 /*Merge*/) == count);
    admin.close();
  }

  @Test
  public void testMultiSwitches() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    assertTrue(admin.splitSwitch(false, false));
    assertTrue(admin.mergeSwitch(false, false));

    assertFalse(admin.isSplitEnabled());
    assertFalse(admin.isMergeEnabled());
    admin.close();
  }

  private void initSwitchStatus(Admin admin) throws IOException {
    if (!admin.isSplitEnabled()) {
      admin.splitSwitch(true, false);
    }
    if (!admin.isMergeEnabled()) {
      admin.mergeSwitch(true, false);
    }
    assertTrue(admin.isSplitEnabled());
    assertTrue(admin.isMergeEnabled());
  }
}
