/**
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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.junit.rules.TestName;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

@Category({MasterTests.class, MediumTests.class})
public class TestCatalogJanitorInMemoryStates {
  private static final Log LOG = LogFactory.getLog(TestCatalogJanitorInMemoryStates.class);
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
     withTimeout(this.getClass()).withLookingForStuckThread(true).build();
  @Rule public final TestName name = new TestName();
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster(1);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test clearing a split parent from memory.
   */
  @Test(timeout = 180000)
  public void testInMemoryParentCleanup() throws IOException, InterruptedException {
    final AssignmentManager am = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    final ServerManager sm = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();
    final CatalogJanitor janitor = TEST_UTIL.getHBaseCluster().getMaster().catalogJanitorChore;

    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.enableCatalogJanitor(false);

    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table t = TEST_UTIL.createTable(tableName, FAMILY);
    int rowCount = TEST_UTIL.loadTable(t, FAMILY, false);

    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
    List<HRegionLocation> allRegionLocations = locator.getAllRegionLocations();

    // We need to create a valid split with daughter regions
    HRegionLocation parent = allRegionLocations.get(0);
    List<HRegionLocation> daughters = splitRegion(parent.getRegionInfo());
    LOG.info("Parent region: " + parent);
    LOG.info("Daughter regions: " + daughters);
    assertNotNull("Should have found daughter regions for " + parent, daughters);

    assertTrue("Parent region should exist in RegionStates",
        am.getRegionStates().isRegionInRegionStates(parent.getRegionInfo()));
    assertTrue("Parent region should exist in ServerManager",
        sm.isRegionInServerManagerStates(parent.getRegionInfo()));

    // clean the parent
    Result r = MetaMockingUtil.getMetaTableRowResult(parent.getRegionInfo(), null,
        daughters.get(0).getRegionInfo(), daughters.get(1).getRegionInfo());
    // We need to wait a little before and after we remove the parent
    Thread.sleep(5000);
    janitor.cleanParent(parent.getRegionInfo(), r);
    Thread.sleep(5000);
    assertFalse("Parent region should have been removed from RegionStates",
        am.getRegionStates().isRegionInRegionStates(parent.getRegionInfo()));
    assertFalse("Parent region should have been removed from ServerManager",
        sm.isRegionInServerManagerStates(parent.getRegionInfo()));

  }

  /*
 * Splits a region
 * @param t Region to split.
 * @return List of region locations
 * @throws IOException, InterruptedException
 */
  private List<HRegionLocation> splitRegion(final HRegionInfo r)
      throws IOException, InterruptedException {
    List<HRegionLocation> locations = new ArrayList<>();
    // Split this table in two.
    Admin admin = TEST_UTIL.getHBaseAdmin();
    Connection connection = TEST_UTIL.getConnection();
    admin.splitRegion(r.getEncodedNameAsBytes());
    admin.close();
    PairOfSameType<HRegionInfo> regions = waitOnDaughters(r);
    if (regions != null) {
      try (RegionLocator rl = connection.getRegionLocator(r.getTable())) {
        locations.add(rl.getRegionLocation(regions.getFirst().getEncodedNameAsBytes()));
        locations.add(rl.getRegionLocation(regions.getSecond().getEncodedNameAsBytes()));
      }
      return locations;
    }
    return locations;
  }

  /*
   * Wait on region split. May return because we waited long enough on the split
   * and it didn't happen.  Caller should check.
   * @param r
   * @return Daughter regions; caller needs to check table actually split.
   */
  private PairOfSameType<HRegionInfo> waitOnDaughters(final HRegionInfo r)
      throws IOException {
    long start = System.currentTimeMillis();
    PairOfSameType<HRegionInfo> pair = null;
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
         Table metaTable = conn.getTable(TableName.META_TABLE_NAME)) {
      Result result = null;
      HRegionInfo region = null;
      while ((System.currentTimeMillis() - start) < 60000) {
        result = metaTable.get(new Get(r.getRegionName()));
        if (result == null) {
          break;
        }
        region = MetaTableAccessor.getHRegionInfo(result);
        if (region.isSplitParent()) {
          LOG.debug(region.toString() + " IS a parent!");
          pair = MetaTableAccessor.getDaughterRegions(result);
          break;
        }
        Threads.sleep(100);
      }
      if (pair == null || pair.getFirst() == null || pair.getSecond() == null) {
        throw new IOException("Failed to get daughters, for parent region: " + r);
      }
      return pair;
    }
  }
}
