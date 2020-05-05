/*
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

import static org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MasterTests.class, LargeTests.class})
public class TestMetaFixer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaFixer.class);
  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void deleteRegion(MasterServices services, RegionInfo ri) throws IOException {
    MetaTableAccessor.deleteRegionInfo(TEST_UTIL.getConnection(), ri);
    // Delete it from Master context too else it sticks around.
    services.getAssignmentManager().getRegionStates().deleteRegion(ri);
  }

  @Test
  public void testPlugsHoles() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tn, HConstants.CATALOG_FAMILY);
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    MasterServices services = TEST_UTIL.getHBaseCluster().getMaster();
    int initialSize = services.getAssignmentManager().getRegionStates().getRegionStates().size();
    services.getCatalogJanitor().scan();
    CatalogJanitor.Report report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.isEmpty());
    int originalCount = ris.size();
    // Remove first, last and middle region. See if hole gets plugged. Table has 26 regions.
    deleteRegion(services, ris.get(ris.size() -1));
    deleteRegion(services, ris.get(3));
    deleteRegion(services, ris.get(0));
    assertEquals(initialSize - 3,
        services.getAssignmentManager().getRegionStates().getRegionStates().size());
    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    assertEquals(report.toString(), 3, report.getHoles().size());
    MetaFixer fixer = new MetaFixer(services);
    fixer.fixHoles(report);
    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.toString(), report.isEmpty());
    assertEquals(initialSize,
        services.getAssignmentManager().getRegionStates().getRegionStates().size());

    // wait for RITs to settle -- those are the fixed regions being assigned -- or until the
    // watchdog TestRule terminates the test.
    await(50, () -> isNotEmpty(services.getAssignmentManager().getRegionsInTransition()));

    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertEquals(originalCount, ris.size());
  }

  /**
   * Just make sure running fixMeta does right thing for the case
   * of a single-region Table where the region gets dropped.
   * There is nothing much we can do. We can't restore what
   * we don't know about (at least from a read of hbase:meta).
   */
  @Test
  public void testOneRegionTable() throws IOException {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createTable(tn, HConstants.CATALOG_FAMILY);
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    MasterServices services = TEST_UTIL.getHBaseCluster().getMaster();
    services.getCatalogJanitor().scan();
    deleteRegion(services, ris.get(0));
    services.getCatalogJanitor().scan();
    CatalogJanitor.Report report = services.getCatalogJanitor().getLastReport();
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertTrue(ris.isEmpty());
    MetaFixer fixer = new MetaFixer(services);
    fixer.fixHoles(report);
    report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.isEmpty());
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertEquals(0, ris.size());
  }

  private static void makeOverlap(MasterServices services, RegionInfo a, RegionInfo b)
      throws IOException {
    RegionInfo overlapRegion = RegionInfoBuilder.newBuilder(a.getTable()).
        setStartKey(a.getStartKey()).
        setEndKey(b.getEndKey()).
        build();
    MetaTableAccessor.putsToMetaTable(services.getConnection(),
        Collections.singletonList(MetaTableAccessor.makePutFromRegionInfo(overlapRegion,
            System.currentTimeMillis())));
    // TODO: Add checks at assign time to PREVENT being able to assign over existing assign.
    services.getAssignmentManager().assign(overlapRegion);
  }

  @Test
  public void testOverlap() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    Table t = TEST_UTIL.createMultiRegionTable(tn, HConstants.CATALOG_FAMILY);
    TEST_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertTrue(ris.size() > 5);
    HMaster services = TEST_UTIL.getHBaseCluster().getMaster();
    HbckChore hbckChore = services.getHbckChore();
    services.getCatalogJanitor().scan();
    CatalogJanitor.Report report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.isEmpty());
    // Make a simple overlap spanning second and third region.
    makeOverlap(services, ris.get(1), ris.get(3));
    makeOverlap(services, ris.get(2), ris.get(3));
    makeOverlap(services, ris.get(2), ris.get(4));
    Threads.sleep(10000);
    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    assertEquals(6, report.getOverlaps().size());
    assertEquals(1, MetaFixer.calculateMerges(10, report.getOverlaps()).size());
    MetaFixer fixer = new MetaFixer(services);
    fixer.fixOverlaps(report);
    await(10, () -> {
      try {
        services.getCatalogJanitor().scan();
        final CatalogJanitor.Report postReport = services.getCatalogJanitor().getLastReport();
        return postReport.isEmpty();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    hbckChore.chore();
    assertEquals(0, hbckChore.getOrphanRegionsOnFS().size());
  }

  /**
   * Await the successful return of {@code condition}, sleeping {@code sleepMillis} between
   * invocations.
   */
  private static void await(final long sleepMillis, final BooleanSupplier condition)
    throws InterruptedException {
    try {
      while (!condition.getAsBoolean()) {
        Thread.sleep(sleepMillis);
      }
    } catch (RuntimeException e) {
      if (e.getCause() instanceof AssertionError) {
        throw (AssertionError) e.getCause();
      }
      throw e;
    }
  }
}
