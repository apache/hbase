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
package org.apache.hadoop.hbase.master.janitor;

import static org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.HbckChore;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.GCRegionProcedure;
import org.apache.hadoop.hbase.master.assignment.GCMultipleMergedRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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

  private void testPlugsHolesWithReadReplicaInternal(final TableName tn, final int replicaCount)
    throws Exception {
    TEST_UTIL.createMultiRegionTable(tn, replicaCount, new byte[][] { HConstants.CATALOG_FAMILY });
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    MasterServices services = TEST_UTIL.getHBaseCluster().getMaster();
    int initialSize = services.getAssignmentManager().getRegionStates().getRegionStates().size();
    services.getCatalogJanitor().scan();
    Report report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.isEmpty());
    int originalCount = ris.size();
    // Remove first, last and middle region. See if hole gets plugged. Table has 26 * replicaCount regions.
    for (int i = 0; i < replicaCount; i ++) {
      deleteRegion(services, ris.get(3 * replicaCount + i));
      deleteRegion(services, ris.get(i));
      deleteRegion(services, ris.get(ris.size() - 1 - i));
    }
    assertEquals(initialSize - 3 * replicaCount,
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
    HBaseTestingUtility.await(50,
      () -> services.getMasterProcedureExecutor().getActiveProcIds().size() == 0);

    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertEquals(originalCount, ris.size());
  }

  @Test
  public void testPlugsHoles() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    testPlugsHolesWithReadReplicaInternal(tn, 1);
  }

  @Test
  public void testPlugsHolesWithReadReplica() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    testPlugsHolesWithReadReplicaInternal(tn, 3);
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
    Report report = services.getCatalogJanitor().getLastReport();
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertTrue(ris.isEmpty());
    MetaFixer fixer = new MetaFixer(services);
    fixer.fixHoles(report);
    report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.isEmpty());
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertEquals(0, ris.size());
  }

  private static RegionInfo makeOverlap(MasterServices services, RegionInfo a, RegionInfo b)
      throws IOException {
    RegionInfo overlapRegion = RegionInfoBuilder.newBuilder(a.getTable()).
        setStartKey(a.getStartKey()).
        setEndKey(b.getEndKey()).
        build();
    MetaTableAccessor.putsToMetaTable(services.getConnection(),
        Collections.singletonList(MetaTableAccessor.makePutFromRegionInfo(overlapRegion,
            System.currentTimeMillis())));
    // TODO: Add checks at assign time to PREVENT being able to assign over existing assign.
    long assign = services.getAssignmentManager().assign(overlapRegion);
    ProcedureTestingUtility.waitProcedures(services.getMasterProcedureExecutor(), assign);
    return overlapRegion;
  }

  private void testOverlapCommon(final TableName tn) throws Exception {
    Table t = TEST_UTIL.createMultiRegionTable(tn, HConstants.CATALOG_FAMILY);
    TEST_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertTrue(ris.size() > 5);
    HMaster services = TEST_UTIL.getHBaseCluster().getMaster();
    services.getCatalogJanitor().scan();
    Report report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.isEmpty());
    // Make a simple overlap spanning second and third region.
    makeOverlap(services, ris.get(1), ris.get(3));
    makeOverlap(services, ris.get(2), ris.get(3));
    makeOverlap(services, ris.get(2), ris.get(4));
  }

  @Test
  public void testOverlap() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    testOverlapCommon(tn);
    HMaster services = TEST_UTIL.getHBaseCluster().getMaster();
    HbckChore hbckChore = services.getHbckChore();

    CatalogJanitor cj = services.getCatalogJanitor();
    cj.scan();
    Report report = cj.getLastReport();
    assertEquals(6, report.getOverlaps().size());
    assertEquals(1,
      MetaFixer.calculateMerges(10, report.getOverlaps()).size());
    MetaFixer fixer = new MetaFixer(services);
    fixer.fixOverlaps(report);

    HBaseTestingUtility. await(10, () -> {
      try {
        if (cj.scan() > 0) {
          // It submits GC once, then it will immediately kick off another GC to test if
          // GCMultipleMergedRegionsProcedure is idempotent. If it is not, it will create
          // a hole.
          Map<RegionInfo, Result> mergedRegions = cj.getLastReport().mergedRegions;
          for (Map.Entry<RegionInfo, Result> e : mergedRegions.entrySet()) {
            List<RegionInfo> parents = MetaTableAccessor.getMergeRegions(e.getValue().rawCells());
            if (parents != null) {
              ProcedureExecutor<MasterProcedureEnv> pe = services.getMasterProcedureExecutor();
              pe.submitProcedure(new GCMultipleMergedRegionsProcedure(pe.getEnvironment(),
                e.getKey(), parents));
            }
          }
          return true;
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Wait until all GCs settled down
    HBaseTestingUtility.await(10, () -> {
      return services.getMasterProcedureExecutor().getActiveProcIds().isEmpty();
    });

    // No orphan regions on FS
    hbckChore.choreForTesting();
    assertEquals(0, hbckChore.getOrphanRegionsOnFS().size());

    // No holes reported.
    cj.scan();
    final Report postReport = cj.getLastReport();
    assertTrue(postReport.isEmpty());
  }

  @Test
  public void testMultipleTableOverlaps() throws Exception {
    TableName t1 = TableName.valueOf("t1");
    TableName t2 = TableName.valueOf("t2");
    TEST_UTIL.createMultiRegionTable(t1, new byte[][] { HConstants.CATALOG_FAMILY });
    TEST_UTIL.createMultiRegionTable(t2, new byte[][] { HConstants.CATALOG_FAMILY });
    TEST_UTIL.waitTableAvailable(t2);

    HMaster services = TEST_UTIL.getHBaseCluster().getMaster();
    services.getCatalogJanitor().scan();
    Report report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.isEmpty());

    // Make a simple overlap for t1
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), t1);
    makeOverlap(services, ris.get(1), ris.get(2));
    // Make a simple overlap for t2
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), t2);
    makeOverlap(services, ris.get(1), ris.get(2));

    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    assertEquals("Region overlaps count does not match.", 4, report.getOverlaps().size());

    MetaFixer fixer = new MetaFixer(services);
    List<Long> longs = fixer.fixOverlaps(report);
    long[] procIds = longs.stream().mapToLong(l -> l).toArray();
    ProcedureTestingUtility.waitProcedures(services.getMasterProcedureExecutor(), procIds);

    // After fix, verify no overlaps are left.
    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    assertTrue("After fix there should not have been any overlaps.", report.isEmpty());
  }

  @Test
  public void testOverlapWithSmallMergeCount() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    try {
      testOverlapCommon(tn);
      HMaster services = TEST_UTIL.getHBaseCluster().getMaster();
      CatalogJanitor cj = services.getCatalogJanitor();
      cj.scan();
      Report report = cj.getLastReport();
      assertEquals(6, report.getOverlaps().size());
      assertEquals(2,
        MetaFixer.calculateMerges(5, report.getOverlaps()).size());

      // The max merge count is set to 5 so overlap regions are divided into
      // two merge requests.
      TEST_UTIL.getHBaseCluster().getMaster().getConfiguration().setInt(
        "hbase.master.metafixer.max.merge.count", 5);

      // Get overlap regions
      HashSet<String> overlapRegions = new HashSet<>();
      for (Pair<RegionInfo, RegionInfo> pair : report.getOverlaps()) {
        overlapRegions.add(pair.getFirst().getRegionNameAsString());
        overlapRegions.add(pair.getSecond().getRegionNameAsString());
      }

      MetaFixer fixer = new MetaFixer(services);
      fixer.fixOverlaps(report);
      AssignmentManager am = services.getAssignmentManager();

      HBaseTestingUtility.await(200, () -> {
        try {
          cj.scan();
          final Report postReport = cj.getLastReport();
          RegionStates regionStates = am.getRegionStates();

          // Make sure that two merged regions are opened and GCs are done.
          if (postReport.getOverlaps().size() == 1) {
            Pair<RegionInfo, RegionInfo> pair = postReport.getOverlaps().get(0);
            if ((!overlapRegions.contains(pair.getFirst().getRegionNameAsString()) &&
              regionStates.getRegionState(pair.getFirst()).isOpened()) &&
              (!overlapRegions.contains(pair.getSecond().getRegionNameAsString()) &&
              regionStates.getRegionState(pair.getSecond()).isOpened())) {
              // Make sure GC is done.
              List<RegionInfo> firstParents = MetaTableAccessor.getMergeRegions(
                services.getConnection(), pair.getFirst().getRegionName());
              List<RegionInfo> secondParents = MetaTableAccessor.getMergeRegions(
                services.getConnection(), pair.getSecond().getRegionName());

              return (firstParents == null || firstParents.isEmpty()) &&
                (secondParents == null || secondParents.isEmpty());
            }
          }
          return false;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // Second run of fixOverlap should fix all.
      report = cj.getLastReport();
      fixer.fixOverlaps(report);

      HBaseTestingUtility.await(20, () -> {
        try {
          // Make sure it GC only once.
          return (cj.scan() > 0);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // No holes reported.
      cj.scan();
      final Report postReport = cj.getLastReport();
      assertTrue(postReport.isEmpty());

    } finally {
      TEST_UTIL.getHBaseCluster().getMaster().getConfiguration().unset(
        "hbase.master.metafixer.max.merge.count");

      TEST_UTIL.deleteTable(tn);
    }
  }

  /**
   * This test covers the case that one of merged parent regions is a merged child region that
   * has not been GCed but there is no reference files anymore. In this case, it will kick off
   * a GC procedure, but no merge will happen.
   */
  @Test
  public void testMergeWithMergedChildRegion() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tn, HConstants.CATALOG_FAMILY);
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertTrue(ris.size() > 5);
    HMaster services = TEST_UTIL.getHBaseCluster().getMaster();
    CatalogJanitor cj = services.getCatalogJanitor();
    cj.scan();
    Report report = cj.getLastReport();
    assertTrue(report.isEmpty());
    RegionInfo overlapRegion = makeOverlap(services, ris.get(1), ris.get(2));

    cj.scan();
    report = cj.getLastReport();
    assertEquals(2, report.getOverlaps().size());

    // Mark it as a merged child region.
    RegionInfo fakedParentRegion = RegionInfoBuilder.newBuilder(tn).
      setStartKey(overlapRegion.getStartKey()).
      build();

    Table meta = MetaTableAccessor.getMetaHTable(TEST_UTIL.getConnection());
    Put putOfMerged = MetaTableAccessor.makePutFromRegionInfo(overlapRegion,
      HConstants.LATEST_TIMESTAMP);
    String qualifier = String.format(HConstants.MERGE_QUALIFIER_PREFIX_STR + "%04d", 0);
    putOfMerged.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(
      putOfMerged.getRow()).
      setFamily(HConstants.CATALOG_FAMILY).
      setQualifier(Bytes.toBytes(qualifier)).
      setTimestamp(putOfMerged.getTimestamp()).
      setType(Cell.Type.Put).
      setValue(RegionInfo.toByteArray(fakedParentRegion)).
      build());

    meta.put(putOfMerged);

    MetaFixer fixer = new MetaFixer(services);
    fixer.fixOverlaps(report);

    // Wait until all procedures settled down
    HBaseTestingUtility.await(200, () -> {
      return services.getMasterProcedureExecutor().getActiveProcIds().isEmpty();
    });

    // No merge is done, overlap is still there.
    cj.scan();
    report = cj.getLastReport();
    assertEquals(2, report.getOverlaps().size());

    fixer.fixOverlaps(report);

    // Wait until all procedures settled down
    HBaseTestingUtility.await(200, () -> {
      return services.getMasterProcedureExecutor().getActiveProcIds().isEmpty();
    });

    // Merge is done and no more overlaps
    cj.scan();
    report = cj.getLastReport();
    assertEquals(0, report.getOverlaps().size());
  }

  /**
   * Make it so a big overlap spans many Regions, some of which are non-contiguous. Make it so
   * we can fix this condition. HBASE-24247
   */
  @Test
  public void testOverlapWithMergeOfNonContiguous() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tn, HConstants.CATALOG_FAMILY);
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    assertTrue(ris.size() > 5);
    MasterServices services = TEST_UTIL.getHBaseCluster().getMaster();
    services.getCatalogJanitor().scan();
    Report report = services.getCatalogJanitor().getLastReport();
    assertTrue(report.isEmpty());
    // Make a simple overlap spanning second and third region.
    makeOverlap(services, ris.get(1), ris.get(5));
    // Now Delete a region under the overlap to manufacture non-contiguous sub regions.
    RegionInfo deletedRegion = ris.get(3);
    long pid = services.getAssignmentManager().unassign(deletedRegion);
    while (!services.getMasterProcedureExecutor().isFinished(pid)) {
      Threads.sleep(100);
    }
    GCRegionProcedure procedure =
      new GCRegionProcedure(services.getMasterProcedureExecutor().getEnvironment(), ris.get(3));
    pid = services.getMasterProcedureExecutor().submitProcedure(procedure);
    while (!services.getMasterProcedureExecutor().isFinished(pid)) {
      Threads.sleep(100);
    }
    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    assertEquals(1, MetaFixer.calculateMerges(10, report.getOverlaps()).size());
    MetaFixer fixer = new MetaFixer(services);
    fixer.fixOverlaps(report);
    HBaseTestingUtility.await(10, () -> {
      try {
        services.getCatalogJanitor().scan();
        final Report postReport = services.getCatalogJanitor().getLastReport();
        return postReport.isEmpty();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
