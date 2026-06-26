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
package org.apache.hadoop.hbase.client;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Base class for testing the clone-snapshot flow when the snapshot was taken after a split that
 * produced whole-file {@code HFileLink}s for the daughters instead of {@code Reference} files.
 * <p>
 * Since HBASE-26421, a split builds an {@code HFileLink} (not a {@code Reference}) for a daughter
 * whenever a store file lies entirely on one side of the split point. When every store file falls
 * on one side, the snapshot contains no reference files at all, so the daughters link directly to
 * the snapshot files and do not depend on the cloned parent region. This is the complement of the
 * {@code Reference} case verified by {@code CloneSnapshotFromClientAfterSplittingRegionTestBase}
 * (the regression HBASE-29111 guards against): here there is no parent-to-daughter mapping to
 * record, and the cloned table must still be safe after the source table and snapshot are removed.
 */
public class CloneSnapshotFromClientAfterSplittingRegionWithLinksTestBase
  extends CloneSnapshotFromClientTestBase {

  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static final int ROWS_PER_BATCH = 10;

  // Two disjoint key ranges and a split point strictly between them. After splitting, the "low"
  // store file lies entirely below SPLIT_KEY and the "high" store file entirely above it, so each
  // daughter receives one whole store file as an HFileLink and neither side gets a Reference.
  private static final String LOW_PREFIX = "a";
  private static final String HIGH_PREFIX = "z";
  private static final byte[] SPLIT_KEY = Bytes.toBytes("m");

  private TableName clonedTableName;
  private String snapshotName;

  protected CloneSnapshotFromClientAfterSplittingRegionWithLinksTestBase(int numReplicas) {
    super(numReplicas);
  }

  protected static void setupConfiguration() {
    CloneSnapshotFromClientTestBase.setupConfiguration();
    // CloneSnapshotFromClientTestBase already disables compaction, which keeps the two store files
    // we create from being merged into one that would straddle the split point. On top of that,
    // make archived files immediately eligible for cleaning so that the data-survival check below
    // is meaningful: only the cloned table's HFileLink back-references should keep them alive.
    TEST_UTIL.getConfiguration().setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
  }

  @Override
  protected void initSnapshotNames(long tid) {
    clonedTableName = TableName.valueOf(getValidMethodName() + "-clone-" + tid);
    snapshotName = "snaptb-links-" + tid;
  }

  @Override
  protected void createTableAndSnapshots() throws Exception {
    createTable();
    admin.catalogJanitorSwitch(false);
  }

  @AfterEach
  public void tearDownClone() throws Exception {
    admin.catalogJanitorSwitch(true);
    if (clonedTableName != null && admin.tableExists(clonedTableName)) {
      TEST_UTIL.deleteTable(clonedTableName);
    }
  }

  /**
   * Create a single-region table so that we fully control its store files before splitting.
   */
  @Override
  protected void createTable() throws IOException, InterruptedException {
    SnapshotTestingUtils.createTable(TEST_UTIL, tableName, numReplicas, 1, FAMILY);
  }

  @TestTemplate
  public void testClonedTableWithLinksSurvivesSourceDeletion() throws Exception {
    // Write two store files whose key ranges are disjoint and sit on opposite sides of SPLIT_KEY.
    int totalRows = loadTwoDisjointStoreFiles();

    // Split between the two ranges. Each daughter receives one whole store file as an HFileLink,
    // so the snapshot contains only HFileLinks and no Reference files.
    int numRegions = admin.getRegions(tableName).size();
    admin.split(tableName, SPLIT_KEY);
    await().atMost(Duration.ofSeconds(30)).untilAsserted(
      () -> assertEquals(numRegions + numReplicas, admin.getRegions(tableName).size()));

    // Guard: the split must have produced only HFileLinks (no Reference files) for the daughters.
    // admin.getRegions() can report the daughters (from meta) before the region servers have opened
    // them and loaded their store files, so retry until the links are actually online.
    await().atMost(Duration.ofSeconds(30)).untilAsserted(this::assertDaughtersHaveOnlyLinks);

    // Take a snapshot and clone it.
    admin.snapshot(snapshotName, tableName);
    admin.cloneSnapshot(snapshotName, clonedTableName);
    SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

    // The cloned table must contain all rows.
    verifyRowCount(TEST_UTIL, clonedTableName, totalRows);

    // Guard: because the daughters were cloned from whole-file HFileLinks (not References), the
    // cloned table's meta does not record the parent's split daughters (SPLITA/SPLITB). The
    // daughters link directly to the snapshot files and do not depend on the cloned parent.
    assertClonedSplitParentHasNoDaughters();

    // Remove the source table and the snapshot, then run the HFile cleaner. The cloned table's
    // HFileLinks (and their back-references) must keep the underlying files alive in the archive.
    TEST_UTIL.deleteTable(tableName);
    admin.deleteSnapshot(snapshotName);
    runHFileCleaner();

    // Reopen the cloned table to force the store files to be re-resolved from disk, then verify
    // that no data was lost.
    admin.disableTable(clonedTableName);
    admin.enableTable(clonedTableName);
    SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, totalRows);
  }

  private int loadTwoDisjointStoreFiles() throws IOException {
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      putBatch(table, LOW_PREFIX);
      TEST_UTIL.flush(tableName);
      putBatch(table, HIGH_PREFIX);
      TEST_UTIL.flush(tableName);
      return countRows(table);
    }
  }

  private void putBatch(Table table, String prefix) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < ROWS_PER_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(prefix + String.format("%03d", i)));
      put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(prefix + i));
      puts.add(put);
    }
    table.put(puts);
  }

  private void assertDaughtersHaveOnlyLinks() throws IOException {
    int linkFiles = 0;
    for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(tableName)) {
      if (
        !RegionReplicaUtil.isDefaultReplica(region.getRegionInfo())
          || region.getRegionInfo().isSplitParent()
      ) {
        continue;
      }
      for (HStore store : region.getStores()) {
        for (HStoreFile sf : store.getStorefiles()) {
          assertTrue(sf.getFileInfo().isLink(),
            "Expected only whole-file HFileLinks after the split, but found a non-link store file: "
              + sf.getPath());
          linkFiles++;
        }
      }
    }
    assertTrue(linkFiles >= 2,
      "Expected at least two HFileLink files across the daughter regions, found " + linkFiles);
  }

  private void assertClonedSplitParentHasNoDaughters() throws IOException {
    RegionStates regionStates =
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
    List<RegionInfo> splitParents =
      regionStates.getRegionByStateOfTable(clonedTableName).get(RegionState.State.SPLIT);
    assertNotNull(splitParents);
    assertFalse(splitParents.isEmpty(), "The cloned table should contain a split parent region");
    for (RegionInfo splitParent : splitParents) {
      Result result = MetaTableAccessor.getRegionResult(TEST_UTIL.getConnection(), splitParent);
      PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(result);
      assertNull(daughters.getFirst(),
        "Did not expect SPLITA to be recorded for an all-HFileLink clone");
      assertNull(daughters.getSecond(),
        "Did not expect SPLITB to be recorded for an all-HFileLink clone");
    }
  }

  private void runHFileCleaner() throws IOException {
    TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
  }
}
