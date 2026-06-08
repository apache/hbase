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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;
import org.apache.hadoop.hbase.regionserver.CompactedHFilesDischarger;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.jupiter.api.TestTemplate;

public class CloneSnapshotFromClientAfterMergingRegionTestBase
  extends CloneSnapshotFromClientTestBase {
  protected CloneSnapshotFromClientAfterMergingRegionTestBase(int numReplicas) {
    super(numReplicas);
  }

  /**
   * Regression test for the bug where HFiles referenced by HFileLink Reference files in a cloned
   * table are deleted from the archive by {@code HFileLinkCleaner} after the pre-merge regions are
   * GC'd.
   *
   * <p>Sequence that triggers the bug:
   * <ol>
   *   <li>Flush the table so every region has HFiles on disk.</li>
   *   <li>Merge two regions — the merged result gets reference files pointing at the pre-merge
   *       regions' HFiles, which are immediately moved to the archive by the merge procedure.</li>
   *   <li>Snapshot the table while the merged result still holds reference files (compaction
   *       disabled).</li>
   *   <li>Clone the snapshot — {@code RestoreSnapshotHelper.restoreReferenceFile()} creates
   *       HFileLink Reference files in the clone pointing at the pre-merge HFiles, but does NOT
   *       create the back-references that {@code HFileLinkCleaner} relies on.</li>
   *   <li>Delete the snapshot so {@code SnapshotHFileCleaner} no longer protects the archived
   *       HFiles, then force a cache refresh to evict stale entries.</li>
   *   <li>Re-enable compaction and compact the parent table to resolve the merged result's
   *       reference files.</li>
   *   <li>{@code HFileLinkCleaner} deletes the archived pre-merge HFiles because there are no
   *       back-references — breaking every read on the cloned table.</li>
   * </ol>
   *
   * <p>Without the fix, the final {@code getAvailablePath} call throws
   * {@code FileNotFoundException}.
   */
  @TestTemplate
  public void testCloneSnapshotAfterMergingRegionAndGC() throws Exception {
    admin.catalogJanitorSwitch(false);
    TableName clonedTableName =
      TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());
    try {
      // Step 1: flush all regions so each has HFiles on disk before the merge.
      // The merge procedure creates reference files in the merged result that point to these HFiles,
      // which are then moved to the archive. Without on-disk HFiles there is nothing to reference.
      admin.flush(tableName);
      setCompactionsEnabled(false);
      // Step 2: merge two adjacent regions. Compaction is globally disabled in setupConfiguration()
      // so the merged result will retain reference files pointing at the pre-merge HFiles.
      // Two regions are sufficient to reproduce the bug: any clone region whose store files are
      // HFileLink References (created by RestoreSnapshotHelper.restoreReferenceFile) has no
      // back-references, so HFileLinkCleaner deletes the archived pre-merge HFiles and the
      // clone's region fails to open.
      RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
      List<RegionInfo> openBefore =
        regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OPEN);

      List<RegionInfo> defaultRegions = admin.getRegions(tableName);
      RegionReplicaUtil.removeNonDefaultRegions(defaultRegions);
      List<RegionInfo> preMergeRegions = new ArrayList<>(defaultRegions.subList(0, 2));

      admin.mergeRegionsAsync(
        new byte[][] { preMergeRegions.get(0).getEncodedNameAsBytes(),
          preMergeRegions.get(1).getEncodedNameAsBytes() },
        false);

      // After merging 2 primaries into 1, the open count drops by numReplicas.
      int expectedOpen = openBefore.size() - numReplicas;
      TEST_UTIL.waitFor(60_000, () -> {
        List<RegionInfo> open =
          regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OPEN);
        return open != null && open.size() == expectedOpen;
      });

      // Sanity-check: the merged result must have reference files.
      FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
      Path tableDir = CommonFSUtils.getTableDir(
        CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration()), tableName);
      assertTrue(hasReferenceFiles(fs, tableDir),
        "Merged result must have reference files before taking the snapshot");

      // Step 3: snapshot the table while the merged result still holds reference files.
      admin.snapshot(snapshotName2, tableName);

      // Step 4: clone the snapshot. RestoreSnapshotHelper.restoreReferenceFile() will create
      // HFileLink Reference files in the clone pointing at the pre-merge HFiles, but will NOT
      // create back-references in the archive — this is the bug being tested.
      admin.cloneSnapshot(snapshotName2, clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      // The clone must have reference files pointing at the pre-merge regions' HFiles.
      Path cloneTableDir = CommonFSUtils.getTableDir(
        CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration()), clonedTableName);
      assertTrue(hasReferenceFiles(fs, cloneTableDir),
        "Clone must have HFileLink Reference files pointing at the pre-merge HFiles");

      // Step 5: delete ALL snapshots so SnapshotHFileCleaner no longer protects the archived
      // pre-merge HFiles. snapshotName1 (taken by @BeforeEach before the merge) directly
      // references A's and B's HFiles; leaving it would cause SnapshotHFileCleaner to protect
      // those files even after snapshotName2 is gone, masking the bug entirely.
      // Force a cache refresh so stale entries are evicted immediately; without it the cache
      // may still hold the pre-merge HFile names and incorrectly protect them.
      SnapshotTestingUtils.deleteAllSnapshots(admin);

      HFileCleaner hfileCleaner =
        TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner();
      for (Object delegate : hfileCleaner.getDelegatesForTesting()) {
        if (delegate instanceof SnapshotHFileCleaner) {
          ((SnapshotHFileCleaner) delegate).getFileCacheForTesting()
            .triggerCacheRefreshForTesting();
          break;
        }
      }

      // Step 6: re-enable compaction and compact the parent table so the merged result resolves
      // its reference files into standalone HFiles.
      setCompactionsEnabled(true);
      admin.majorCompact(tableName);

      // Wait for compaction to commit AND for HDFS to reflect the new state. The two operations
      // must happen in the same loop iteration because HStore.hasReferences() checks both current
      // store files AND compacted files: until discharge clears the compacted list, hasReferences()
      // always returns true even after compaction commits. By running the synchronous discharger
      // first in each iteration, we clear the compacted list once compaction commits, causing
      // hasReferences() to return false on the same iteration — giving CatalogJanitor a clean
      // HDFS view (no reference files) when it runs.
      // The foundMergedRegion guard prevents a premature true return in the brief window between
      // the merge completing and the merged result being assigned to any RS.
      TEST_UTIL.waitFor(60_000, () -> {
        boolean foundMergedRegion = false;
        for (int i = 0; i < TEST_UTIL.getHBaseCluster().getNumLiveRegionServers(); i++) {
          CompactedHFilesDischarger discharger =
            TEST_UTIL.getHBaseCluster().getRegionServer(i).getCompactedHFilesDischarger();
          boolean prev = discharger.setUseExecutor(false);
          try {
            discharger.chore();
          } finally {
            discharger.setUseExecutor(prev);
          }
          for (HRegion region :
            TEST_UTIL.getHBaseCluster().getRegionServer(i).getRegions(tableName)) {
            if (region.getRegionInfo().getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
              continue;
            }
            foundMergedRegion = true;
            for (HStore store : region.getStores()) {
              if (store.hasReferences()) {
                return false;
              }
            }
          }
        }
        return foundMergedRegion;
      });

      // Step 7: CatalogJanitor GCs the pre-merge regions (A and B), archiving their HFiles.
      // The merge procedure calls AssignmentManager.markRegionAsMerged() which calls
      // regionStates.deleteRegion() for each parent — so A and B are fully removed from
      // RegionStates (not put into MERGED state). We therefore use preMergeRegions directly
      // rather than querying RegionState.State.MERGED (which always returns an empty list).
      admin.catalogJanitorSwitch(true);
      TEST_UTIL.getMiniHBaseCluster().getMaster().getCatalogJanitor().choreForTesting();
      TEST_UTIL.waitFor(60_000, () -> {
        try {
          for (RegionInfo parent : preMergeRegions) {
            if (fs.exists(new Path(tableDir, parent.getEncodedName()))) {
              return false;
            }
          }
          return true;
        } catch (IOException e) {
          return false;
        }
      });

      // Step 8: expire the archive TTL and run HFileCleaner.
      // Without the fix, HFileLinkCleaner sees no back-references for the pre-merge HFiles and
      // deletes them, breaking every read on the cloned table.
      Path archivePath = HFileArchiveUtil.getArchivePath(TEST_UTIL.getConfiguration());
      long expiredTime =
        EnvironmentEdgeManager.currentTime() - TimeToLiveHFileCleaner.DEFAULT_TTL * 1000;
      setFileTimesRecursively(fs, archivePath, expiredTime);
      hfileCleaner.triggerCleanerNow().get();

      // Step 9: the clone must still be fully readable —
      // its HFileLink References must still resolve.
      for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(clonedTableName)) {
        for (HStore store : region.getStores()) {
          for (HStoreFile sf : store.getStorefiles()) {
            Path path = sf.getPath();
            if (sf.isReference() && StoreFileInfo.isReference(path)) {
              Path refPath = StoreFileInfo.getReferredToFile(path);
              if (HFileLink.isHFileLink(refPath)) {
                HFileLink link =
                  HFileLink.buildFromHFileLinkPattern(TEST_UTIL.getConfiguration(), refPath);
                Path actualPath = link.getAvailablePath(fs);
                assertTrue(fs.exists(actualPath), "Actual file does not exist: " + actualPath);
              }
            }
          }
        }
      }

      // Step 10: disable and re-enable the clone table to force all region servers to close their
      // existing HFile handles and open fresh ones. Without the fix, the region open fails with
      // FileNotFoundException because HFileLink.getAvailablePath() cannot find the archived HFile
      // at any of its candidate locations (live, archive, .tmp, mobdir) — it was deleted by
      // HFileLinkCleaner. The table therefore never becomes available within the timeout.
      admin.disableTable(clonedTableName);
      admin.enableTable(clonedTableName);
      verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);
    } finally {
      setCompactionsEnabled(true);
      admin.catalogJanitorSwitch(true);
      if (admin.tableExists(clonedTableName)) {
        TEST_UTIL.deleteTable(clonedTableName);
      }
    }
  }

  /**
   * Returns true if any live region of the table at {@code tableDir} contains a reference file
   * (standard split/merge reference or HFileLink Reference).
   */
  private boolean hasReferenceFiles(FileSystem fs, Path tableDir) throws IOException {
    if (!fs.exists(tableDir)) {
      return false;
    }
    for (FileStatus regionStatus : fs.listStatus(tableDir)) {
      if (!regionStatus.isDirectory()) {
        continue;
      }
      String regionDirName = regionStatus.getPath().getName();
      if (regionDirName.startsWith(".")) {
        continue; // skip .tmp and similar special directories
      }
      FileStatus[] familyDirs = fs.listStatus(regionStatus.getPath());
      if (familyDirs == null) {
        continue;
      }
      for (FileStatus familyStatus : familyDirs) {
        if (!familyStatus.isDirectory()) {
          continue;
        }
        String familyDirName = familyStatus.getPath().getName();
        if (familyDirName.startsWith(".") || familyDirName.equals("recovered.edits")) {
          continue;
        }
        FileStatus[] storeFiles = fs.listStatus(familyStatus.getPath());
        if (storeFiles == null) {
          continue;
        }
        for (FileStatus fileStatus : storeFiles) {
          if (StoreFileInfo.isReference(fileStatus.getPath())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private void setCompactionsEnabled(boolean enabled) {
    for (int i = 0; i < TEST_UTIL.getHBaseCluster().getNumLiveRegionServers(); i++) {
      TEST_UTIL.getHBaseCluster().getRegionServer(i)
        .getCompactSplitThread().setCompactionsEnabled(enabled);
    }
  }

  private void setFileTimesRecursively(FileSystem fs, Path path, long time) throws IOException {
    fs.setTimes(path, time, -1);
    if (fs.isDirectory(path)) {
      for (FileStatus child : fs.listStatus(path)) {
        setFileTimesRecursively(fs, child.getPath(), time);
      }
    }
  }
}
