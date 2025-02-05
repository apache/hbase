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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Test;

public class CloneSnapshotFromClientAfterSplittingRegionTestBase
  extends CloneSnapshotFromClientTestBase {

  private void splitRegion() throws IOException {
    try (Table k = TEST_UTIL.getConnection().getTable(tableName);
      ResultScanner scanner = k.getScanner(new Scan())) {
      // Split on the second row to make sure that the snapshot contains reference files.
      // We also disable the compaction so that the reference files are not compacted away.
      scanner.next();
      admin.split(tableName, scanner.next().getRow());
    }
  }

  @Test
  public void testCloneSnapshotAfterSplittingRegion() throws IOException, InterruptedException {
    // Turn off the CatalogJanitor
    admin.catalogJanitorSwitch(false);

    try {
      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      RegionReplicaUtil.removeNonDefaultRegions(regionInfos);

      // Split a region
      splitRegion();

      // Take a snapshot
      admin.snapshot(snapshotName2, tableName);

      // Clone the snapshot to another table
      TableName clonedTableName =
        TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());
      admin.cloneSnapshot(snapshotName2, clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);

      RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();

      // The region count of the cloned table should be the same as the one of the original table
      int openRegionCountOfOriginalTable =
        regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OPEN).size();
      int openRegionCountOfClonedTable =
        regionStates.getRegionByStateOfTable(clonedTableName).get(RegionState.State.OPEN).size();
      assertEquals(openRegionCountOfOriginalTable, openRegionCountOfClonedTable);

      int splitRegionCountOfOriginalTable =
        regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.SPLIT).size();
      List<RegionInfo> splitParents =
        regionStates.getRegionByStateOfTable(clonedTableName).get(RegionState.State.SPLIT);
      int splitRegionCountOfClonedTable = splitParents.size();
      assertEquals(splitRegionCountOfOriginalTable, splitRegionCountOfClonedTable);

      // Make sure that the meta table was updated with the correct split information
      for (RegionInfo splitParent : splitParents) {
        Result result = MetaTableAccessor.getRegionResult(TEST_UTIL.getConnection(), splitParent);
        for (RegionInfo daughter : MetaTableAccessor.getDaughterRegions(result)) {
          assertNotNull(daughter);
        }
      }

      TEST_UTIL.deleteTable(clonedTableName);
    } finally {
      admin.catalogJanitorSwitch(true);
    }
  }

  @Test
  public void testCloneSnapshotBeforeSplittingRegionAndDroppingTable()
    throws IOException, InterruptedException {
    // Turn off the CatalogJanitor
    admin.catalogJanitorSwitch(false);

    try {
      // Take a snapshot
      admin.snapshot(snapshotName2, tableName);

      // Clone the snapshot to another table
      TableName clonedTableName =
        TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());
      admin.cloneSnapshot(snapshotName2, clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      // Split a region of the original table
      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      RegionReplicaUtil.removeNonDefaultRegions(regionInfos);
      splitRegion();

      // Drop the original table
      admin.disableTable(tableName);
      admin.deleteTable(tableName);

      // Disable and enable the cloned table. This should be successful
      admin.disableTable(clonedTableName);
      admin.enableTable(clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);
    } finally {
      admin.catalogJanitorSwitch(true);
    }
  }
}
