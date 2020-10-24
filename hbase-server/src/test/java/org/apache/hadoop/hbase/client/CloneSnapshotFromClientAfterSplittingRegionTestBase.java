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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class CloneSnapshotFromClientAfterSplittingRegionTestBase
    extends CloneSnapshotFromClientTestBase {

  private void splitRegion(final RegionInfo regionInfo) throws IOException {
    byte[][] splitPoints = Bytes.split(regionInfo.getStartKey(), regionInfo.getEndKey(), 1);
    admin.split(regionInfo.getTable(), splitPoints[1]);
  }

  @Test
  public void testCloneSnapshotAfterSplittingRegion() throws IOException, InterruptedException {
    // Turn off the CatalogJanitor
    admin.catalogJanitorSwitch(false);

    try {
      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      RegionReplicaUtil.removeNonDefaultRegions(regionInfos);

      // Split the first region
      splitRegion(regionInfos.get(0));

      // Take a snapshot
      admin.snapshot(snapshotName2, tableName);

      // Clone the snapshot to another table
      TableName clonedTableName =
        TableName.valueOf(getValidMethodName() + "-" + System.currentTimeMillis());
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
      int splitRegionCountOfClonedTable =
        regionStates.getRegionByStateOfTable(clonedTableName).get(RegionState.State.SPLIT).size();
      assertEquals(splitRegionCountOfOriginalTable, splitRegionCountOfClonedTable);

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
        TableName.valueOf(getValidMethodName() + "-" + System.currentTimeMillis());
      admin.cloneSnapshot(snapshotName2, clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      // Split the first region of the original table
      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      RegionReplicaUtil.removeNonDefaultRegions(regionInfos);
      splitRegion(regionInfos.get(0));

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
