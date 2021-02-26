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

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

public class CloneSnapshotFromClientCloneLinksAfterDeleteTestBase
    extends CloneSnapshotFromClientTestBase {

  /**
   * Verify that tables created from the snapshot are still alive after source table deletion.
   */
  @Test
  public void testCloneLinksAfterDelete() throws IOException, InterruptedException {
    // Clone a table from the first snapshot
    final TableName clonedTableName =
      TableName.valueOf(getValidMethodName() + "1-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot0Rows);

    // Take a snapshot of this cloned table.
    admin.disableTable(clonedTableName);
    admin.snapshot(snapshotName2, clonedTableName);

    // Clone the snapshot of the cloned table
    final TableName clonedTableName2 =
      TableName.valueOf(getValidMethodName() + "2-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName2, clonedTableName2);
    verifyRowCount(TEST_UTIL, clonedTableName2, snapshot0Rows);
    admin.disableTable(clonedTableName2);

    // Remove the original table
    TEST_UTIL.deleteTable(tableName);
    waitCleanerRun();

    // Verify the first cloned table
    admin.enableTable(clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot0Rows);

    // Verify the second cloned table
    admin.enableTable(clonedTableName2);
    verifyRowCount(TEST_UTIL, clonedTableName2, snapshot0Rows);
    admin.disableTable(clonedTableName2);

    // Delete the first cloned table
    TEST_UTIL.deleteTable(clonedTableName);
    waitCleanerRun();

    // Verify the second cloned table
    admin.enableTable(clonedTableName2);
    verifyRowCount(TEST_UTIL, clonedTableName2, snapshot0Rows);

    // Clone a new table from cloned
    final TableName clonedTableName3 =
      TableName.valueOf(getValidMethodName() + "3-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName2, clonedTableName3);
    verifyRowCount(TEST_UTIL, clonedTableName3, snapshot0Rows);

    // Delete the cloned tables
    TEST_UTIL.deleteTable(clonedTableName2);
    TEST_UTIL.deleteTable(clonedTableName3);
    admin.deleteSnapshot(snapshotName2);
  }

  private void waitCleanerRun() throws InterruptedException {
    TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
  }
}
