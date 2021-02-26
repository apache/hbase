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
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.junit.Test;

public class RestoreSnapshotFromClientCloneTestBase extends RestoreSnapshotFromClientTestBase {

  @Test
  public void testCloneSnapshotOfCloned() throws IOException, InterruptedException {
    TableName clonedTableName =
      TableName.valueOf(getValidMethodName() + "-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(clonedTableName, admin, getNumReplicas());
    admin.disableTable(clonedTableName);
    admin.snapshot(snapshotName2, clonedTableName);
    TEST_UTIL.deleteTable(clonedTableName);
    waitCleanerRun();

    admin.cloneSnapshot(snapshotName2, clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(clonedTableName, admin, getNumReplicas());
    TEST_UTIL.deleteTable(clonedTableName);
  }

  @Test
  public void testCloneAndRestoreSnapshot() throws IOException, InterruptedException {
    TEST_UTIL.deleteTable(tableName);
    waitCleanerRun();

    admin.cloneSnapshot(snapshotName0, tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
    waitCleanerRun();

    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  private void waitCleanerRun() throws InterruptedException {
    TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
  }
}
