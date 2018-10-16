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
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.junit.Test;

public class CloneSnapshotFromClientNormalTestBase extends CloneSnapshotFromClientTestBase {

  @Test
  public void testCloneSnapshot() throws IOException, InterruptedException {
    TableName clonedTableName =
      TableName.valueOf(getValidMethodName() + "-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
    testCloneSnapshot(clonedTableName, emptySnapshot, 0);
  }

  private void testCloneSnapshot(TableName tableName, byte[] snapshotName, int snapshotRows)
      throws IOException, InterruptedException {
    // create a new table from snapshot
    admin.cloneSnapshot(snapshotName, tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshotRows);

    verifyReplicasCameOnline(tableName);
    TEST_UTIL.deleteTable(tableName);
  }

  private void verifyReplicasCameOnline(TableName tableName) throws IOException {
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  @Test
  public void testCloneSnapshotCrossNamespace() throws IOException, InterruptedException {
    String nsName = getValidMethodName() + "_ns_" + System.currentTimeMillis();
    admin.createNamespace(NamespaceDescriptor.create(nsName).build());
    final TableName clonedTableName =
      TableName.valueOf(nsName, getValidMethodName() + "-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
    testCloneSnapshot(clonedTableName, emptySnapshot, 0);
  }
}
