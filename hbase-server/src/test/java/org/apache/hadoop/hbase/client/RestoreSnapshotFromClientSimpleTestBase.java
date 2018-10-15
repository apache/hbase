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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class RestoreSnapshotFromClientSimpleTestBase extends RestoreSnapshotFromClientTestBase {

  @Test
  public void testRestoreSnapshot() throws IOException {
    verifyRowCount(TEST_UTIL, tableName, snapshot1Rows);
    admin.disableTable(tableName);
    admin.snapshot(snapshotName1, tableName);
    // Restore from snapshot-0
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from emptySnapshot
    admin.disableTable(tableName);
    admin.restoreSnapshot(emptySnapshot);
    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, 0);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    TEST_UTIL.deleteTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    verifyRowCount(TEST_UTIL, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  @Test
  public void testCorruptedSnapshot() throws IOException, InterruptedException {
    SnapshotTestingUtils.corruptSnapshot(TEST_UTIL, Bytes.toString(snapshotName0));
    TableName cloneName =
      TableName.valueOf(getValidMethodName() + "-" + System.currentTimeMillis());
    try {
      admin.cloneSnapshot(snapshotName0, cloneName);
      fail("Expected CorruptedSnapshotException, got succeeded cloneSnapshot()");
    } catch (CorruptedSnapshotException e) {
      // Got the expected corruption exception.
      // check for no references of the cloned table.
      assertFalse(admin.tableExists(cloneName));
    } catch (Exception e) {
      fail("Expected CorruptedSnapshotException got: " + e);
    }
  }
}
