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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.junit.Test;

public class RestoreSnapshotFromClientAfterTruncateTestBase
    extends RestoreSnapshotFromClientTestBase {

  @Test
  public void testRestoreSnapshotAfterTruncate() throws Exception {
    TableName tableName = TableName.valueOf(getValidMethodName());
    SnapshotTestingUtils.createTable(TEST_UTIL, tableName, getNumReplicas(), FAMILY);
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);
    int numOfRows = 0;

    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      numOfRows = countRows(table);
    }
    // take snapshot
    admin.snapshot("snap", tableName);
    admin.disableTable(tableName);
    admin.truncateTable(tableName, false);
    admin.disableTable(tableName);
    admin.restoreSnapshot("snap");

    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, numOfRows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }
}
