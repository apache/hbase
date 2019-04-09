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
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.junit.Test;

public class CloneSnapshotFromClientErrorTestBase extends CloneSnapshotFromClientTestBase {

  @Test(expected = SnapshotDoesNotExistException.class)
  public void testCloneNonExistentSnapshot() throws IOException, InterruptedException {
    String snapshotName = "random-snapshot-" + System.currentTimeMillis();
    final TableName tableName =
      TableName.valueOf(getValidMethodName() + "-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName, tableName);
  }

  @Test(expected = NamespaceNotFoundException.class)
  public void testCloneOnMissingNamespace() throws IOException, InterruptedException {
    final TableName clonedTableName = TableName.valueOf("unknownNS:" + getValidMethodName());
    admin.cloneSnapshot(snapshotName1, clonedTableName);
  }
}
