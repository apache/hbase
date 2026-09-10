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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.TestTemplate;

public class CloneSnapshotFromClientErrorTestBase extends CloneSnapshotFromClientTestBase {

  protected CloneSnapshotFromClientErrorTestBase(int numReplicas) {
    super(numReplicas);
  }

  @TestTemplate
  public void testCloneNonExistentSnapshot() throws IOException, InterruptedException {
    String snapshotName = "random-snapshot-" + EnvironmentEdgeManager.currentTime();
    final TableName tableName =
      TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());
    assertThrows(SnapshotDoesNotExistException.class,
      () -> admin.cloneSnapshot(snapshotName, tableName));
  }

  @TestTemplate
  public void testCloneOnMissingNamespace() throws IOException, InterruptedException {
    final TableName clonedTableName = TableName.valueOf("unknownNS:" + getValidMethodName());
    assertThrows(NamespaceNotFoundException.class,
      () -> admin.cloneSnapshot(snapshotName1, clonedTableName));
  }
}
