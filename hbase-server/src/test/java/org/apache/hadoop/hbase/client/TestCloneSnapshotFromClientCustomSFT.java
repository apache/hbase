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
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
public class TestCloneSnapshotFromClientCustomSFT extends CloneSnapshotFromClientTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCloneSnapshotFromClientCustomSFT.class);

  public static final String CLONE_SFT = "FILE";

  @Test
  public void testCloneSnapshotWithCustomSFT() throws IOException, InterruptedException {
    TableName clonedTableName =
      TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());

    admin.cloneSnapshot(Bytes.toString(snapshotName1), clonedTableName, false, CLONE_SFT);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);

    TableDescriptor td = admin.getDescriptor(clonedTableName);
    assertEquals(CLONE_SFT, td.getValue(StoreFileTrackerFactory.TRACKER_IMPL));

    TEST_UTIL.deleteTable(clonedTableName);
  }

  @Test
  public void testCloneSnapshotWithIncorrectCustomSFT() throws IOException, InterruptedException {
    TableName clonedTableName =
      TableName.valueOf(getValidMethodName() + "-" + EnvironmentEdgeManager.currentTime());

    IOException ioException = assertThrows(IOException.class, () -> {
      admin.cloneSnapshot(Bytes.toString(snapshotName1), clonedTableName, false, "IncorrectSFT");
    });

    assertEquals(
      "java.lang.RuntimeException: java.lang.RuntimeException: "
        + "java.lang.ClassNotFoundException: Class IncorrectSFT not found",
      ioException.getMessage());
  }
}
