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
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.List;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncSnapshotAdminApi extends TestAsyncAdminBase {

  @Test
  public void testTakeSnapshot() throws Exception {
    String snapshotName1 = "snapshotName1";
    String snapshotName2 = "snapshotName2";
    TableName tableName = TableName.valueOf("testTakeSnapshot");
    Admin syncAdmin = TEST_UTIL.getAdmin();

    try {
      Table table = TEST_UTIL.createTable(tableName, Bytes.toBytes("f1"));
      for (int i = 0; i < 3000; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("cq"),
          Bytes.toBytes(i)));
      }

      admin.snapshot(snapshotName1, tableName).get();
      admin.snapshot(snapshotName2, tableName).get();
      List<SnapshotDescription> snapshots = syncAdmin.listSnapshots();
      Collections.sort(snapshots, (snap1, snap2) -> {
        Assert.assertNotNull(snap1);
        Assert.assertNotNull(snap1.getName());
        Assert.assertNotNull(snap2);
        Assert.assertNotNull(snap2.getName());
        return snap1.getName().compareTo(snap2.getName());
      });

      Assert.assertEquals(snapshotName1, snapshots.get(0).getName());
      Assert.assertEquals(tableName, snapshots.get(0).getTableName());
      Assert.assertEquals(SnapshotType.FLUSH, snapshots.get(0).getType());
      Assert.assertEquals(snapshotName2, snapshots.get(1).getName());
      Assert.assertEquals(tableName, snapshots.get(1).getTableName());
      Assert.assertEquals(SnapshotType.FLUSH, snapshots.get(1).getType());
    } finally {
      syncAdmin.deleteSnapshot(snapshotName1);
      syncAdmin.deleteSnapshot(snapshotName2);
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testCloneSnapshot() throws Exception {
    String snapshotName1 = "snapshotName1";
    TableName tableName = TableName.valueOf("testCloneSnapshot");
    TableName tableName2 = TableName.valueOf("testCloneSnapshot2");
    Admin syncAdmin = TEST_UTIL.getAdmin();

    try {
      Table table = TEST_UTIL.createTable(tableName, Bytes.toBytes("f1"));
      for (int i = 0; i < 3000; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("cq"),
          Bytes.toBytes(i)));
      }

      admin.snapshot(snapshotName1, tableName).get();
      List<SnapshotDescription> snapshots = syncAdmin.listSnapshots();
      Assert.assertEquals(snapshots.size(), 1);
      Assert.assertEquals(snapshotName1, snapshots.get(0).getName());
      Assert.assertEquals(tableName, snapshots.get(0).getTableName());
      Assert.assertEquals(SnapshotType.FLUSH, snapshots.get(0).getType());

      // cloneSnapshot into a existed table.
      boolean failed = false;
      try {
        admin.cloneSnapshot(snapshotName1, tableName).get();
      } catch (Exception e) {
        failed = true;
      }
      Assert.assertTrue(failed);

      // cloneSnapshot into a new table.
      Assert.assertTrue(!syncAdmin.tableExists(tableName2));
      admin.cloneSnapshot(snapshotName1, tableName2).get();
      syncAdmin.tableExists(tableName2);
    } finally {
      syncAdmin.deleteSnapshot(snapshotName1);
      TEST_UTIL.deleteTable(tableName);
    }
  }
}