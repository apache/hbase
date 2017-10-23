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
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncSnapshotAdminApi extends TestAsyncAdminBase {

  private static final Pattern MATCH_ALL = Pattern.compile(".*");

  String snapshotName1 = "snapshotName1";
  String snapshotName2 = "snapshotName2";
  String snapshotName3 = "snapshotName3";

  @After
  public void cleanup() throws Exception {
    admin.deleteSnapshots(MATCH_ALL).get();
    admin.listTableNames().get().forEach(t -> admin.disableTable(t).join());
    admin.listTableNames().get().forEach(t -> admin.deleteTable(t).join());
  }

  @Test
  public void testTakeSnapshot() throws Exception {
    Admin syncAdmin = TEST_UTIL.getAdmin();

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
  }

  @Test
  public void testCloneSnapshot() throws Exception {
    TableName tableName2 = TableName.valueOf("testCloneSnapshot2");
    Admin syncAdmin = TEST_UTIL.getAdmin();

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
  }

  private void assertResult(TableName tableName, int expectedRowCount) throws IOException {
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Scan scan = new Scan();
      try (ResultScanner scanner = table.getScanner(scan)) {
        Result result;
        int rowCount = 0;
        while ((result = scanner.next()) != null) {
          Assert.assertArrayEquals(result.getRow(), Bytes.toBytes(rowCount));
          Assert.assertArrayEquals(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("cq")),
            Bytes.toBytes(rowCount));
          rowCount += 1;
        }
        Assert.assertEquals(rowCount, expectedRowCount);
      }
    }
  }

  @Test
  public void testRestoreSnapshot() throws Exception {
    Table table = TEST_UTIL.createTable(tableName, Bytes.toBytes("f1"));
    for (int i = 0; i < 3000; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("cq"),
        Bytes.toBytes(i)));
    }
    Assert.assertEquals(admin.listSnapshots().get().size(), 0);

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 2);

    admin.disableTable(tableName).get();
    admin.restoreSnapshot(snapshotName1, true).get();
    admin.enableTable(tableName).get();
    assertResult(tableName, 3000);

    admin.disableTable(tableName).get();
    admin.restoreSnapshot(snapshotName2, false).get();
    admin.enableTable(tableName).get();
    assertResult(tableName, 3000);
  }

  @Test
  public void testListSnapshots() throws Exception {
    Table table = TEST_UTIL.createTable(tableName, Bytes.toBytes("f1"));
    for (int i = 0; i < 3000; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("cq"),
        Bytes.toBytes(i)));
    }
    Assert.assertEquals(admin.listSnapshots().get().size(), 0);

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    admin.snapshot(snapshotName3, tableName).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 3);

    Assert.assertEquals(admin.listSnapshots(Pattern.compile("(.*)")).get().size(), 3);
    Assert.assertEquals(admin.listSnapshots(Pattern.compile("snapshotName(\\d+)")).get().size(), 3);
    Assert.assertEquals(admin.listSnapshots(Pattern.compile("snapshotName[1|3]")).get().size(), 2);
    Assert.assertEquals(admin.listSnapshots(Pattern.compile("snapshot(.*)")).get().size(), 3);
    Assert.assertEquals(
      admin.listTableSnapshots(Pattern.compile("testListSnapshots"), Pattern.compile("s(.*)")).get()
          .size(),
      3);
    Assert.assertEquals(
      admin.listTableSnapshots(Pattern.compile("fakeTableName"), Pattern.compile("snap(.*)")).get()
          .size(),
      0);
    Assert.assertEquals(
      admin.listTableSnapshots(Pattern.compile("test(.*)"), Pattern.compile("snap(.*)[1|3]")).get()
          .size(),
      2);
  }

  @Test
  public void testDeleteSnapshots() throws Exception {
    Table table = TEST_UTIL.createTable(tableName, Bytes.toBytes("f1"));
    for (int i = 0; i < 3000; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("cq"),
        Bytes.toBytes(i)));
    }
    Assert.assertEquals(admin.listSnapshots().get().size(), 0);

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    admin.snapshot(snapshotName3, tableName).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 3);

    admin.deleteSnapshot(snapshotName1).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 2);

    admin.deleteSnapshots(Pattern.compile("(.*)abc")).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 2);

    admin.deleteSnapshots(Pattern.compile("(.*)1")).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 2);

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)1")).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 2);

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)2")).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 1);

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)3")).get();
    Assert.assertEquals(admin.listSnapshots().get().size(), 0);
  }
}
