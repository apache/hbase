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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncSnapshotAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncSnapshotAdminApi.class);

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
      assertNotNull(snap1);
      assertNotNull(snap1.getName());
      assertNotNull(snap2);
      assertNotNull(snap2.getName());
      return snap1.getName().compareTo(snap2.getName());
    });

    assertEquals(snapshotName1, snapshots.get(0).getName());
    assertEquals(tableName, snapshots.get(0).getTableName());
    assertEquals(SnapshotType.FLUSH, snapshots.get(0).getType());
    assertEquals(snapshotName2, snapshots.get(1).getName());
    assertEquals(tableName, snapshots.get(1).getTableName());
    assertEquals(SnapshotType.FLUSH, snapshots.get(1).getType());
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
    assertEquals(1, snapshots.size());
    assertEquals(snapshotName1, snapshots.get(0).getName());
    assertEquals(tableName, snapshots.get(0).getTableName());
    assertEquals(SnapshotType.FLUSH, snapshots.get(0).getType());

    // cloneSnapshot into a existed table.
    boolean failed = false;
    try {
      admin.cloneSnapshot(snapshotName1, tableName).get();
    } catch (Exception e) {
      failed = true;
    }
    assertTrue(failed);

    // cloneSnapshot into a new table.
    assertTrue(!syncAdmin.tableExists(tableName2));
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
          assertArrayEquals(result.getRow(), Bytes.toBytes(rowCount));
          assertArrayEquals(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("cq")),
            Bytes.toBytes(rowCount));
          rowCount += 1;
        }
        assertEquals(rowCount, expectedRowCount);
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
    assertEquals(0, admin.listSnapshots().get().size());

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    assertEquals(2, admin.listSnapshots().get().size());

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
    assertEquals(0, admin.listSnapshots().get().size());

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    admin.snapshot(snapshotName3, tableName).get();
    assertEquals(3, admin.listSnapshots().get().size());

    assertEquals(3, admin.listSnapshots(Pattern.compile("(.*)")).get().size());
    assertEquals(3, admin.listSnapshots(Pattern.compile("snapshotName(\\d+)")).get().size());
    assertEquals(2, admin.listSnapshots(Pattern.compile("snapshotName[1|3]")).get().size());
    assertEquals(3, admin.listSnapshots(Pattern.compile("snapshot(.*)")).get().size());
    assertEquals(3, admin.listTableSnapshots(Pattern.compile("testListSnapshots"),
        Pattern.compile("s(.*)")).get().size());
    assertEquals(0, admin.listTableSnapshots(Pattern.compile("fakeTableName"),
            Pattern.compile("snap(.*)")).get().size());
    assertEquals(2, admin.listTableSnapshots(Pattern.compile("test(.*)"),
            Pattern.compile("snap(.*)[1|3]")).get().size());
  }

  @Test
  public void testDeleteSnapshots() throws Exception {
    Table table = TEST_UTIL.createTable(tableName, Bytes.toBytes("f1"));
    for (int i = 0; i < 3000; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("cq"),
        Bytes.toBytes(i)));
    }
    assertEquals(0, admin.listSnapshots().get().size());

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    admin.snapshot(snapshotName3, tableName).get();
    assertEquals(3, admin.listSnapshots().get().size());

    admin.deleteSnapshot(snapshotName1).get();
    assertEquals(2, admin.listSnapshots().get().size());

    admin.deleteSnapshots(Pattern.compile("(.*)abc")).get();
    assertEquals(2, admin.listSnapshots().get().size());

    admin.deleteSnapshots(Pattern.compile("(.*)1")).get();
    assertEquals(2, admin.listSnapshots().get().size());

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)1")).get();
    assertEquals(2, admin.listSnapshots().get().size());

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)2")).get();
    assertEquals(1, admin.listSnapshots().get().size());

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)3")).get();
    assertEquals(0, admin.listSnapshots().get().size());
  }
}
