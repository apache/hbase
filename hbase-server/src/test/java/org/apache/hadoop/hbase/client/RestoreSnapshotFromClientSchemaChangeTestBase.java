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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;

public class RestoreSnapshotFromClientSchemaChangeTestBase
    extends RestoreSnapshotFromClientTestBase {

  private Set<String> getFamiliesFromFS(final TableName tableName) throws IOException {
    MasterFileSystem mfs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Set<String> families = new HashSet<>();
    Path tableDir = CommonFSUtils.getTableDir(mfs.getRootDir(), tableName);
    for (Path regionDir : FSUtils.getRegionDirs(mfs.getFileSystem(), tableDir)) {
      for (Path familyDir : FSUtils.getFamilyDirs(mfs.getFileSystem(), regionDir)) {
        families.add(familyDir.getName());
      }
    }
    return families;
  }

  protected ColumnFamilyDescriptor getTestRestoreSchemaChangeHCD() {
    return ColumnFamilyDescriptorBuilder.of(TEST_FAMILY2);
  }

  @Test
  public void testRestoreSchemaChange() throws Exception {
    Table table = TEST_UTIL.getConnection().getTable(tableName);

    // Add one column family and put some data in it
    admin.disableTable(tableName);
    admin.addColumnFamily(tableName, getTestRestoreSchemaChangeHCD());
    admin.enableTable(tableName);
    assertEquals(2, table.getDescriptor().getColumnFamilyCount());
    TableDescriptor htd = admin.getDescriptor(tableName);
    assertEquals(2, htd.getColumnFamilyCount());
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, TEST_FAMILY2);
    long snapshot2Rows = snapshot1Rows + 500L;
    assertEquals(snapshot2Rows, countRows(table));
    assertEquals(500, countRows(table, TEST_FAMILY2));
    Set<String> fsFamilies = getFamiliesFromFS(tableName);
    assertEquals(2, fsFamilies.size());

    // Take a snapshot
    admin.disableTable(tableName);
    admin.snapshot(snapshotName2, tableName);

    // Restore the snapshot (without the cf)
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    assertEquals(1, table.getDescriptor().getColumnFamilyCount());
    try {
      countRows(table, TEST_FAMILY2);
      fail("family '" + Bytes.toString(TEST_FAMILY2) + "' should not exists");
    } catch (NoSuchColumnFamilyException e) {
      // expected
    }
    assertEquals(snapshot0Rows, countRows(table));
    htd = admin.getDescriptor(tableName);
    assertEquals(1, htd.getColumnFamilyCount());
    fsFamilies = getFamiliesFromFS(tableName);
    assertEquals(1, fsFamilies.size());

    // Restore back the snapshot (with the cf)
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName2);
    admin.enableTable(tableName);
    htd = admin.getDescriptor(tableName);
    assertEquals(2, htd.getColumnFamilyCount());
    assertEquals(2, table.getDescriptor().getColumnFamilyCount());
    assertEquals(500, countRows(table, TEST_FAMILY2));
    assertEquals(snapshot2Rows, countRows(table));
    fsFamilies = getFamiliesFromFS(tableName);
    assertEquals(2, fsFamilies.size());
    table.close();
  }
}
