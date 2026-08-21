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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the restore/clone operation from a file-system point of view.
 */
@Tag(MediumTests.TAG)
public class TestMobRestoreSnapshotHelper extends TestRestoreSnapshotHelper {

  final Logger LOG = LoggerFactory.getLogger(getClass());

  @Override
  protected void setupConf(Configuration conf) {
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @Override
  protected SnapshotMock createSnapshotMock() throws IOException {
    return new SnapshotMock(TEST_UTIL.getConfiguration(), fs, rootDir);
  }

  @Override
  protected void createTableAndSnapshot(TableName tableName, String snapshotName)
    throws IOException {
    byte[] column = Bytes.toBytes("A");
    Table table = MobSnapshotTestingUtils.createMobTable(TEST_UTIL, tableName, column);
    TEST_UTIL.loadTable(table, column);
    TEST_UTIL.getAdmin().snapshot(snapshotName, tableName);
  }

  @Test
  public void testRestoreMobTableFromSnapshot() throws IOException {
    TableName mobTableName = TableName.valueOf("testRestoreMobTable");
    String snapshotName = "testRestoreMobTable_snapshot";
    createTableAndSnapshot(mobTableName, snapshotName);
    assertTrue(TEST_UTIL.getAdmin().tableExists(mobTableName));
    assertTrue(MobUtils.hasMobColumns(TEST_UTIL.getAdmin().getDescriptor(mobTableName)));
    assertTrue(TEST_UTIL.getAdmin().listSnapshots().stream()
      .anyMatch(snapshotDescription -> snapshotName.equals(snapshotDescription.getName())));

    // Clone the snapshot to a new MOB table
    TableName newMobTableName = TableName.valueOf("newTestRestoreMobTable");
    TEST_UTIL.getAdmin().cloneSnapshot(snapshotName, newMobTableName);
    assertTrue(TEST_UTIL.getAdmin().tableExists(newMobTableName));
    assertTrue(MobUtils.hasMobColumns(TEST_UTIL.getAdmin().getDescriptor(newMobTableName)));

    Path hbaseRootDir = TEST_UTIL.getDefaultRootDirPath();
    Path mobRegionPath = MobUtils.getMobTableDir(hbaseRootDir, newMobTableName);
    assertTrue(fs.exists(mobRegionPath));
    Path errorMobRegionPathInTableDir =
      new Path(CommonFSUtils.getTableDir(hbaseRootDir, newMobTableName),
        MobUtils.getMobRegionInfo(newMobTableName).getEncodedName());
    assertFalse(fs.exists(errorMobRegionPathInTableDir));

    try (Table originMobTable = TEST_UTIL.getConnection().getTable(mobTableName);
      Table clonedMobTable = TEST_UTIL.getConnection().getTable(newMobTableName)) {
      assertEquals(HBaseTestingUtil.countRows(originMobTable),
        HBaseTestingUtil.countRows(clonedMobTable));
    }

    // Delete the original MOB table and restore it from the snapshot
    TEST_UTIL.deleteTable(mobTableName);
    assertFalse(TEST_UTIL.getAdmin().tableExists(mobTableName));
    TEST_UTIL.getAdmin().cloneSnapshot(snapshotName, mobTableName);
    assertTrue(TEST_UTIL.getAdmin().tableExists(mobTableName));
    assertTrue(MobUtils.hasMobColumns(TEST_UTIL.getAdmin().getDescriptor(mobTableName)));
    mobRegionPath = MobUtils.getMobTableDir(hbaseRootDir, mobTableName);
    assertTrue(fs.exists(mobRegionPath));
    errorMobRegionPathInTableDir =
      new Path(CommonFSUtils.getTableDir(hbaseRootDir, mobTableName),
        MobUtils.getMobRegionInfo(mobTableName).getEncodedName());
    assertFalse(fs.exists(errorMobRegionPathInTableDir));
  }
}
