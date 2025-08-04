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
package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.LogRoller;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupRestoreExpiry extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupRestoreExpiry.class);

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.setLong(HConstants.DEFAULT_SNAPSHOT_TTL_CONFIG_KEY, 30);
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    setUpHelper();
  }

  public void ensurePreviousBackupTestsAreCleanedUp() throws Exception {
    TEST_UTIL.flush(table1);
    TEST_UTIL.flush(table2);

    TEST_UTIL.truncateTable(table1).close();
    TEST_UTIL.truncateTable(table2).close();

    if (TEST_UTIL.getAdmin().tableExists(table1_restore)) {
      TEST_UTIL.flush(table1_restore);
      TEST_UTIL.truncateTable(table1_restore).close();
    }

    TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().forEach(rst -> {
      try {
        LogRoller walRoller = rst.getRegionServer().getWalRoller();
        walRoller.requestRollAll();
        walRoller.waitUntilWalRollFinished();
      } catch (Exception ignored) {
      }
    });

    try (Table table = TEST_UTIL.getConnection().getTable(table1)) {
      loadTable(table);
    }

    try (Table table = TEST_UTIL.getConnection().getTable(table2)) {
      loadTable(table);
    }
  }

  @Test
  public void testSequentially() throws Exception {
    try {
      testRestoreOnExpiredFullBackup();
    } catch (Exception e) {
      throw e;
    } finally {
      ensurePreviousBackupTestsAreCleanedUp();
    }

    try {
      testIncrementalBackupOnExpiredFullBackup();
    } catch (Exception e) {
      throw e;
    } finally {
      ensurePreviousBackupTestsAreCleanedUp();
    }
  }

  public void testRestoreOnExpiredFullBackup() throws Exception {
    byte[] mobFam = Bytes.toBytes("mob");

    List<TableName> tables = Lists.newArrayList(table1);
    TableDescriptor newTable1Desc =
      TableDescriptorBuilder.newBuilder(table1Desc).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(mobFam).setMobEnabled(true).setMobThreshold(5L).build()).build();
    TEST_UTIL.getAdmin().modifyTable(newTable1Desc);

    Connection conn = TEST_UTIL.getConnection();
    BackupAdminImpl backupAdmin = new BackupAdminImpl(conn);
    BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
    String fullBackupId = backupAdmin.backupTables(request);
    assertTrue(checkSucceeded(fullBackupId));

    TableName[] fromTables = new TableName[] { table1 };
    TableName[] toTables = new TableName[] { table1_restore };

    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      // time + 30s
      @Override
      public long currentTime() {
        return System.currentTimeMillis() + (30 * 1000);
      }
    });

    assertThrows(SnapshotTTLExpiredException.class, () -> {
      backupAdmin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, fullBackupId, false,
        fromTables, toTables, true, true));
    });

    EnvironmentEdgeManager.reset();
    backupAdmin.close();
  }

  public void testIncrementalBackupOnExpiredFullBackup() throws Exception {
    byte[] mobFam = Bytes.toBytes("mob");

    List<TableName> tables = Lists.newArrayList(table1);
    TableDescriptor newTable1Desc =
      TableDescriptorBuilder.newBuilder(table1Desc).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(mobFam).setMobEnabled(true).setMobThreshold(5L).build()).build();
    TEST_UTIL.getAdmin().modifyTable(newTable1Desc);

    Connection conn = TEST_UTIL.getConnection();
    BackupAdminImpl backupAdmin = new BackupAdminImpl(conn);
    BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
    String fullBackupId = backupAdmin.backupTables(request);
    assertTrue(checkSucceeded(fullBackupId));

    TableName[] fromTables = new TableName[] { table1 };
    TableName[] toTables = new TableName[] { table1_restore };

    List<LocatedFileStatus> preRestoreBackupFiles = getBackupFiles();
    backupAdmin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, fullBackupId, false,
      fromTables, toTables, true, true));
    List<LocatedFileStatus> postRestoreBackupFiles = getBackupFiles();

    // Check that the backup files are the same before and after the restore process
    Assert.assertEquals(postRestoreBackupFiles, preRestoreBackupFiles);
    Assert.assertEquals(TEST_UTIL.countRows(table1_restore), NB_ROWS_IN_BATCH);

    int ROWS_TO_ADD = 1_000;
    // different IDs so that rows don't overlap
    insertIntoTable(conn, table1, famName, 3, ROWS_TO_ADD);
    insertIntoTable(conn, table1, mobFam, 4, ROWS_TO_ADD);

    Admin admin = conn.getAdmin();
    List<HRegion> currentRegions = TEST_UTIL.getHBaseCluster().getRegions(table1);
    for (HRegion region : currentRegions) {
      byte[] name = region.getRegionInfo().getEncodedNameAsBytes();
      admin.splitRegionAsync(name).get();
    }

    TEST_UTIL.waitTableAvailable(table1);

    // Make sure we've split regions
    assertNotEquals(currentRegions, TEST_UTIL.getHBaseCluster().getRegions(table1));

    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      // time + 30s
      @Override
      public long currentTime() {
        return System.currentTimeMillis() + (30 * 1000);
      }
    });

    IOException e = assertThrows(IOException.class, () -> {
      backupAdmin
        .backupTables(createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR));
    });
    assertTrue(e.getCause() instanceof SnapshotTTLExpiredException);

    EnvironmentEdgeManager.reset();
    backupAdmin.close();
  }

  private List<LocatedFileStatus> getBackupFiles() throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(BACKUP_ROOT_DIR), true);
    List<LocatedFileStatus> files = new ArrayList<>();

    while (iter.hasNext()) {
      files.add(iter.next());
    }

    return files;
  }
}
