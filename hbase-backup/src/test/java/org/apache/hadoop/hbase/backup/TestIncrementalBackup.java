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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category(LargeTests.class)
@RunWith(Parameterized.class)
public class TestIncrementalBackup extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncrementalBackup.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestIncrementalBackup.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    provider = "multiwal";
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[] { Boolean.TRUE });
    return params;
  }

  public TestIncrementalBackup(Boolean b) {
  }

  // implement all test cases in 1 test since incremental
  // backup/restore has dependencies
  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  public void TestIncBackupRestore() throws Exception {
    int ADD_ROWS = 99;

    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");
    List<TableName> tables = Lists.newArrayList(table1, table2);
    final byte[] fam3Name = Bytes.toBytes("f3");
    final byte[] mobName = Bytes.toBytes("mob");

    TableDescriptor newTable1Desc = TableDescriptorBuilder.newBuilder(table1Desc)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam3Name))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(mobName).setMobEnabled(true)
        .setMobThreshold(5L).build())
      .build();
    TEST_UTIL.getAdmin().modifyTable(newTable1Desc);

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      int NB_ROWS_FAM3 = 6;
      insertIntoTable(conn, table1, fam3Name, 3, NB_ROWS_FAM3).close();
      insertIntoTable(conn, table1, mobName, 3, NB_ROWS_FAM3).close();
      Admin admin = conn.getAdmin();
      BackupAdminImpl client = new BackupAdminImpl(conn);
      BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
      String backupIdFull = client.backupTables(request);
      assertTrue(checkSucceeded(backupIdFull));

      // #2 - insert some data to table
      Table t1 = insertIntoTable(conn, table1, famName, 1, ADD_ROWS);
      LOG.debug("writing " + ADD_ROWS + " rows to " + table1);
      Assert.assertEquals(HBaseTestingUtil.countRows(t1),
        NB_ROWS_IN_BATCH + ADD_ROWS + NB_ROWS_FAM3);
      LOG.debug("written " + ADD_ROWS + " rows to " + table1);
      // additionally, insert rows to MOB cf
      int NB_ROWS_MOB = 111;
      insertIntoTable(conn, table1, mobName, 3, NB_ROWS_MOB);
      LOG.debug("written " + NB_ROWS_MOB + " rows to " + table1 + " to Mob enabled CF");
      t1.close();
      Assert.assertEquals(HBaseTestingUtil.countRows(t1),
        NB_ROWS_IN_BATCH + ADD_ROWS + NB_ROWS_MOB);
      Table t2 = conn.getTable(table2);
      Put p2;
      for (int i = 0; i < 5; i++) {
        p2 = new Put(Bytes.toBytes("row-t2" + i));
        p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        t2.put(p2);
      }
      Assert.assertEquals(NB_ROWS_IN_BATCH + 5, HBaseTestingUtil.countRows(t2));
      t2.close();
      LOG.debug("written " + 5 + " rows to " + table2);
      // split table1
      SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      List<HRegion> regions = cluster.getRegions(table1);
      byte[] name = regions.get(0).getRegionInfo().getRegionName();
      long startSplitTime = EnvironmentEdgeManager.currentTime();
      try {
        admin.splitRegionAsync(name).get();
      } catch (Exception e) {
        // although split fail, this may not affect following check in current API,
        // exception will be thrown.
        LOG.debug("region is not splittable, because " + e);
      }
      while (!admin.isTableAvailable(table1)) {
        Thread.sleep(100);
      }
      long endSplitTime = EnvironmentEdgeManager.currentTime();
      // split finished
      LOG.debug("split finished in =" + (endSplitTime - startSplitTime));

      // #3 - incremental backup for multiple tables
      tables = Lists.newArrayList(table1, table2);
      request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
      String backupIdIncMultiple = client.backupTables(request);
      assertTrue(checkSucceeded(backupIdIncMultiple));
      BackupManifest manifest =
        HBackupFileSystem.getManifest(conf1, new Path(BACKUP_ROOT_DIR), backupIdIncMultiple);
      assertEquals(Sets.newHashSet(table1, table2), new HashSet<>(manifest.getTableList()));

      // add column family f2 to table1
      // drop column family f3
      final byte[] fam2Name = Bytes.toBytes("f2");
      newTable1Desc = TableDescriptorBuilder.newBuilder(newTable1Desc)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam2Name)).removeColumnFamily(fam3Name)
        .build();
      TEST_UTIL.getAdmin().modifyTable(newTable1Desc);

      int NB_ROWS_FAM2 = 7;
      Table t3 = insertIntoTable(conn, table1, fam2Name, 2, NB_ROWS_FAM2);
      t3.close();

      // Wait for 5 sec to make sure that old WALs were deleted
      Thread.sleep(5000);

      // #4 - additional incremental backup for multiple tables
      request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
      String backupIdIncMultiple2 = client.backupTables(request);
      assertTrue(checkSucceeded(backupIdIncMultiple2));

      // #5 - restore full backup for all tables
      TableName[] tablesRestoreFull = new TableName[] { table1, table2 };
      TableName[] tablesMapFull = new TableName[] { table1_restore, table2_restore };

      LOG.debug("Restoring full " + backupIdFull);
      client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupIdFull, false,
        tablesRestoreFull, tablesMapFull, true));

      // #6.1 - check tables for full restore
      Admin hAdmin = TEST_UTIL.getAdmin();
      assertTrue(hAdmin.tableExists(table1_restore));
      assertTrue(hAdmin.tableExists(table2_restore));
      hAdmin.close();

      // #6.2 - checking row count of tables for full restore
      Table hTable = conn.getTable(table1_restore);
      Assert.assertEquals(HBaseTestingUtil.countRows(hTable), NB_ROWS_IN_BATCH + NB_ROWS_FAM3);
      hTable.close();

      hTable = conn.getTable(table2_restore);
      Assert.assertEquals(NB_ROWS_IN_BATCH, HBaseTestingUtil.countRows(hTable));
      hTable.close();

      // #7 - restore incremental backup for multiple tables, with overwrite
      TableName[] tablesRestoreIncMultiple = new TableName[] { table1, table2 };
      TableName[] tablesMapIncMultiple = new TableName[] { table1_restore, table2_restore };
      client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupIdIncMultiple2, false,
        tablesRestoreIncMultiple, tablesMapIncMultiple, true));
      hTable = conn.getTable(table1_restore);

      LOG.debug("After incremental restore: " + hTable.getDescriptor());
      int countFamName = TEST_UTIL.countRows(hTable, famName);
      LOG.debug("f1 has " + countFamName + " rows");
      Assert.assertEquals(countFamName, NB_ROWS_IN_BATCH + ADD_ROWS);

      int countFam2Name = TEST_UTIL.countRows(hTable, fam2Name);
      LOG.debug("f2 has " + countFam2Name + " rows");
      Assert.assertEquals(countFam2Name, NB_ROWS_FAM2);

      int countMobName = TEST_UTIL.countRows(hTable, mobName);
      LOG.debug("mob has " + countMobName + " rows");
      Assert.assertEquals(countMobName, NB_ROWS_MOB);
      hTable.close();

      hTable = conn.getTable(table2_restore);
      Assert.assertEquals(NB_ROWS_IN_BATCH + 5, HBaseTestingUtil.countRows(hTable));
      hTable.close();

      // Test bulkloads
      newTable1Desc = TableDescriptorBuilder.newBuilder(newTable1Desc)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam3Name)).build();
      updateDesc(newTable1Desc, tables, conn);
      testBulkloads(conn, famName, mobName);
      admin.close();
    }
  }

  private void updateDesc(TableDescriptor desc, List<TableName> tablesToBackup, Connection conn)
    throws Exception {
    TEST_UTIL.getAdmin().modifyTable(desc);
    BackupAdminImpl backupAdmin = new BackupAdminImpl(conn);
    BackupRequest req = createBackupRequest(BackupType.FULL, tablesToBackup, BACKUP_ROOT_DIR);
    String backupId = backupAdmin.backupTables(req);
    checkSucceeded(backupId);
  }

  private void testBulkloads(Connection conn, byte[]... famsToLoad) throws Exception {
    BackupAdminImpl backupAdmin = new BackupAdminImpl(conn);
    List<HRegion> allRegions = TEST_UTIL.getHBaseCluster().getRegions(table1);
    HRegion regionToBulkload = allRegions.get(0);
    String regionName = regionToBulkload.getRegionInfo().getEncodedName();
    Path regionDir = createHFiles(table1, regionName, famsToLoad);
    doBulkload(table1, regionDir);
    TEST_UTIL.waitTableAvailable(table1);

    BackupRequest request =
      createBackupRequest(BackupType.INCREMENTAL, Lists.newArrayList(table1), BACKUP_ROOT_DIR);
    String incrementalBackupId = backupAdmin.backupTables(request);
    assertTrue(checkSucceeded(incrementalBackupId));
    TableName[] fromTables = new TableName[] { table1 };
    TableName[] toTables = new TableName[] { table1_restore };

    backupAdmin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, incrementalBackupId,
      false, fromTables, toTables, true));

    int rowsExpected = HBaseTestingUtil.countRows(conn.getTable(table1), famsToLoad);

    Table hTable = conn.getTable(table1_restore);
    Assert.assertEquals(HBaseTestingUtil.countRows(hTable, famsToLoad), rowsExpected);
  }

  private static Path createHFiles(TableName tn, String regionName, byte[]... fams)
    throws IOException {
    Path rootdir = CommonFSUtils.getRootDir(conf1);
    Path regionDir = CommonFSUtils.getRegionDir(rootdir, tn, regionName);

    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    fs.mkdirs(rootdir);

    for (byte[] fam : fams) {
      int rows = ThreadLocalRandom.current().nextInt(100);
      Path famDir = new Path(regionDir, Bytes.toString(fam));
      Path hFileLocation =
        new Path(famDir, UUID.randomUUID() + StoreFileInfo.formatBulkloadSeqId(15));

      try (HFile.Writer writer = HFile.getWriterFactoryNoCache(conf1).withPath(fs, hFileLocation)
        .withFileContext(
          new HFileContextBuilder().withTableName(tn.toBytes()).withColumnFamily(fam).build())
        .create()) {
        for (int i = 0; i < rows; i++) {
          byte[] row = Bytes.add(Bytes.toBytes(i), Bytes.toBytes(UUID.randomUUID().toString()));
          ExtendedCell cell =
            ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(row).setFamily(fam)
              .setQualifier(Bytes.toBytes(i)).setValue(row).setType(Cell.Type.Put).build();
          writer.append(cell);
        }
      }
      verifyHFileHasNRows(hFileLocation, rows);
    }

    return regionDir;
  }

  private static void verifyHFileHasNRows(Path path, int rows) throws IOException {
    FileSystem fs = FileSystem.get(conf1);
    try (HFile.Reader reader = HFile.createReader(fs, path, conf1)) {
      assertEquals(rows, reader.getEntries());
    }
  }

  private static void doBulkload(TableName tn, Path regionDir) throws IOException {
    Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> results =
      BulkLoadHFiles.create(conf1).bulkLoad(tn, regionDir);
    assertFalse(results.isEmpty());
  }
}
