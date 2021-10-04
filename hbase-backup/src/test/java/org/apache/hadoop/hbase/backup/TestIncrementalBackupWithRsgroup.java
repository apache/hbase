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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
@RunWith(Parameterized.class)
public class TestIncrementalBackupWithRsgroup extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncrementalBackupWithRsgroup.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestIncrementalBackupWithRsgroup.class);

  public TestIncrementalBackupWithRsgroup(Boolean b) {
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[] { Boolean.TRUE });
    return params;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf1 = TEST_UTIL.getConfiguration();
    enableRSgroup = true;
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    setUpHelper();
  }

  // implement all test cases in 1 test since incremental
  // backup/restore has dependencies
  @Test
  public void TestIncBackupRestore() throws Exception {
    int ADD_ROWS = 99;

    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");
    List<TableName> tables = Lists.newArrayList(RSGROUP_TABLE_1, RSGROUP_TABLE_2);
    final byte[] fam3Name = Bytes.toBytes("f3");
    final byte[] mobName = Bytes.toBytes("mob");

    TableDescriptor newTable1Desc = TableDescriptorBuilder.newBuilder(RSGROUP_TABLE_1)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(famName))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam3Name))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(mobName).setMobEnabled(true)
        .setMobThreshold(5L).build())
      .build();
    TEST_UTIL.getAdmin().modifyTable(newTable1Desc);

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      int NB_ROWS_FAM3 = 6;
      insertIntoTable(conn, RSGROUP_TABLE_1, fam3Name, 3, NB_ROWS_FAM3).close();
      insertIntoTable(conn, RSGROUP_TABLE_1, mobName, 3, NB_ROWS_FAM3).close();
      Admin admin = conn.getAdmin();
      BackupAdminImpl client = new BackupAdminImpl(conn);
      BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
      String backupIdFull = client.backupTables(request).getBackupId();
      assertTrue(checkSucceeded(backupIdFull));

      // #2 - insert some data to table
      Table t1 = insertIntoTable(conn, RSGROUP_TABLE_1, famName, 1, ADD_ROWS);
      LOG.debug("writing " + ADD_ROWS + " rows to " + RSGROUP_TABLE_1);
      Assert.assertEquals(HBaseTestingUtil.countRows(t1),
        NB_ROWS_IN_BATCH + ADD_ROWS + NB_ROWS_FAM3);
      LOG.debug("written " + ADD_ROWS + " rows to " + RSGROUP_TABLE_1);
      // additionally, insert rows to MOB cf
      int NB_ROWS_MOB = 111;
      insertIntoTable(conn, RSGROUP_TABLE_1, mobName, 3, NB_ROWS_MOB);
      LOG.debug("written " + NB_ROWS_MOB + " rows to " + RSGROUP_TABLE_1 + " to Mob enabled CF");
      t1.close();
      Assert.assertEquals(HBaseTestingUtil.countRows(t1),
        NB_ROWS_IN_BATCH + ADD_ROWS + NB_ROWS_MOB);
      Table t2 = conn.getTable(RSGROUP_TABLE_2);
      Put p2;
      for (int i = 0; i < 5; i++) {
        p2 = new Put(Bytes.toBytes("row-t2" + i));
        p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        t2.put(p2);
      }
      Assert.assertEquals(NB_ROWS_IN_BATCH + 5, HBaseTestingUtil.countRows(t2));
      t2.close();
      LOG.debug("written " + 5 + " rows to " + RSGROUP_TABLE_2);
      // split RSGROUP_TABLE_1
      SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      List<HRegion> regions = cluster.getRegions(RSGROUP_TABLE_1);
      byte[] name = regions.get(0).getRegionInfo().getRegionName();
      long startSplitTime = EnvironmentEdgeManager.currentTime();
      try {
        admin.splitRegionAsync(name).get();
      } catch (Exception e) {
        // although split fail, this may not affect following check in current API,
        // exception will be thrown.
        LOG.debug("region is not splittable, because " + e);
      }
      while (!admin.isTableAvailable(RSGROUP_TABLE_1)) {
        Thread.sleep(100);
      }
      long endSplitTime = EnvironmentEdgeManager.currentTime();
      // split finished
      LOG.debug("split finished in =" + (endSplitTime - startSplitTime));

      // #3 - incremental backup for multiple tables
      tables = Lists.newArrayList(RSGROUP_TABLE_1, RSGROUP_TABLE_2);
      request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
      BackupInfo backupInfoIncMultiple = client.backupTables(request);
      String backupIdIncMultiple = backupInfoIncMultiple.getBackupId();
      assertTrue(checkSucceeded(backupIdIncMultiple));
      checkIfWALFilesBelongToRsgroup(backupInfoIncMultiple.getIncrBackupFileList(), RSGROUP_NAME);

      // add column family f2 to RSGROUP_TABLE_1
      // drop column family f3
      final byte[] fam2Name = Bytes.toBytes("f2");
      newTable1Desc = TableDescriptorBuilder.newBuilder(newTable1Desc)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam2Name)).removeColumnFamily(fam3Name)
        .build();
      TEST_UTIL.getAdmin().modifyTable(newTable1Desc);

      int NB_ROWS_FAM2 = 7;
      Table t3 = insertIntoTable(conn, RSGROUP_TABLE_1, fam2Name, 2, NB_ROWS_FAM2);
      t3.close();

      // Wait for 5 sec to make sure that old WALs were deleted
      Thread.sleep(5000);

      // #4 - additional incremental backup for multiple tables
      request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
      BackupInfo backupInfoIncMultiple2 = client.backupTables(request);
      String backupIdIncMultiple2 = backupInfoIncMultiple2.getBackupId();
      assertTrue(checkSucceeded(backupIdIncMultiple2));
      checkIfWALFilesBelongToRsgroup(backupInfoIncMultiple2.getIncrBackupFileList(), RSGROUP_NAME);

      // #5 - restore full backup for all tables
      TableName[] tablesRestoreFull = new TableName[] { RSGROUP_TABLE_1, RSGROUP_TABLE_2 };
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
      TableName[] tablesRestoreIncMultiple = new TableName[] { RSGROUP_TABLE_1, RSGROUP_TABLE_2 };
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
      admin.close();
    }
  }
}
