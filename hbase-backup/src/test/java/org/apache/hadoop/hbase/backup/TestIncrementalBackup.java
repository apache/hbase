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
package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
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
  @Test
  public void TestIncBackupRestore() throws Exception {
    int ADD_ROWS = 99;

    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");
    List<TableName> tables = Lists.newArrayList(table1, table2);
    final byte[] fam3Name = Bytes.toBytes("f3");
    final byte[] mobName = Bytes.toBytes("mob");

    table1Desc.addFamily(new HColumnDescriptor(fam3Name));
    HColumnDescriptor mobHcd = new HColumnDescriptor(mobName);
    mobHcd.setMobEnabled(true);
    mobHcd.setMobThreshold(5L);
    table1Desc.addFamily(mobHcd);
    HBaseTestingUtility.modifyTableSync(TEST_UTIL.getAdmin(), table1Desc);

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      int NB_ROWS_FAM3 = 6;
      insertIntoTable(conn, table1, fam3Name, 3, NB_ROWS_FAM3).close();
      insertIntoTable(conn, table1, mobName, 3, NB_ROWS_FAM3).close();
      HBaseAdmin admin = null;
      admin = (HBaseAdmin) conn.getAdmin();
      BackupAdminImpl client = new BackupAdminImpl(conn);
      BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
      String backupIdFull = client.backupTables(request);
      assertTrue(checkSucceeded(backupIdFull));

      // #2 - insert some data to table
      Table t1 = insertIntoTable(conn, table1, famName, 1, ADD_ROWS);
      LOG.debug("writing " + ADD_ROWS + " rows to " + table1);
      Assert.assertEquals(HBaseTestingUtility.countRows(t1),
              NB_ROWS_IN_BATCH + ADD_ROWS + NB_ROWS_FAM3);
      LOG.debug("written " + ADD_ROWS + " rows to " + table1);

      // additionally, insert rows to MOB cf
      int NB_ROWS_MOB = 111;
      insertIntoTable(conn, table1, mobName, 3, NB_ROWS_MOB);
      LOG.debug("written " + NB_ROWS_MOB + " rows to " + table1 + " to Mob enabled CF");
      t1.close();
      Assert.assertEquals(HBaseTestingUtility.countRows(t1),
              NB_ROWS_IN_BATCH + ADD_ROWS + NB_ROWS_MOB);

      Table t2 = conn.getTable(table2);
      Put p2;
      for (int i = 0; i < 5; i++) {
        p2 = new Put(Bytes.toBytes("row-t2" + i));
        p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        t2.put(p2);
      }
      Assert.assertEquals(NB_ROWS_IN_BATCH + 5, HBaseTestingUtility.countRows(t2));
      t2.close();
      LOG.debug("written " + 5 + " rows to " + table2);

      // split table1
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
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

      // add column family f2 to table1
      final byte[] fam2Name = Bytes.toBytes("f2");
      table1Desc.addFamily(new HColumnDescriptor(fam2Name));

      // drop column family f3
      table1Desc.removeFamily(fam3Name);
      HBaseTestingUtility.modifyTableSync(TEST_UTIL.getAdmin(), table1Desc);

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
      HBaseAdmin hAdmin = TEST_UTIL.getHBaseAdmin();
      assertTrue(hAdmin.tableExists(table1_restore));
      assertTrue(hAdmin.tableExists(table2_restore));
      hAdmin.close();

      // #6.2 - checking row count of tables for full restore
      Table hTable = conn.getTable(table1_restore);
      Assert.assertEquals(HBaseTestingUtility.countRows(hTable), NB_ROWS_IN_BATCH + NB_ROWS_FAM3);
      hTable.close();

      hTable = conn.getTable(table2_restore);
      Assert.assertEquals(NB_ROWS_IN_BATCH, HBaseTestingUtility.countRows(hTable));
      hTable.close();

      // #7 - restore incremental backup for multiple tables, with overwrite
      TableName[] tablesRestoreIncMultiple = new TableName[] { table1, table2 };
      TableName[] tablesMapIncMultiple = new TableName[] { table1_restore, table2_restore };
      client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupIdIncMultiple2,
              false, tablesRestoreIncMultiple, tablesMapIncMultiple, true));
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
      Assert.assertEquals(NB_ROWS_IN_BATCH + 5, HBaseTestingUtility.countRows(hTable));
      hTable.close();
      admin.close();
    }
  }
}