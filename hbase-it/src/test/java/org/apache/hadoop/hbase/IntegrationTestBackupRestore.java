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

package org.apache.hadoop.hbase;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.BackupAdmin;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * An integration test to detect regressions in HBASE-7912. Create
 * a table with many regions, load data, perform series backup/load operations,
 * then restore and verify data
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-7912">HBASE-7912</a>
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-14123">HBASE-14123</a>
 */
@Category(IntegrationTests.class)
public class IntegrationTestBackupRestore extends IntegrationTestBase {
  private static final String CLASS_NAME = IntegrationTestBackupRestore.class.getSimpleName();
  protected static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBackupRestore.class);
  protected static final TableName TABLE_NAME1 = TableName.valueOf(CLASS_NAME + ".table1");
  protected static final TableName TABLE_NAME2 = TableName.valueOf(CLASS_NAME + ".table2");
  protected static final String COLUMN_NAME = "f";
  protected static final String REGION_COUNT_KEY = "regions_per_rs";
  protected static final String REGIONSERVER_COUNT_KEY = "region_servers";
  protected static final int DEFAULT_REGION_COUNT = 10;
  protected static final int DEFAULT_REGIONSERVER_COUNT = 2;
  protected static int regionsCountPerServer;
  protected static int regionServerCount;
  protected static final String NB_ROWS_IN_BATCH_KEY = "rows_in_batch";
  protected static final int DEFAULT_NB_ROWS_IN_BATCH = 20000;
  private static int rowsInBatch;
  private static String BACKUP_ROOT_DIR = "backupIT";

  @Override
  @Before
  public void setUp() throws Exception {
    util = new IntegrationTestingUtility();
    Configuration conf = util.getConfiguration();
    regionsCountPerServer = conf.getInt(REGION_COUNT_KEY, DEFAULT_REGION_COUNT);
    regionServerCount =
        conf.getInt(REGIONSERVER_COUNT_KEY, DEFAULT_REGIONSERVER_COUNT);
    rowsInBatch = conf.getInt(NB_ROWS_IN_BATCH_KEY, DEFAULT_NB_ROWS_IN_BATCH);
    enableBackup(conf);
    LOG.info(String.format("Initializing cluster with %d region servers.", regionServerCount));
    util.initializeCluster(regionServerCount);
    LOG.info("Cluster initialized");
    util.deleteTableIfAny(TABLE_NAME1);
    util.deleteTableIfAny(TABLE_NAME2);
    LOG.info("Cluster ready");
  }

  @After
  public void tearDown() throws IOException {
    LOG.info("Cleaning up after test.");
    if(util.isDistributedCluster()) {
      util.deleteTableIfAny(TABLE_NAME1);
      LOG.info("Cleaning up after test. TABLE1 done");
      util.deleteTableIfAny(TABLE_NAME2);
      LOG.info("Cleaning up after test. TABLE2 done");
      cleanUpBackupDir();
    }
    LOG.info("Restoring cluster.");
    util.restoreCluster();
    LOG.info("Cluster restored.");
  }

  private void enableBackup(Configuration conf) {
    // Enable backup
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    BackupManager.decorateMasterConfiguration(conf);
    BackupManager.decorateRegionServerConfiguration(conf);
  }

  private void cleanUpBackupDir() throws IOException {
    FileSystem fs = FileSystem.get(util.getConfiguration());
    fs.delete(new Path(BACKUP_ROOT_DIR), true);
  }

  @Test
  public void testBackupRestore() throws Exception {
    BACKUP_ROOT_DIR = util.getDataTestDirOnTestFS() + Path.SEPARATOR + BACKUP_ROOT_DIR;
    createTable(TABLE_NAME1);
    createTable(TABLE_NAME2);
    runTest();
  }

  private void createTable(TableName tableName) throws Exception {
    long startTime, endTime;
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor[] columns =
        new HColumnDescriptor[]{new HColumnDescriptor(COLUMN_NAME)};
    LOG.info(String.format("Creating table %s with %d splits.", tableName,
      regionsCountPerServer));
    startTime = System.currentTimeMillis();
    HBaseTestingUtility.createPreSplitLoadTestTable(util.getConfiguration(), desc, columns,
      regionsCountPerServer);
    util.waitTableAvailable(tableName);
    endTime = System.currentTimeMillis();
    LOG.info(String.format("Pre-split table created successfully in %dms.",
      (endTime - startTime)));
  }

  private void loadData(TableName table, int numRows) throws IOException {
    Connection conn = util.getConnection();
    // #0- insert some data to a table
    Table t1 = conn.getTable(table);
    util.loadRandomRows(t1, new byte[]{'f'}, 100, numRows);
    // flush table
    conn.getAdmin().flush(TableName.valueOf(table.getName()));
  }

  private void runTest() throws IOException {
    // Check if backup is enabled
    if (!BackupManager.isBackupEnabled(getConf())) {
      LOG.error(BackupRestoreConstants.ENABLE_BACKUP);
      System.exit(EXIT_FAILURE);
    }

    LOG.info(BackupRestoreConstants.VERIFY_BACKUP);

    try (Connection conn = util.getConnection();
         Admin admin = conn.getAdmin();
         BackupAdmin client = new BackupAdminImpl(conn)) {
      // #0- insert some data to table TABLE_NAME1, TABLE_NAME2
      loadData(TABLE_NAME1, rowsInBatch);
      loadData(TABLE_NAME2, rowsInBatch);
      // #1 - create full backup for all tables
      LOG.info("create full backup image for all tables");
      List<TableName> tables = Lists.newArrayList(TABLE_NAME1, TABLE_NAME2);

      BackupRequest.Builder builder = new BackupRequest.Builder();
      BackupRequest request =
          builder.withBackupType(BackupType.FULL).withTableList(tables)
              .withTargetRootDir(BACKUP_ROOT_DIR).build();
      String backupIdFull = client.backupTables(request);
      assertTrue(checkSucceeded(backupIdFull));
      // #2 - insert some data to table
      loadData(TABLE_NAME1, rowsInBatch);
      loadData(TABLE_NAME2, rowsInBatch);

      try (HTable t1 = (HTable) conn.getTable(TABLE_NAME1)) {
        Assert.assertEquals(util.countRows(t1), rowsInBatch * 2);
      }
      try (HTable t2 = (HTable) conn.getTable(TABLE_NAME2)) {
        Assert.assertEquals(util.countRows(t2), rowsInBatch * 2);
      }

      // #3 - incremental backup for tables
      tables = Lists.newArrayList(TABLE_NAME1, TABLE_NAME2);
      builder = new BackupRequest.Builder();
      request =
          builder.withBackupType(BackupType.INCREMENTAL).withTableList(tables)
              .withTargetRootDir(BACKUP_ROOT_DIR).build();
      String backupIdIncMultiple = client.backupTables(request);
      assertTrue(checkSucceeded(backupIdIncMultiple));
      // #4 - restore full backup for all tables, without overwrite
      TableName[] tablesRestoreFull = new TableName[] { TABLE_NAME1, TABLE_NAME2 };
      client.restore(createRestoreRequest(BACKUP_ROOT_DIR, backupIdFull, false, tablesRestoreFull,
        null, true));
      // #5.1 - check tables for full restore
      assertTrue(admin.tableExists(TABLE_NAME1));
      assertTrue(admin.tableExists(TABLE_NAME2));
      // #5.2 - checking row count of tables for full restore
      HTable hTable = (HTable) conn.getTable(TABLE_NAME1);
      Assert.assertEquals(util.countRows(hTable), rowsInBatch);
      hTable.close();
      hTable = (HTable) conn.getTable(TABLE_NAME2);
      Assert.assertEquals(util.countRows(hTable), rowsInBatch);
      hTable.close();
      // #6 - restore incremental backup for multiple tables, with overwrite
      TableName[] tablesRestoreIncMultiple = new TableName[] { TABLE_NAME1, TABLE_NAME2 };
      client.restore(createRestoreRequest(BACKUP_ROOT_DIR, backupIdIncMultiple, false,
        tablesRestoreIncMultiple, null, true));
      hTable = (HTable) conn.getTable(TABLE_NAME1);
      Assert.assertEquals(util.countRows(hTable), rowsInBatch * 2);
      hTable.close();
      hTable = (HTable) conn.getTable(TABLE_NAME2);
      Assert.assertEquals(util.countRows(hTable), rowsInBatch * 2);
      hTable.close();
    }
  }

  /**
   *
   * @param backupId pass backup ID to check status of
   * @return status of backup
   */
  protected boolean checkSucceeded(String backupId) throws IOException {
    BackupInfo status = getBackupInfo(backupId);
    if (status == null) return false;
    return status.getState() == BackupState.COMPLETE;
  }

  private BackupInfo getBackupInfo(String backupId) throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(util.getConnection())) {
      return table.readBackupInfo(backupId);
    }
  }

  /**
   * Get restore request.
   *
   * @param backupRootDir directory where backup is located
   * @param backupId backup ID
   * @param check check the backup
   * @param fromTables table names to restore from
   * @param toTables new table names to restore to
   * @param isOverwrite overwrite the table(s)
   * @return an instance of RestoreRequest
   */
  public RestoreRequest createRestoreRequest(String backupRootDir, String backupId, boolean check,
      TableName[] fromTables, TableName[] toTables, boolean isOverwrite) {
    RestoreRequest.Builder builder = new RestoreRequest.Builder();
    return builder.withBackupRootDir(backupRootDir)
                                    .withBackupId(backupId)
                                    .withCheck(check)
                                    .withFromTables(fromTables)
                                    .withToTables(toTables)
                                    .withOvewrite(isOverwrite).build();
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    enableBackup(getConf());
    LOG.debug("Initializing/checking cluster has " + regionServerCount + " servers");
    util.initializeCluster(regionServerCount);
    LOG.debug("Done initializing/checking cluster");
  }

  /**
   *
   * @return status of CLI execution
   */
  @Override
  public int runTestFromCommandLine() throws Exception {
     // Check if backup is enabled
    if (!BackupManager.isBackupEnabled(getConf())) {
      System.err.println(BackupRestoreConstants.ENABLE_BACKUP);
      return -1;
    }

    System.out.println(BackupRestoreConstants.VERIFY_BACKUP);

    testBackupRestore();
    return 0;
  }

  @Override
  public TableName getTablename() {
    // That is only valid when Monkey is CALM (no monkey)
    return null;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    // That is only valid when Monkey is CALM (no monkey)
    return null;
  }

  @Override
  protected void addOptions() {
    addOptWithArg(REGIONSERVER_COUNT_KEY, "Total number of region servers. Default: '"
        + DEFAULT_REGIONSERVER_COUNT + "'");
    addOptWithArg(REGION_COUNT_KEY, "Total number of regions. Default: " + DEFAULT_REGION_COUNT);
    addOptWithArg(NB_ROWS_IN_BATCH_KEY, "Total number of data rows to be loaded (per table/batch."
        + " Total number of batches=2). Default: " + DEFAULT_NB_ROWS_IN_BATCH);

  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    regionsCountPerServer =
        Integer.parseInt(cmd.getOptionValue(REGION_COUNT_KEY,
          Integer.toString(DEFAULT_REGION_COUNT)));
    regionServerCount =
        Integer.parseInt(cmd.getOptionValue(REGIONSERVER_COUNT_KEY,
          Integer.toString(DEFAULT_REGIONSERVER_COUNT)));
    rowsInBatch =
        Integer.parseInt(cmd.getOptionValue(NB_ROWS_IN_BATCH_KEY,
          Integer.toString(DEFAULT_NB_ROWS_IN_BATCH)));
    LOG.info(MoreObjects.toStringHelper("Parsed Options").
      add(REGION_COUNT_KEY, regionsCountPerServer)
        .add(REGIONSERVER_COUNT_KEY, regionServerCount).add(NB_ROWS_IN_BATCH_KEY, rowsInBatch)
        .toString());
  }

  /**
   *
   * @param args argument list
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestBackupRestore(), args);
    System.exit(status);
  }
}
