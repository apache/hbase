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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.IntegrationTestingUtility.createPreSplitLoadTestTable;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
import org.apache.hadoop.hbase.chaos.actions.RestartRandomRsExceptMetaAction;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;
import org.apache.hadoop.hbase.chaos.policies.Policy;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * An integration test to detect regressions in HBASE-7912. Create a table with many regions, load
 * data, perform series backup/load operations, then restore and verify data
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-7912">HBASE-7912</a>
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-14123">HBASE-14123</a>
 */
@Category(IntegrationTests.class)
public class IntegrationTestBackupRestore extends IntegrationTestBase {
  private static final String CLASS_NAME = IntegrationTestBackupRestore.class.getSimpleName();
  protected static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBackupRestore.class);
  protected static final String NUMBER_OF_TABLES_KEY = "num_tables";
  protected static final String COLUMN_NAME = "f";
  protected static final String REGION_COUNT_KEY = "regions_per_rs";
  protected static final String REGIONSERVER_COUNT_KEY = "region_servers";
  protected static final String ROWS_PER_ITERATION_KEY = "rows_in_iteration";
  protected static final String NUM_ITERATIONS_KEY = "num_iterations";
  protected static final int DEFAULT_REGION_COUNT = 10;
  protected static final int DEFAULT_REGIONSERVER_COUNT = 5;
  protected static final int DEFAULT_NUMBER_OF_TABLES = 1;
  protected static final int DEFAULT_NUM_ITERATIONS = 10;
  protected static final int DEFAULT_ROWS_IN_ITERATION = 500000;
  protected static final String SLEEP_TIME_KEY = "sleeptime";
  // short default interval because tests don't run very long.
  protected static final long SLEEP_TIME_DEFAULT = 50000L;

  protected static int rowsInIteration;
  protected static int regionsCountPerServer;
  protected static int regionServerCount;

  protected static int numIterations;
  protected static int numTables;
  protected static TableName[] tableNames;
  protected long sleepTime;
  protected static Object lock = new Object();

  private static String BACKUP_ROOT_DIR = "backupIT";

  /*
   * This class is used to run the backup and restore thread(s). Throwing an exception in this
   * thread will not cause the test to fail, so the purpose of this class is to both kick off the
   * backup and restore and record any exceptions that occur so they can be thrown in the main
   * thread.
   */
  protected class BackupAndRestoreThread implements Runnable {
    private final TableName table;
    private Exception exc;

    public BackupAndRestoreThread(TableName table) {
      this.table = table;
      this.exc = null;
    }

    public Exception getException() {
      return this.exc;
    }

    @Override
    public void run() {
      try {
        runTestSingle(this.table);
      } catch (Exception e) {
        LOG.error(
          "An exception occurred in thread {} when performing a backup and restore with table {}: ",
          Thread.currentThread().getName(), this.table.getNameAsString(), e);
        this.exc = e;
      }
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    util = new IntegrationTestingUtility();
    Configuration conf = util.getConfiguration();
    regionsCountPerServer = conf.getInt(REGION_COUNT_KEY, DEFAULT_REGION_COUNT);
    regionServerCount = conf.getInt(REGIONSERVER_COUNT_KEY, DEFAULT_REGIONSERVER_COUNT);
    rowsInIteration = conf.getInt(ROWS_PER_ITERATION_KEY, DEFAULT_ROWS_IN_ITERATION);
    numIterations = conf.getInt(NUM_ITERATIONS_KEY, DEFAULT_NUM_ITERATIONS);
    numTables = conf.getInt(NUMBER_OF_TABLES_KEY, DEFAULT_NUMBER_OF_TABLES);
    sleepTime = conf.getLong(SLEEP_TIME_KEY, SLEEP_TIME_DEFAULT);
    enableBackup(conf);
    LOG.info("Initializing cluster with {} region servers.", regionServerCount);
    util.initializeCluster(regionServerCount);
    LOG.info("Cluster initialized and ready");
  }

  @After
  public void tearDown() throws IOException {
    LOG.info("Cleaning up after test.");
    if (util.isDistributedCluster()) {
      deleteTablesIfAny();
      LOG.info("Cleaning up after test. Deleted tables");
      cleanUpBackupDir();
    }
    LOG.info("Restoring cluster.");
    util.restoreCluster();
    LOG.info("Cluster restored.");
  }

  @Override
  public void setUpMonkey() throws Exception {
    Policy p =
      new PeriodicRandomActionPolicy(sleepTime, new RestartRandomRsExceptMetaAction(sleepTime));
    this.monkey = new PolicyBasedChaosMonkey(util, p);
    startMonkey();
  }

  private void deleteTablesIfAny() throws IOException {
    for (TableName table : tableNames) {
      util.deleteTableIfAny(table);
    }
  }

  private void createTables() throws Exception {
    tableNames = new TableName[numTables];
    for (int i = 0; i < numTables; i++) {
      tableNames[i] = TableName.valueOf(CLASS_NAME + ".table." + i);
    }
    for (TableName table : tableNames) {
      createTable(table);
    }
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
    createTables();
    runTestMulti();
  }

  private void runTestMulti() throws Exception {
    LOG.info("IT backup & restore started");
    Thread[] workers = new Thread[numTables];
    BackupAndRestoreThread[] backupAndRestoreThreads = new BackupAndRestoreThread[numTables];
    for (int i = 0; i < numTables; i++) {
      final TableName table = tableNames[i];
      BackupAndRestoreThread backupAndRestoreThread = new BackupAndRestoreThread(table);
      backupAndRestoreThreads[i] = backupAndRestoreThread;
      workers[i] = new Thread(backupAndRestoreThread);
      workers[i].start();
    }
    // Wait for all workers to finish and check for errors
    Exception error = null;
    Exception threadExc;
    for (int i = 0; i < numTables; i++) {
      Uninterruptibles.joinUninterruptibly(workers[i]);
      threadExc = backupAndRestoreThreads[i].getException();
      if (threadExc == null) {
        continue;
      }
      if (error == null) {
        error = threadExc;
      } else {
        error.addSuppressed(threadExc);
      }
    }
    // Throw any found errors after all threads have completed
    if (error != null) {
      throw error;
    }
    LOG.info("IT backup & restore finished");
  }

  private void createTable(TableName tableName) throws Exception {
    long startTime, endTime;

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);

    TableDescriptor desc = builder.build();
    ColumnFamilyDescriptorBuilder cbuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_NAME.getBytes(Charset.defaultCharset()));
    ColumnFamilyDescriptor[] columns = new ColumnFamilyDescriptor[] { cbuilder.build() };
    LOG.info("Creating table {} with {} splits.", tableName,
      regionsCountPerServer * regionServerCount);
    startTime = EnvironmentEdgeManager.currentTime();
    createPreSplitLoadTestTable(util.getConfiguration(), desc, columns, regionsCountPerServer);
    util.waitTableAvailable(tableName);
    endTime = EnvironmentEdgeManager.currentTime();
    LOG.info("Pre-split table created successfully in {}ms.", (endTime - startTime));
  }

  private void loadData(TableName table, int numRows) throws IOException {
    Connection conn = util.getConnection();
    // #0- insert some data to a table
    Table t1 = conn.getTable(table);
    util.loadRandomRows(t1, new byte[] { 'f' }, 100, numRows);
    // flush table
    conn.getAdmin().flush(TableName.valueOf(table.getName()));
  }

  private String backup(BackupRequest request, BackupAdmin client) throws IOException {
    return client.backupTables(request);
  }

  private void restore(RestoreRequest request, BackupAdmin client) throws IOException {
    client.restore(request);
  }

  private void merge(String[] backupIds, BackupAdmin client) throws IOException {
    client.mergeBackups(backupIds);
  }

  private void runTestSingle(TableName table) throws IOException {

    List<String> backupIds = new ArrayList<String>();

    try (Connection conn = util.getConnection(); Admin admin = conn.getAdmin();
      BackupAdmin client = new BackupAdminImpl(conn);) {

      // #0- insert some data to table 'table'
      loadData(table, rowsInIteration);

      // #1 - create full backup for table first
      LOG.info("create full backup image for {}", table);
      List<TableName> tables = Lists.newArrayList(table);
      BackupRequest.Builder builder = new BackupRequest.Builder();
      BackupRequest request = builder.withBackupType(BackupType.FULL).withTableList(tables)
        .withTargetRootDir(BACKUP_ROOT_DIR).build();

      String backupIdFull = backup(request, client);
      assertTrue(checkSucceeded(backupIdFull));

      backupIds.add(backupIdFull);
      // Now continue with incremental backups
      int count = 1;
      while (count++ < numIterations) {

        // Load data
        loadData(table, rowsInIteration);
        // Do incremental backup
        builder = new BackupRequest.Builder();
        request = builder.withBackupType(BackupType.INCREMENTAL).withTableList(tables)
          .withTargetRootDir(BACKUP_ROOT_DIR).build();
        String backupId = backup(request, client);
        assertTrue(checkSucceeded(backupId));
        backupIds.add(backupId);

        // Restore incremental backup for table, with overwrite for previous backup
        String previousBackupId = backupIds.get(backupIds.size() - 2);
        restoreVerifyTable(conn, client, table, previousBackupId, rowsInIteration * (count - 1));
        // Restore incremental backup for table, with overwrite for last backup
        restoreVerifyTable(conn, client, table, backupId, rowsInIteration * count);
      }
      // Now merge all incremental and restore
      String[] incBackupIds = allIncremental(backupIds);
      merge(incBackupIds, client);
      // Restore last one
      String backupId = incBackupIds[incBackupIds.length - 1];
      // restore incremental backup for table, with overwrite
      TableName[] tablesRestoreIncMultiple = new TableName[] { table };
      restore(createRestoreRequest(BACKUP_ROOT_DIR, backupId, false, tablesRestoreIncMultiple, null,
        true), client);
      Table hTable = conn.getTable(table);
      Assert.assertEquals(util.countRows(hTable), rowsInIteration * numIterations);
      hTable.close();
      LOG.info("{} loop {} finished.", Thread.currentThread().getName(), (count - 1));
    }
  }

  private void restoreVerifyTable(Connection conn, BackupAdmin client, TableName table,
    String backupId, long expectedRows) throws IOException {
    TableName[] tablesRestoreIncMultiple = new TableName[] { table };
    restore(
      createRestoreRequest(BACKUP_ROOT_DIR, backupId, false, tablesRestoreIncMultiple, null, true),
      client);
    Table hTable = conn.getTable(table);
    Assert.assertEquals(expectedRows, util.countRows(hTable));
    hTable.close();
  }

  private String[] allIncremental(List<String> backupIds) {
    int size = backupIds.size();
    backupIds = backupIds.subList(1, size);
    String[] arr = new String[size - 1];
    backupIds.toArray(arr);
    return arr;
  }

  /** Returns status of backup */
  protected boolean checkSucceeded(String backupId) throws IOException {
    BackupInfo status = getBackupInfo(backupId);
    if (status == null) {
      return false;
    }
    return status.getState() == BackupState.COMPLETE;
  }

  private BackupInfo getBackupInfo(String backupId) throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(util.getConnection())) {
      return table.readBackupInfo(backupId);
    }
  }

  /**
   * Get restore request.
   * @param backupRootDir directory where backup is located
   * @param backupId      backup ID
   * @param check         check the backup
   * @param fromTables    table names to restore from
   * @param toTables      new table names to restore to
   * @param isOverwrite   overwrite the table(s)
   * @return an instance of RestoreRequest
   */
  public RestoreRequest createRestoreRequest(String backupRootDir, String backupId, boolean check,
    TableName[] fromTables, TableName[] toTables, boolean isOverwrite) {
    RestoreRequest.Builder builder = new RestoreRequest.Builder();
    return builder.withBackupRootDir(backupRootDir).withBackupId(backupId).withCheck(check)
      .withFromTables(fromTables).withToTables(toTables).withOvewrite(isOverwrite).build();
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    enableBackup(getConf());
    LOG.debug("Initializing/checking cluster has {} servers", regionServerCount);
    util.initializeCluster(regionServerCount);
    LOG.debug("Done initializing/checking cluster");
  }

  /** Returns status of CLI execution */
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
    addOptWithArg(REGIONSERVER_COUNT_KEY,
      "Total number of region servers. Default: '" + DEFAULT_REGIONSERVER_COUNT + "'");
    addOptWithArg(REGION_COUNT_KEY, "Total number of regions. Default: " + DEFAULT_REGION_COUNT);
    addOptWithArg(ROWS_PER_ITERATION_KEY,
      "Total number of data rows to be loaded during one iteration." + " Default: "
        + DEFAULT_ROWS_IN_ITERATION);
    addOptWithArg(NUM_ITERATIONS_KEY,
      "Total number iterations." + " Default: " + DEFAULT_NUM_ITERATIONS);
    addOptWithArg(NUMBER_OF_TABLES_KEY,
      "Total number of tables in the test." + " Default: " + DEFAULT_NUMBER_OF_TABLES);
    addOptWithArg(SLEEP_TIME_KEY, "Sleep time of chaos monkey in ms "
      + "to restart random region server. Default: " + SLEEP_TIME_DEFAULT);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    regionsCountPerServer = Integer
      .parseInt(cmd.getOptionValue(REGION_COUNT_KEY, Integer.toString(DEFAULT_REGION_COUNT)));
    regionServerCount = Integer.parseInt(
      cmd.getOptionValue(REGIONSERVER_COUNT_KEY, Integer.toString(DEFAULT_REGIONSERVER_COUNT)));
    rowsInIteration = Integer.parseInt(
      cmd.getOptionValue(ROWS_PER_ITERATION_KEY, Integer.toString(DEFAULT_ROWS_IN_ITERATION)));
    numIterations = Integer
      .parseInt(cmd.getOptionValue(NUM_ITERATIONS_KEY, Integer.toString(DEFAULT_NUM_ITERATIONS)));
    numTables = Integer.parseInt(
      cmd.getOptionValue(NUMBER_OF_TABLES_KEY, Integer.toString(DEFAULT_NUMBER_OF_TABLES)));
    sleepTime =
      Long.parseLong(cmd.getOptionValue(SLEEP_TIME_KEY, Long.toString(SLEEP_TIME_DEFAULT)));

    LOG.info(MoreObjects.toStringHelper("Parsed Options")
      .add(REGION_COUNT_KEY, regionsCountPerServer).add(REGIONSERVER_COUNT_KEY, regionServerCount)
      .add(ROWS_PER_ITERATION_KEY, rowsInIteration).add(NUM_ITERATIONS_KEY, numIterations)
      .add(NUMBER_OF_TABLES_KEY, numTables).add(SLEEP_TIME_KEY, sleepTime).toString());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestBackupRestore(), args);
    System.exit(status);
  }
}
