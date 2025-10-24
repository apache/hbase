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

import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_BACKUP_MAX_WAL_SIZE;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_STAGED_WAL_FLUSH_INITIAL_DELAY;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_STAGED_WAL_FLUSH_INTERVAL;
import static org.apache.hadoop.hbase.mapreduce.WALPlayer.IGNORE_EMPTY_FILES;
import static org.apache.hadoop.hbase.mapreduce.WALPlayer.IGNORE_MISSING_FILES;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * An integration test to detect regressions in HBASE-28957. Create a table with many regions, load
 * data, perform series backup/load operations with continuous backup enabled, then restore and
 * verify data.
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-28957">HBASE-28957</a>
 */
@Category(IntegrationTests.class)
public class IntegrationTestContinuousBackupRestore extends IntegrationTestBackupRestoreBase {
  private static final String CLASS_NAME =
    IntegrationTestContinuousBackupRestore.class.getSimpleName();
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestContinuousBackupRestore.class);

  @Override
  @Before
  public void setUp() throws Exception {
    util = new IntegrationTestingUtility();
    conf = util.getConfiguration();
    regionsCountPerServer = conf.getInt(REGION_COUNT_KEY, DEFAULT_REGION_COUNT);
    // We are using only 1 region server because we cannot wait for all region servers to catch up
    // with replication. Therefore, we cannot be sure about how many rows will be restored after an
    // incremental backup.
    regionServerCount = 1;
    rowsInIteration = conf.getInt(ROWS_PER_ITERATION_KEY, DEFAULT_ROWS_IN_ITERATION);
    numIterations = conf.getInt(NUM_ITERATIONS_KEY, DEFAULT_NUM_ITERATIONS);
    numTables = conf.getInt(NUMBER_OF_TABLES_KEY, DEFAULT_NUMBER_OF_TABLES);
    sleepTime = conf.getLong(SLEEP_TIME_KEY, SLEEP_TIME_DEFAULT);
    BackupTestUtil.enableBackup(conf);
    conf.set(CONF_BACKUP_MAX_WAL_SIZE, "10240");
    conf.set(CONF_STAGED_WAL_FLUSH_INITIAL_DELAY, "10");
    conf.set(CONF_STAGED_WAL_FLUSH_INTERVAL, "10");
    conf.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);
    conf.setBoolean(IGNORE_EMPTY_FILES, true);
    conf.setBoolean(IGNORE_MISSING_FILES, true);

    LOG.info("Initializing cluster with {} region server(s)", regionServerCount);
    util.initializeCluster(regionServerCount);
    LOG.info("Cluster initialized and ready");

    backupRootDir = util.getDataTestDirOnTestFS() + Path.SEPARATOR + backupRootDir;
    LOG.info("The backup root directory is: {}", backupRootDir);
    createAndSetBackupWalDir();
    fs = FileSystem.get(conf);
  }

  @Test
  public void testContinuousBackupRestore() throws Exception {
    LOG.info("Running backup and restore integration test with continuous backup enabled");
    createTables(CLASS_NAME);
    runTestMulti(true);
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
    testContinuousBackupRestore();
    return 0;
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);

    LOG.info(
      MoreObjects.toStringHelper("Parsed Options").add(REGION_COUNT_KEY, regionsCountPerServer)
        .add(ROWS_PER_ITERATION_KEY, rowsInIteration).add(NUM_ITERATIONS_KEY, numIterations)
        .add(NUMBER_OF_TABLES_KEY, numTables).add(SLEEP_TIME_KEY, sleepTime).toString());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestContinuousBackupRestore(), args);
    System.exit(status);
  }
}
