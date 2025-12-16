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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Tests that WAL files from offline/inactive RegionServers are handled correctly during backup.
 * Specifically verifies that WALs from an offline RS are:
 * <ol>
 * <li>Backed up once in the first backup after the RS goes offline</li>
 * <li>NOT re-backed up in subsequent backups</li>
 * </ol>
 */
@Category(LargeTests.class)
public class TestBackupOfflineRS extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupOfflineRS.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupOfflineRS.class);

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.setInt("hbase.regionserver.info.port", -1);
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    setUpHelper();
    // Start an additional RS so we have at least 2
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    TEST_UTIL.waitTableAvailable(table1);
  }

  /**
   * Tests that when a RegionServer goes offline, its WAL files are backed up once in the first
   * incremental backup and NOT re-backed up in subsequent incremental backups.
   */
  @Test
  public void testIncrementalBackupWithOfflineRS() throws Exception {
    LOG.info("Starting testIncrementalBackupWithOfflineRS");

    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    // 1. Run full backup to establish baseline
    LOG.info("Taking full backup");
    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    // 2. Insert some data to generate WAL entries
    LOG.info("Inserting data to generate WAL entries");
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 1, 100);
    }

    // 3. Stop one RS to simulate it going offline
    int rsToStop = 0;
    HRegionServer rsBeforeStop = cluster.getRegionServer(rsToStop);
    String offlineHost = rsBeforeStop.getServerName().getHostAndPort();
    String offlineHostPrefix = offlineHost.split(",")[0];
    LOG.info("Stopping RS: {} (prefix: {})", offlineHost, offlineHostPrefix);

    cluster.stopRegionServer(rsToStop);
    // Wait for WALs to be moved to oldlogs
    Thread.sleep(5000);

    // 4. Run first incremental backup - should include offline host's WALs
    LOG.info("Taking first incremental backup (should include offline RS WALs)");
    String incr1 = incrementalTableBackup(tables);
    assertTrue("First incremental backup should succeed", checkSucceeded(incr1));

    // 5. Verify offline host is recorded in trslm
    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      LOG.info("RS timestamps after first incremental: {}", rsTimestamps);

      boolean offlineHostRecorded =
        rsTimestamps.keySet().stream().anyMatch(k -> k.contains(offlineHostPrefix));
      assertTrue("Offline host should have timestamp recorded in trslm", offlineHostRecorded);

      // 6. Get WAL file list for first incremental
      BackupInfo backupInfo1 = sysTable.readBackupInfo(incr1);
      List<String> walFiles1 = backupInfo1.getIncrBackupFileList();
      LOG.info("WAL files in first incremental: {}", walFiles1);

      long offlineHostWalCount1 =
        walFiles1.stream().filter(f -> f.contains(offlineHostPrefix)).count();
      LOG.info("Offline host WAL count in first incremental: {}", offlineHostWalCount1);
      assertTrue("First incremental should include offline host WALs", offlineHostWalCount1 > 0);

      // 7. Run second incremental backup - should NOT include offline host's WALs
      LOG.info("Taking second incremental backup (should NOT include offline RS WALs)");
      String incr2 = incrementalTableBackup(tables);
      assertTrue("Second incremental backup should succeed", checkSucceeded(incr2));

      // 8. Verify second incremental does not include offline host's WALs
      BackupInfo backupInfo2 = sysTable.readBackupInfo(incr2);
      List<String> walFiles2 = backupInfo2.getIncrBackupFileList();
      LOG.info("WAL files in second incremental: {}", walFiles2);

      long offlineHostWalCount2 =
        walFiles2.stream().filter(f -> f.contains(offlineHostPrefix)).count();
      LOG.info("Offline host WAL count in second incremental: {}", offlineHostWalCount2);
      assertEquals("Second incremental should NOT include offline host WALs", 0,
        offlineHostWalCount2);
    }

    LOG.info("testIncrementalBackupWithOfflineRS completed successfully");
  }

  /**
   * Tests that when a full backup is taken while an RS is offline (with WALs in oldlogs), the
   * offline host's timestamps are recorded so subsequent incremental backups don't re-include those
   * WALs.
   */
  @Test
  public void testFullBackupWithOfflineRS() throws Exception {
    LOG.info("Starting testFullBackupWithOfflineRS");

    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    // Ensure we have at least 2 RSes
    if (cluster.getNumLiveRegionServers() < 2) {
      cluster.startRegionServer();
      Thread.sleep(2000);
    }

    // 1. Insert some data to generate WAL entries
    LOG.info("Inserting data to generate WAL entries");
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 2, 100);
    }

    // 2. Stop one RS to simulate it going offline
    int rsToStop = 0;
    HRegionServer rsBeforeStop = cluster.getRegionServer(rsToStop);
    String offlineHost = rsBeforeStop.getServerName().getHostAndPort();
    String offlineHostPrefix = offlineHost.split(",")[0];
    LOG.info("Stopping RS: {} (prefix: {})", offlineHost, offlineHostPrefix);

    cluster.stopRegionServer(rsToStop);
    // Wait for WALs to be moved to oldlogs
    Thread.sleep(5000);

    // 3. Run full backup - should record offline host timestamps
    LOG.info("Taking full backup (with offline RS WALs in oldlogs)");
    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    // 4. Verify offline host is recorded in trslm
    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      LOG.info("RS timestamps after full backup: {}", rsTimestamps);

      boolean offlineHostRecorded =
        rsTimestamps.keySet().stream().anyMatch(k -> k.contains(offlineHostPrefix));
      assertTrue("Offline host should have timestamp recorded in trslm after full backup",
        offlineHostRecorded);

      // 5. Run incremental backup - should NOT include offline host's WALs from before full backup
      LOG.info("Taking incremental backup (should NOT include offline RS WALs)");
      String incrBackupId = incrementalTableBackup(tables);
      assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

      // 6. Verify incremental does not include offline host's WALs
      BackupInfo backupInfo = sysTable.readBackupInfo(incrBackupId);
      List<String> walFiles = backupInfo.getIncrBackupFileList();
      LOG.info("WAL files in incremental: {}", walFiles);

      long offlineHostWalCount =
        walFiles.stream().filter(f -> f.contains(offlineHostPrefix)).count();
      LOG.info("Offline host WAL count in incremental: {}", offlineHostWalCount);
      assertEquals("Incremental after full should NOT include offline host WALs", 0,
        offlineHostWalCount);
    }

    LOG.info("testFullBackupWithOfflineRS completed successfully");
  }
}
