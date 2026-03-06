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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Before;
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
    TEST_UTIL = new HBaseTestingUtil();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.setInt("hbase.regionserver.info.port", -1);
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    setUpHelper();
    // Start an additional RS so we have at least 2
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    TEST_UTIL.waitTableAvailable(table1);
  }

  @Before
  public void cleanupBetweenTests() throws Exception {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    while (cluster.getNumLiveRegionServers() < 2) {
      cluster.startRegionServer();
      Thread.sleep(2000);
    }
    TEST_UTIL.waitTableAvailable(table1);
    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      try {
        sysTable.finishBackupExclusiveOperation();
      } catch (Exception ignored) {
      }
      for (BackupInfo info : sysTable.getBackupHistory()) {
        try {
          sysTable.deleteBackupInfo(info.getBackupId());
        } catch (Exception ignored) {
        }
      }
      try {
        sysTable.deleteIncrementalBackupTableSet(BACKUP_ROOT_DIR);
      } catch (Exception ignored) {
      }
    }
  }

  /**
   * Tests that when a full backup is taken while an RS is offline (with WALs in oldlogs), the
   * offline host's timestamps are recorded so subsequent incremental backups don't re-include those
   * WALs.
   */
  @Test
  public void testBackupWithOfflineRS() throws Exception {
    LOG.info("Starting testFullBackupWithOfflineRS");

    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    if (cluster.getNumLiveRegionServers() < 2) {
      cluster.startRegionServer();
      Thread.sleep(2000);
    }

    LOG.info("Inserting data to generate WAL entries");
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 2, 100);
    }

    int rsToStop = 0;
    HRegionServer rsBeforeStop = cluster.getRegionServer(rsToStop);
    String offlineHost =
      rsBeforeStop.getServerName().getHostname() + ":" + rsBeforeStop.getServerName().getPort();
    LOG.info("Stopping RS: {}", offlineHost);

    cluster.stopRegionServer(rsToStop);
    // Wait for WALs to be moved to oldlogs
    Thread.sleep(5000);

    LOG.info("Taking full backup (with offline RS WALs in oldlogs)");
    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      LOG.info("RS timestamps after full backup: {}", rsTimestamps);

      Long tsAfterFullBackup = rsTimestamps.get(offlineHost);
      assertNotNull("Offline host should have timestamp recorded in trslm after full backup",
        tsAfterFullBackup);

      LOG.info("Taking incremental backup (should NOT include offline RS WALs)");
      String incrBackupId = incrementalTableBackup(tables);
      assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

      timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      rsTimestamps = timestamps.get(table1);
      assertFalse("Offline host should not have a boundary ",
        rsTimestamps.containsKey(offlineHost));
    }
  }

  /**
   * Tests that WALs written to an RS after a full backup are correctly included in the subsequent
   * incremental backup, even if that RS has gone offline before the incremental runs.
   */
  @Test
  public void testRSGoesOfflineAfterFullBackupBeforeIncremental() throws Exception {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    if (cluster.getNumLiveRegionServers() < 2) {
      cluster.startRegionServer();
      Thread.sleep(2000);
    }

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 3, 50);
    }

    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    // Insert more data after the full backup to generate WALs that belong in the incremental
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 4, 50);
    }

    // Pick a live RS, force a roll to ensure WAL files exist after the full backup's boundary,
    // then stop it before the incremental runs
    HRegionServer rsBeforeStop = cluster.getLiveRegionServerThreads().get(0).getRegionServer();
    rsBeforeStop.getWalRoller().requestRollAll();
    rsBeforeStop.getWalRoller().waitUntilWalRollFinished();
    String offlineHost =
      rsBeforeStop.getServerName().getHostname() + ":" + rsBeforeStop.getServerName().getPort();

    cluster.stopRegionServer(rsBeforeStop.getServerName());
    Thread.sleep(5000);

    String incrBackupId = incrementalTableBackup(tables);
    assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      assertNotNull(
        "Offline RS should have a timestamp boundary after the incremental that backed up its WALs",
        rsTimestamps.get(offlineHost));

      String incrBackupId2 = incrementalTableBackup(tables);
      assertTrue("Second incremental backup should succeed", checkSucceeded(incrBackupId2));

      timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      rsTimestamps = timestamps.get(table1);
      assertFalse("Offline RS should not have a boundary after all its WALs have been backed up",
        rsTimestamps.containsKey(offlineHost));
    }
  }

  /**
   * Tests that a brand-new RS that comes online and goes offline before any backup correctly has
   * its WALs covered by the full backup. This prevents subsequent incremental backups from
   * re-including those WALs.
   */
  @Test
  public void testTransientRSBeforeFullBackup() throws Exception {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    // Start a new RS that will be online briefly and then go offline before the full backup
    HRegionServer transientRS = cluster.startRegionServerAndWait(10000).getRegionServer();
    try (Admin admin = TEST_UTIL.getConnection().getAdmin()) {
      List<RegionInfo> regions = admin.getRegions(table1);
      if (!regions.isEmpty()) {
        admin.move(regions.get(0).getEncodedNameAsBytes(), transientRS.getServerName());
        TEST_UTIL.waitUntilAllRegionsAssigned(table1);
      }
    }
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 5, 50);
    }
    transientRS.getWalRoller().requestRollAll();
    transientRS.getWalRoller().waitUntilWalRollFinished();
    String transientHost =
      transientRS.getServerName().getHostname() + ":" + transientRS.getServerName().getPort();

    cluster.stopRegionServer(transientRS.getServerName());
    Thread.sleep(5000);

    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      assertNotNull(
        "Transient RS that went offline before full backup should have its WAL boundary recorded",
        rsTimestamps.get(transientHost));

      String incrBackupId = incrementalTableBackup(tables);
      assertTrue("Incremental backup after transient RS went offline should succeed",
        checkSucceeded(incrBackupId));

      timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      rsTimestamps = timestamps.get(table1);
      assertFalse(
        "Transient RS should not have a boundary after the full backup covered all its WALs",
        rsTimestamps.containsKey(transientHost));
    }
  }

  /**
   * Tests that WALs from an RS that comes online and goes offline between a full backup and an
   * incremental backup are correctly included in the incremental backup and not re-included in
   * subsequent incremental backups.
   */
  @Test
  public void testTransientRSAfterFullBackupBeforeIncremental() throws Exception {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 6, 50);
    }

    String fullBackupId = fullTableBackup(tables);
    assertTrue("Full backup should succeed", checkSucceeded(fullBackupId));

    // Start a new RS after the full backup, move a region to it, let it accumulate WALs, then stop
    HRegionServer transientRS = cluster.startRegionServerAndWait(10000).getRegionServer();
    try (Admin admin = TEST_UTIL.getConnection().getAdmin()) {
      List<RegionInfo> regions = admin.getRegions(table1);
      if (!regions.isEmpty()) {
        admin.move(regions.get(0).getEncodedNameAsBytes(), transientRS.getServerName());
        TEST_UTIL.waitUntilAllRegionsAssigned(table1);
      }
    }
    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 7, 50);
    }
    transientRS.getWalRoller().requestRollAll();
    transientRS.getWalRoller().waitUntilWalRollFinished();
    String transientHost =
      transientRS.getServerName().getHostname() + ":" + transientRS.getServerName().getPort();

    cluster.stopRegionServer(transientRS.getServerName());
    Thread.sleep(5000);

    String incrBackupId = incrementalTableBackup(tables);
    assertTrue("Incremental backup should succeed", checkSucceeded(incrBackupId));

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      assertNotNull(
        "Transient RS should have a timestamp boundary after the incremental backed up its WALs",
        rsTimestamps.get(transientHost));

      String incrBackupId2 = incrementalTableBackup(tables);
      assertTrue("Second incremental backup should succeed", checkSucceeded(incrBackupId2));

      timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      rsTimestamps = timestamps.get(table1);
      assertFalse("Transient RS should not have a boundary after all its WALs have been backed up",
        rsTimestamps.containsKey(transientHost));
    }
  }
}
