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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
@Tag(LargeTests.TAG)
public class TestBackupOfflineRS extends TestBackupBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupOfflineRS.class);

  @BeforeAll
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.setInt("hbase.regionserver.info.port", -1);
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    setUpHelper();
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    TEST_UTIL.waitTableAvailable(table1);
  }

  /**
   * Tests that when a full backup is taken while an RS is offline (with WALs in oldlogs), the
   * offline host's timestamps are recorded so subsequent incremental backups don't reinclude those
   * WALs.
   */
  @Test
  public void testBackupWithOfflineRS() throws Exception {
    LOG.info("Starting testBackupWithOfflineRS");

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
    Thread.sleep(5000);

    LOG.info("Taking full backup (with offline RS WALs in oldlogs)");
    String fullBackupId = fullTableBackup(tables);
    assertTrue(checkSucceeded(fullBackupId), "Full backup should succeed");

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      LOG.info("RS timestamps after full backup: {}", rsTimestamps);

      Long tsAfterFullBackup = rsTimestamps.get(offlineHost);
      assertNotNull(tsAfterFullBackup,
        "Offline host should have timestamp recorded in trslm after full backup");

      LOG.info("Taking incremental backup (should NOT include offline RS WALs)");
      String incrBackupId = incrementalTableBackup(tables);
      assertTrue(checkSucceeded(incrBackupId), "Incremental backup should succeed");

      timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      rsTimestamps = timestamps.get(table1);
      assertFalse(rsTimestamps.containsKey(offlineHost),
        "Offline host should not have a boundary after incremental");
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
    assertTrue(checkSucceeded(fullBackupId), "Full backup should succeed");

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      insertIntoTable(conn, table1, famName, 4, 50);
    }

    HRegionServer rsBeforeStop = cluster.getLiveRegionServerThreads().get(0).getRegionServer();
    rsBeforeStop.getWalRoller().requestRollAll();
    rsBeforeStop.getWalRoller().waitUntilWalRollFinished();
    String offlineHost =
      rsBeforeStop.getServerName().getHostname() + ":" + rsBeforeStop.getServerName().getPort();

    cluster.stopRegionServer(rsBeforeStop.getServerName());
    Thread.sleep(5000);

    String incrBackupId = incrementalTableBackup(tables);
    assertTrue(checkSucceeded(incrBackupId), "Incremental backup should succeed");

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      assertNotNull(rsTimestamps.get(offlineHost),
        "Offline RS should have a timestamp boundary after the incremental backed up its WALs");

      String incrBackupId2 = incrementalTableBackup(tables);
      assertTrue(checkSucceeded(incrBackupId2), "Second incremental backup should succeed");

      timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      rsTimestamps = timestamps.get(table1);
      assertFalse(rsTimestamps.containsKey(offlineHost),
        "Offline RS should not have a boundary after all its WALs have been backed up");
    }
  }

  /**
   * Tests that a brand-new RS that comes online and goes offline before any backup correctly has its
   * WALs covered by the full backup.
   */
  @Test
  public void testTransientRSBeforeFullBackup() throws Exception {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<TableName> tables = Lists.newArrayList(table1);

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
    assertTrue(checkSucceeded(fullBackupId), "Full backup should succeed");

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      assertNotNull(rsTimestamps.get(transientHost),
        "Transient RS that went offline before full backup should have its WAL boundary recorded");

      String incrBackupId = incrementalTableBackup(tables);
      assertTrue(checkSucceeded(incrBackupId), "Incremental backup after transient RS should succeed");

      timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      rsTimestamps = timestamps.get(table1);
      assertFalse(rsTimestamps.containsKey(transientHost),
        "Transient RS should not have a boundary after the full backup covered all its WALs");
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
    assertTrue(checkSucceeded(fullBackupId), "Full backup should succeed");

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
    assertTrue(checkSucceeded(incrBackupId), "Incremental backup should succeed");

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Map<String, Long>> timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      Map<String, Long> rsTimestamps = timestamps.get(table1);
      assertNotNull(rsTimestamps.get(transientHost),
        "Transient RS should have a timestamp boundary after the incremental backed up its WALs");

      String incrBackupId2 = incrementalTableBackup(tables);
      assertTrue(checkSucceeded(incrBackupId2), "Second incremental backup should succeed");

      timestamps = sysTable.readLogTimestampMap(BACKUP_ROOT_DIR);
      rsTimestamps = timestamps.get(table1);
      assertFalse(rsTimestamps.containsKey(transientHost),
        "Transient RS should not have a boundary after all its WALs have been backed up");
    }
  }
}
