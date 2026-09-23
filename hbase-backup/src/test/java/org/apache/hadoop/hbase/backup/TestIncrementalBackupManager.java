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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.IncrementalBackupManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(LargeTests.TAG)
public class TestIncrementalBackupManager extends TestBackupBase {

  @BeforeAll
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.setBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR, true);
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    setUpHelper();
  }

  @Test
  public void testCollectWALFilesFromRegionServerDirectories() throws Exception {
    testCollectWALFiles(true);
  }

  @Test
  public void testCollectWALFilesFromFlatOldWALDirectory() throws Exception {
    testCollectWALFiles(false);
  }

  private void testCollectWALFiles(boolean separateOldLogDir) throws Exception {
    Configuration testConf = new Configuration(conf1);
    testConf.setBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR, separateOldLogDir);
    List<TableName> tables = List.of(table1);
    try (Connection conn = ConnectionFactory.createConnection(testConf);
      BackupAdminImpl backupAdmin = new BackupAdminImpl(conn)) {
      String fullBackupId =
        backupAdmin.backupTables(createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR));
      assertTrue(checkSucceeded(fullBackupId));

      try (IncrementalBackupManager manager = new IncrementalBackupManager(conn, testConf)) {
        BackupInfo backupInfo = manager.createBackupInfo("backup_test", BackupType.INCREMENTAL,
          tables, BACKUP_ROOT_DIR, -1, -1, false, false);
        Map<String, Long> previousTimestamps =
          BackupUtils.getRSLogTimestampMins(manager.readLogTimestampMap());
        ServerName serverName = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
        Long previousTimestamp = previousTimestamps.get(serverName.getAddress().toString());
        assertNotNull(previousTimestamp);

        TEST_UTIL.waitFor(30_000,
          () -> EnvironmentEdgeManager.currentTime() > previousTimestamp + 1);
        Path walRootDir = CommonFSUtils.getWALRootDir(conf1);
        Path archiveDir = new Path(walRootDir,
          AbstractFSWALProvider.getWALArchiveDirectoryName(testConf, serverName.toString()));
        String walName = (separateOldLogDir ? "wal" : serverName.toString())
          + BackupUtils.LOGNAME_SEPARATOR + (previousTimestamp + 1);
        Path archivedWAL = new Path(archiveDir, walName);
        FileSystem fs = walRootDir.getFileSystem(conf1);
        fs.mkdirs(archiveDir);
        fs.create(archivedWAL).close();

        manager.getIncrBackupLogFileMap();

        assertTrue(backupInfo.getIncrBackupFileList().contains(archivedWAL.toString()));
      }
    }
  }

  /**
   * WALs can be archived out of order, so a region server that took part in the log roll can have
   * an archived WAL newer than its roll result while an older WAL is still in the WALs directory.
   * The newer archived WAL must be deferred to a later backup instead of being backed up twice, and
   * the older WAL must still end up in a backup.
   */
  @Test
  public void testOutOfOrderArchivedWALDoesNotSkipOlderWAL() throws Exception {
    List<TableName> tables = List.of(table1);
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    ServerName serverName = rs.getServerName();
    Path walRootDir = CommonFSUtils.getWALRootDir(conf1);
    FileSystem fs = walRootDir.getFileSystem(conf1);

    try (Connection conn = ConnectionFactory.createConnection(conf1);
      BackupAdminImpl backupAdmin = new BackupAdminImpl(conn)) {
      String fullBackupId =
        backupAdmin.backupTables(createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR));
      assertTrue(checkSucceeded(fullBackupId));

      long olderWALTs = EnvironmentEdgeManager.currentTime() + 1;
      long newerWALTs = olderWALTs + 1;
      Path walDir =
        new Path(walRootDir, AbstractFSWALProvider.getWALDirectoryName(serverName.toString()));
      Path olderWAL =
        new Path(walDir, serverName.toString() + BackupUtils.LOGNAME_SEPARATOR + olderWALTs);
      Path archiveDir = new Path(walRootDir,
        AbstractFSWALProvider.getWALArchiveDirectoryName(conf1, serverName.toString()));
      Path newerArchivedWAL =
        new Path(archiveDir, "wal" + BackupUtils.LOGNAME_SEPARATOR + newerWALTs);
      fs.create(olderWAL).close();
      fs.mkdirs(archiveDir);
      fs.create(newerArchivedWAL).close();

      try {
        List<String> firstBackupFiles;
        try (IncrementalBackupManager manager = new IncrementalBackupManager(conn, conf1)) {
          BackupInfo backupInfo = manager.createBackupInfo("backup_incr_1", BackupType.INCREMENTAL,
            tables, BACKUP_ROOT_DIR, -1, -1, false, false);
          Map<String, Long> boundaries = manager.getIncrBackupLogFileMap();
          manager.writeRegionServerLogTimestamp(backupInfo.getTables(), boundaries);
          firstBackupFiles = backupInfo.getIncrBackupFileList();
        }
        assertFalse(firstBackupFiles.contains(newerArchivedWAL.toString()),
          "Archived WAL newer than the roll result should be deferred to a later backup: "
            + firstBackupFiles);

        TEST_UTIL.waitFor(30_000, () -> EnvironmentEdgeManager.currentTime() > newerWALTs);
        rs.getWalRoller().requestRollAll();
        rs.getWalRoller().waitUntilWalRollFinished();

        List<String> secondBackupFiles;
        try (IncrementalBackupManager manager = new IncrementalBackupManager(conn, conf1)) {
          BackupInfo backupInfo = manager.createBackupInfo("backup_incr_2", BackupType.INCREMENTAL,
            tables, BACKUP_ROOT_DIR, -1, -1, false, false);
          manager.getIncrBackupLogFileMap();
          secondBackupFiles = backupInfo.getIncrBackupFileList();
        }

        assertTrue(
          firstBackupFiles.contains(olderWAL.toString())
            || secondBackupFiles.contains(olderWAL.toString()),
          "WAL " + olderWAL + " was not included in any backup. First backup: " + firstBackupFiles
            + ", second backup: " + secondBackupFiles);
      } finally {
        fs.delete(olderWAL, false);
        fs.delete(newerArchivedWAL, false);
      }
    }
  }

  /**
   * A dead region server's WALs stay in its -splitting directory until WAL splitting finishes. An
   * incremental backup that runs during the split must not lose them once they are archived.
   */
  @Test
  public void testWALOfDeadServerStillSplittingIsBackedUpAfterArchiving() throws Exception {
    List<TableName> tables = List.of(table1);
    ServerName deadServer = ServerName.valueOf("deadhost", 16020, 1001L);
    Path walRootDir = CommonFSUtils.getWALRootDir(conf1);
    FileSystem fs = walRootDir.getFileSystem(conf1);
    Path splittingDir = splittingDir(walRootDir, deadServer);
    Path archiveDir = new Path(walRootDir,
      AbstractFSWALProvider.getWALArchiveDirectoryName(conf1, deadServer.toString()));

    try (Connection conn = ConnectionFactory.createConnection(conf1);
      BackupAdminImpl backupAdmin = new BackupAdminImpl(conn)) {
      String fullBackupId =
        backupAdmin.backupTables(createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR));
      assertTrue(checkSucceeded(fullBackupId));

      long walTs = EnvironmentEdgeManager.currentTime() + 1;
      Path splittingWAL = new Path(splittingDir, walName(deadServer, walTs));
      Path archivedWAL = archivedWAL(archiveDir, walTs);
      fs.create(splittingWAL).close();

      try {
        // Make the live servers' roll results, and so the next boundaries, newer than the WAL.
        TEST_UTIL.waitFor(30_000, () -> EnvironmentEdgeManager.currentTime() > walTs);
        rollAllLiveRegionServers();

        List<String> firstBackupFiles = runIncrementalBackup(conn, tables, "backup_split_1");

        fs.mkdirs(archiveDir);
        assertTrue(fs.rename(splittingWAL, archivedWAL));
        fs.delete(splittingDir, true);

        List<String> secondBackupFiles = runIncrementalBackup(conn, tables, "backup_split_2");

        assertTrue(
          firstBackupFiles.contains(splittingWAL.toString())
            || secondBackupFiles.contains(archivedWAL.toString()),
          "WAL of the dead server was not included in any backup. First backup: " + firstBackupFiles
            + ", second backup: " + secondBackupFiles);
      } finally {
        fs.delete(splittingDir, true);
        fs.delete(archivedWAL, false);
      }
    }
  }

  /**
   * WAL splitting archives a dead region server's WALs in parallel, so a newer WAL can be archived
   * while older ones are still in the -splitting directory. Backing up the newer WAL must not move
   * the boundary past the older ones.
   */
  @Test
  public void testOlderWALsOfDeadServerAreNotSkippedWhenNewerWALIsArchivedFirst() throws Exception {
    List<TableName> tables = List.of(table1);
    ServerName deadServer = ServerName.valueOf("deadhost", 16020, 2002L);
    Path walRootDir = CommonFSUtils.getWALRootDir(conf1);
    FileSystem fs = walRootDir.getFileSystem(conf1);
    Path splittingDir = splittingDir(walRootDir, deadServer);
    Path archiveDir = new Path(walRootDir,
      AbstractFSWALProvider.getWALArchiveDirectoryName(conf1, deadServer.toString()));

    try (Connection conn = ConnectionFactory.createConnection(conf1);
      BackupAdminImpl backupAdmin = new BackupAdminImpl(conn)) {
      String fullBackupId =
        backupAdmin.backupTables(createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR));
      assertTrue(checkSucceeded(fullBackupId));

      long oldestTs = EnvironmentEdgeManager.currentTime() + 1;
      long middleTs = oldestTs + 1;
      long newestTs = oldestTs + 2;
      Path oldestSplittingWAL = new Path(splittingDir, walName(deadServer, oldestTs));
      Path middleSplittingWAL = new Path(splittingDir, walName(deadServer, middleTs));
      Path oldestArchivedWAL = archivedWAL(archiveDir, oldestTs);
      Path middleArchivedWAL = archivedWAL(archiveDir, middleTs);
      Path newestArchivedWAL = archivedWAL(archiveDir, newestTs);
      fs.create(oldestSplittingWAL).close();
      fs.create(middleSplittingWAL).close();
      fs.mkdirs(archiveDir);
      fs.create(newestArchivedWAL).close();

      try {
        TEST_UTIL.waitFor(30_000, () -> EnvironmentEdgeManager.currentTime() > newestTs);

        List<String> firstBackupFiles = runIncrementalBackup(conn, tables, "backup_order_1");

        assertTrue(fs.rename(oldestSplittingWAL, oldestArchivedWAL));
        assertTrue(fs.rename(middleSplittingWAL, middleArchivedWAL));
        fs.delete(splittingDir, true);

        List<String> secondBackupFiles = runIncrementalBackup(conn, tables, "backup_order_2");

        assertAll(
          () -> assertTrue(
            firstBackupFiles.contains(oldestSplittingWAL.toString())
              || secondBackupFiles.contains(oldestArchivedWAL.toString()),
            "Oldest WAL of the dead server was not included in any backup. First backup: "
              + firstBackupFiles + ", second backup: " + secondBackupFiles),
          () -> assertTrue(
            firstBackupFiles.contains(middleSplittingWAL.toString())
              || secondBackupFiles.contains(middleArchivedWAL.toString()),
            "Middle WAL of the dead server was not included in any backup. First backup: "
              + firstBackupFiles + ", second backup: " + secondBackupFiles));
      } finally {
        fs.delete(splittingDir, true);
        fs.delete(oldestArchivedWAL, false);
        fs.delete(middleArchivedWAL, false);
        fs.delete(newestArchivedWAL, false);
      }
    }
  }

  /**
   * A dead region server can keep an old WAL, already covered by its boundary, in its -splitting
   * directory across several incremental backups. Its boundary must survive those backups, or a
   * later backup would include that WAL again and could bring back deleted data.
   */
  @Test
  public void testDeadServerKeepsBoundaryWhileOldWALIsStillSplitting() throws Exception {
    List<TableName> tables = List.of(table1);
    ServerName deadServer = ServerName.valueOf("deadhost", 16020, 3003L);
    Path walRootDir = CommonFSUtils.getWALRootDir(conf1);
    FileSystem fs = walRootDir.getFileSystem(conf1);
    Path splittingDir = splittingDir(walRootDir, deadServer);
    Path archiveDir = new Path(walRootDir,
      AbstractFSWALProvider.getWALArchiveDirectoryName(conf1, deadServer.toString()));

    try (Connection conn = ConnectionFactory.createConnection(conf1);
      BackupAdminImpl backupAdmin = new BackupAdminImpl(conn)) {
      String fullBackupId =
        backupAdmin.backupTables(createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR));
      assertTrue(checkSucceeded(fullBackupId));

      long stuckTs = EnvironmentEdgeManager.currentTime() + 1;
      long archivedTs = stuckTs + 1;
      Path archivedWAL = archivedWAL(archiveDir, archivedTs);
      Path stuckSplittingWAL = new Path(splittingDir, walName(deadServer, stuckTs));
      Path stuckArchivedWAL = archivedWAL(archiveDir, stuckTs);
      fs.mkdirs(archiveDir);
      fs.create(archivedWAL).close();

      try {
        TEST_UTIL.waitFor(30_000, () -> EnvironmentEdgeManager.currentTime() > archivedTs);
        List<String> firstBackupFiles = runIncrementalBackup(conn, tables, "backup_stuck_1");
        assertTrue(firstBackupFiles.contains(archivedWAL.toString()),
          "Archived WAL of the dead server should be in the first backup: " + firstBackupFiles);

        fs.create(stuckSplittingWAL).close();
        List<String> laterBackupFiles = new ArrayList<>();
        laterBackupFiles.addAll(runIncrementalBackup(conn, tables, "backup_stuck_2"));
        laterBackupFiles.addAll(runIncrementalBackup(conn, tables, "backup_stuck_3"));

        assertTrue(fs.rename(stuckSplittingWAL, stuckArchivedWAL));
        fs.delete(splittingDir, true);
        laterBackupFiles.addAll(runIncrementalBackup(conn, tables, "backup_stuck_4"));

        assertFalse(
          laterBackupFiles.contains(stuckSplittingWAL.toString())
            || laterBackupFiles.contains(stuckArchivedWAL.toString()),
          "WAL already covered by the dead server's boundary was backed up again: "
            + laterBackupFiles);
      } finally {
        fs.delete(splittingDir, true);
        fs.delete(archivedWAL, false);
        fs.delete(stuckArchivedWAL, false);
      }
    }
  }

  private static Path splittingDir(Path walRootDir, ServerName serverName) {
    return new Path(walRootDir, AbstractFSWALProvider.getWALDirectoryName(serverName.toString())
      + AbstractFSWALProvider.SPLITTING_EXT);
  }

  private static Path archivedWAL(Path archiveDir, long ts) {
    return new Path(archiveDir, "wal" + BackupUtils.LOGNAME_SEPARATOR + ts);
  }

  private static String walName(ServerName serverName, long ts) {
    return serverName.toString() + BackupUtils.LOGNAME_SEPARATOR + ts;
  }

  private static void rollAllLiveRegionServers() throws Exception {
    for (JVMClusterUtil.RegionServerThread rst : TEST_UTIL.getMiniHBaseCluster()
      .getLiveRegionServerThreads()) {
      rst.getRegionServer().getWalRoller().requestRollAll();
      rst.getRegionServer().getWalRoller().waitUntilWalRollFinished();
    }
  }

  private static List<String> runIncrementalBackup(Connection conn, List<TableName> tables,
    String backupId) throws Exception {
    try (IncrementalBackupManager manager = new IncrementalBackupManager(conn, conf1)) {
      BackupInfo backupInfo = manager.createBackupInfo(backupId, BackupType.INCREMENTAL, tables,
        BACKUP_ROOT_DIR, -1, -1, false, false);
      Map<String, Long> boundaries = manager.getIncrBackupLogFileMap();
      manager.writeRegionServerLogTimestamp(backupInfo.getTables(), boundaries);
      return backupInfo.getIncrBackupFileList();
    }
  }
}
