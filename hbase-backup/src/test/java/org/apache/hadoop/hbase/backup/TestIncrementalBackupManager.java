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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
}
