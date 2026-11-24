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

import static org.apache.hadoop.hbase.backup.TestBackupUtils.assertBackupCompleteFileDoesNotExist;
import static org.apache.hadoop.hbase.backup.TestBackupUtils.assertBackupCompleteFileExists;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(SmallTests.TAG)
public class TestHBackupFileSystem {

  private static final Path ROOT_DIR = new Path("/backup/root");
  private static final String BACKUP_ID = "123";

  @Test
  public void testRootDirManifestPathConversion() throws IOException {
    Path manifestPath =
      HBackupFileSystem.getManifestPath(new Configuration(), ROOT_DIR, BACKUP_ID, false);
    Path convertedRootDir = HBackupFileSystem.getRootDirFromBackupPath(manifestPath, BACKUP_ID);
    assertEquals(ROOT_DIR, convertedRootDir);
  }

  // This test verifies the .tmp directory is skipped over when each subdirectory in the backup
  // root dir is searched for its .backup.manifest file.
  @Test
  public void testGetAllBackupImagesSkipsTmpDir(@TempDir java.nio.file.Path tempDir)
    throws IOException {
    Configuration conf = new Configuration();
    Path backupRootPath = new Path(tempDir.toUri());
    FileSystem fs = FileSystem.get(backupRootPath.toUri(), conf);

    TableName[] tables = new TableName[] { TableName.valueOf("test_table") };
    String backupId1 = "backup_0001";
    String backupId2 = "backup_0002";

    BackupInfo info1 =
      new BackupInfo(backupId1, BackupType.FULL, tables, backupRootPath.toString());
    new BackupManifest(info1).store(conf);

    BackupInfo info2 =
      new BackupInfo(backupId2, BackupType.INCREMENTAL, tables, backupRootPath.toString());
    new BackupManifest(info2).store(conf);

    fs.mkdirs(new Path(backupRootPath, ".tmp"));

    // Use fully-qualified Log4j2 names to avoid banned-import enforcer rule
    org.apache.logging.log4j.core.Appender mockAppender =
      mock(org.apache.logging.log4j.core.Appender.class);
    when(mockAppender.getName()).thenReturn("mockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
    org.apache.logging.log4j.core.Logger log4jLogger =
      (org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
        .getLogger(HBackupFileSystem.class);
    log4jLogger.addAppender(mockAppender);

    try {
      List<BackupImage> images = HBackupFileSystem.getAllBackupImages(conf, backupRootPath);

      // The backupRoot/.tmp dir should not be checked for a .backup.manifest file because it will
      // not have one. Verify an ERROR is not logged with the following message:
      // Cannot load backup manifest from: /path/to/.tmp
      verify(mockAppender, never())
        .append(argThat((org.apache.logging.log4j.core.LogEvent event) -> event.getLevel()
          .equals(org.apache.logging.log4j.Level.ERROR)
          && event.getMessage().getFormattedMessage().contains("Cannot load backup manifest from: ")
          && event.getMessage().getFormattedMessage().contains(".tmp")));

      assertEquals(2, images.size());
      assertEquals(backupId2, images.get(0).getBackupId());
      assertEquals(backupId1, images.get(1).getBackupId());
    } finally {
      log4jLogger.removeAppender(mockAppender);
    }
  }

  // Verifies a failure to create a .backup.complete file in a completed backup directory does
  // not cause the backup to fail
  @Test
  public void testWriteBackupCompleteMarkerFailsGracefully(@TempDir java.nio.file.Path tempDir)
    throws IOException {
    Configuration conf = new Configuration();
    // Tell hadoop to use MarkerCreationFailFileSystem instead of the default LocalFileSystem
    // whenever a file:// URI is resolved
    conf.setClass("fs.file.impl", MarkerCreationFailFileSystem.class, FileSystem.class);
    // Disable cache to force Hadoop to instantiate a fresh filesystem. If LocalFileSystem is
    // cached, then Hadoop will use that instead of MarkerCreationFailFileSystem
    conf.setBoolean("fs.file.impl.disable.cache", true);

    String backupRootDir = tempDir.toUri().toString();
    String backupId = "backup_fail_marker";

    // Create the backup directory so the path is valid
    Path backupDir = HBackupFileSystem.getBackupPath(backupRootDir, backupId);
    FileSystem realFs = FileSystem.getLocal(new Configuration());
    realFs.mkdirs(backupDir);

    // This should NOT throw -- marker creation is best-effort
    HBackupFileSystem.writeBackupCompleteMarker(conf, backupRootDir, backupId);

    // Marker should not exist since create() was intercepted
    assertBackupCompleteFileDoesNotExist(new Configuration(), backupRootDir, backupId);
  }

  @Test
  public void testDeleteBackupCompleteMarker(@TempDir java.nio.file.Path tempDir)
    throws IOException {
    Configuration conf = new Configuration();
    String backupRootDir = tempDir.toUri().toString();
    String backupId = "backup_delete_marker";

    HBackupFileSystem.writeBackupCompleteMarker(conf, backupRootDir, backupId);
    assertBackupCompleteFileExists(conf, backupRootDir, backupId);

    HBackupFileSystem.deleteBackupCompleteMarker(conf, backupRootDir, backupId);
    assertBackupCompleteFileDoesNotExist(conf, backupRootDir, backupId);

    // Deleting a marker that does not exist is a no-op
    HBackupFileSystem.deleteBackupCompleteMarker(conf, backupRootDir, backupId);
  }

  // Verifies a failure to delete the marker is surfaced, so callers do not go on to delete backup
  // data while leaving a marker behind
  @Test
  public void testDeleteBackupCompleteMarkerFailureThrows(@TempDir java.nio.file.Path tempDir)
    throws IOException {
    String backupRootDir = tempDir.toUri().toString();
    String backupId = "backup_delete_marker_fail";
    HBackupFileSystem.writeBackupCompleteMarker(new Configuration(), backupRootDir, backupId);

    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", MarkerDeletionFailFileSystem.class, FileSystem.class);
    conf.setBoolean("fs.file.impl.disable.cache", true);

    assertThrows(IOException.class,
      () -> HBackupFileSystem.deleteBackupCompleteMarker(conf, backupRootDir, backupId));
    assertBackupCompleteFileExists(new Configuration(), backupRootDir, backupId);
  }

  // Verifies backup delete cleanup aborts without deleting any data if the marker cannot be
  // removed, and propagates the failure so the caller does not delete the backup's metadata
  @Test
  public void testBackupDataCleanupAbortsWhenMarkerDeleteFails(@TempDir java.nio.file.Path tempDir)
    throws IOException {
    String backupRootDir = tempDir.toUri().toString();
    BackupInfo info = createBackupWithMarkerAndData(backupRootDir, "backup_cleanup_marker_fail");

    assertThrows(IOException.class,
      () -> BackupUtils.cleanupBackupData(info, createMarkerDeletionFailConf()));
    assertMarkerAndDataRemain(backupRootDir, info);
  }

  // Verifies failed-backup recovery aborts before any destructive step if the marker cannot be
  // removed: no snapshots are deleted, the backup system table is not restored (which would remove
  // the RUNNING backup record), and no backup data is deleted. This leaves the failed backup
  // discoverable so repair can retry.
  @Test
  public void testFailedBackupRecoveryAbortsBeforeDestructiveStepsWhenMarkerDeleteFails(
    @TempDir java.nio.file.Path tempDir) throws IOException {
    String backupRootDir = tempDir.toUri().toString();
    BackupInfo info = createBackupWithMarkerAndData(backupRootDir, "backup_failed_marker_fail");
    info.setPhase(BackupPhase.SNAPSHOTCOPY);
    Connection conn = mock(Connection.class);

    assertThrows(IOException.class, () -> TableBackupClient.cleanupAndRestoreBackupSystem(conn,
      info, createMarkerDeletionFailConf()));
    verifyNoInteractions(conn);
    assertMarkerAndDataRemain(backupRootDir, info);
  }

  private static BackupInfo createBackupWithMarkerAndData(String backupRootDir, String backupId)
    throws IOException {
    TableName table = TableName.valueOf("test_table");
    BackupInfo info =
      new BackupInfo(backupId, BackupType.FULL, new TableName[] { table }, backupRootDir);
    Path tableBackupDir =
      new Path(HBackupFileSystem.getTableBackupDir(backupRootDir, backupId, table));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.mkdirs(tableBackupDir);
    fs.create(new Path(tableBackupDir, "data")).close();
    HBackupFileSystem.writeBackupCompleteMarker(new Configuration(), backupRootDir, backupId);
    assertBackupCompleteFileExists(new Configuration(), backupRootDir, backupId);
    return info;
  }

  private static void assertMarkerAndDataRemain(String backupRootDir, BackupInfo info)
    throws IOException {
    Configuration conf = new Configuration();
    assertBackupCompleteFileExists(conf, backupRootDir, info.getBackupId());
    for (TableName table : info.getTableNames()) {
      Path tableBackupDir =
        new Path(HBackupFileSystem.getTableBackupDir(backupRootDir, info.getBackupId(), table));
      assertTrue(FileSystem.getLocal(conf).exists(new Path(tableBackupDir, "data")));
    }
  }

  private static Configuration createMarkerDeletionFailConf() {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", MarkerDeletionFailFileSystem.class, FileSystem.class);
    conf.setBoolean("fs.file.impl.disable.cache", true);
    return conf;
  }

  public static class MarkerDeletionFailFileSystem extends FilterFileSystem {
    public MarkerDeletionFailFileSystem() {
      super(new LocalFileSystem());
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      if (f.getName().equals(HBackupFileSystem.BACKUP_COMPLETE_FILE_NAME)) {
        return false;
      }
      return super.delete(f, recursive);
    }
  }

  public static class MarkerCreationFailFileSystem extends FilterFileSystem {
    public MarkerCreationFailFileSystem() {
      super(new LocalFileSystem());
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
      if (f.getName().equals(HBackupFileSystem.BACKUP_COMPLETE_FILE_NAME)) {
        throw new IOException("Injected failure for marker creation test");
      }
      return super.create(f, overwrite);
    }
  }
}
