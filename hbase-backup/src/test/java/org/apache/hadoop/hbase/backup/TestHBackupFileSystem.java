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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
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
      verify(mockAppender, never()).append(
        argThat((org.apache.logging.log4j.core.LogEvent event) ->
          event.getLevel().equals(org.apache.logging.log4j.Level.ERROR)
            && event.getMessage().getFormattedMessage().contains("Cannot load backup manifest from: ")
            && event.getMessage().getFormattedMessage().contains(".tmp")));

      assertEquals(2, images.size());
      assertEquals(backupId2, images.get(0).getBackupId());
      assertEquals(backupId1, images.get(1).getBackupId());
    } finally {
      log4jLogger.removeAppender(mockAppender);
    }
  }
}
