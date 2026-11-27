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
package org.apache.hadoop.hbase.backup.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.PointInTimeRestoreRequest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

@Tag(SmallTests.TAG)
public class TestCustomBackupLocationPitrRestoreHandler
  extends TestAbstractPitrRestoreHandlerGetBackupMetadata {

  private BackupImage buildImage(String backupId, BackupType type, long startTime,
    long completeTime) {
    return BackupImage.newBuilder().withBackupId(backupId).withType(type)
      .withRootDir(BACKUP_ROOT_DIR).withTableList(TABLE_LIST).withStartTime(startTime)
      .withCompleteTime(completeTime).build();
  }

  private static void stubBackupImages(MockedStatic<HBackupFileSystem> mockedHBFS,
    List<BackupImage> images) {
    mockedHBFS.when(() -> HBackupFileSystem.getAllBackupImages(any(Configuration.class), any()))
      .thenReturn(images);
  }

  private static void stubMarker(MockedStatic<HBackupFileSystem> mockedHBFS, String backupId,
    boolean present) {
    mockedHBFS.when(() -> HBackupFileSystem.isBackupComplete(any(Configuration.class),
      eq(BACKUP_ROOT_DIR), eq(backupId))).thenReturn(present);
  }

  /**
   * Builds one backup image per state. Only backups in the {@link BackupState#COMPLETE} state get a
   * backup completion marker.
   */
  @Override
  protected List<PitrBackupMetadata> callGetBackupMetadataWithBackupStates(BackupState... states)
    throws IOException {
    List<BackupImage> images = new ArrayList<>();
    try (MockedStatic<HBackupFileSystem> mockedHBFS = mockStatic(HBackupFileSystem.class)) {
      for (int i = 0; i < states.length; i++) {
        String backupId = "backup_" + i;
        BackupType backupType = i == 0 ? BackupType.FULL : BackupType.INCREMENTAL;
        images.add(buildImage(backupId, backupType, (i + 1) * 100L, (i + 1) * 100L + 100));
        stubMarker(mockedHBFS, backupId, states[i] == BackupState.COMPLETE);
      }
      stubBackupImages(mockedHBFS, images);

      CustomBackupLocationPitrRestoreHandler handler =
        new CustomBackupLocationPitrRestoreHandler(mockConn, request);
      return handler.getBackupMetadata(request);
    }
  }

  @Test
  public void testGetBackupMetadataFiltersBackupsWithoutMarker() throws IOException {
    BackupImage imageWithMarker = buildImage("backup_1", BackupType.FULL, 100L, 200L);
    BackupImage imageWithoutMarker = buildImage("backup_2", BackupType.INCREMENTAL, 300L, 400L);

    try (MockedStatic<HBackupFileSystem> mockedHBFS = mockStatic(HBackupFileSystem.class)) {
      stubBackupImages(mockedHBFS, List.of(imageWithMarker, imageWithoutMarker));
      stubMarker(mockedHBFS, "backup_1", true);
      stubMarker(mockedHBFS, "backup_2", false);

      CustomBackupLocationPitrRestoreHandler handler =
        new CustomBackupLocationPitrRestoreHandler(mockConn, request);
      List<PitrBackupMetadata> result = handler.getBackupMetadata(request);

      assertEquals(1, result.size());
      assertEquals("backup_1", result.get(0).getBackupId());
    }
  }

  // A failure to check for the completion marker must not be mistaken for an incomplete backup
  @Test
  public void testGetBackupMetadataPropagatesIOException() {
    BackupImage completeImage = buildImage("backup_1", BackupType.FULL, 100L, 200L);
    BackupImage errorImage = buildImage("backup_2", BackupType.INCREMENTAL, 300L, 400L);

    try (MockedStatic<HBackupFileSystem> mockedHBFS = mockStatic(HBackupFileSystem.class)) {
      stubBackupImages(mockedHBFS, List.of(completeImage, errorImage));
      stubMarker(mockedHBFS, "backup_1", true);
      mockedHBFS.when(() -> HBackupFileSystem.isBackupComplete(any(Configuration.class),
        eq(BACKUP_ROOT_DIR), eq("backup_2"))).thenThrow(new IOException("Connection lost"));

      CustomBackupLocationPitrRestoreHandler handler =
        new CustomBackupLocationPitrRestoreHandler(mockConn, request);

      IOException e = assertThrows(IOException.class, () -> handler.getBackupMetadata(request));
      assertEquals("Connection lost", e.getMessage());
    }
  }

  // Backups in a custom location may have been created by another cluster, so the current
  // cluster's BackupSystemTable must not be used to determine whether a backup is complete
  @Test
  public void testGetBackupMetadataDoesNotUseBackupSystemTable() throws IOException {
    BackupImage image = buildImage("backup_1", BackupType.FULL, 100L, 200L);

    try (MockedStatic<HBackupFileSystem> mockedHBFS = mockStatic(HBackupFileSystem.class);
      MockedConstruction<BackupSystemTable> mockedSysTable =
        mockConstruction(BackupSystemTable.class)) {
      stubBackupImages(mockedHBFS, List.of(image));
      stubMarker(mockedHBFS, "backup_1", true);

      CustomBackupLocationPitrRestoreHandler handler =
        new CustomBackupLocationPitrRestoreHandler(mockConn, request);
      List<PitrBackupMetadata> result = handler.getBackupMetadata(request);

      assertEquals(1, result.size());
      assertEquals("backup_1", result.get(0).getBackupId());
      assertTrue(mockedSysTable.constructed().isEmpty(),
        "BackupSystemTable should not be used for a custom backup location");
    }
  }

  // End-to-end check against a real file system: only the backup with a completion marker on disk
  // is returned, with no BackupSystemTable involved
  @Test
  public void testGetBackupMetadataUsesMarkerFileOnDisk(@TempDir java.nio.file.Path tempDir)
    throws IOException {
    Configuration conf = new Configuration();
    String backupRootDir = new Path(tempDir.toUri()).toString();
    TableName[] tables = TABLE_LIST.toArray(new TableName[0]);
    String completeBackupId = "backup_1000";
    String incompleteBackupId = "backup_2000";

    new BackupManifest(new BackupInfo(completeBackupId, BackupType.FULL, tables, backupRootDir))
      .store(conf);
    new BackupManifest(
      new BackupInfo(incompleteBackupId, BackupType.INCREMENTAL, tables, backupRootDir))
      .store(conf);
    HBackupFileSystem.writeBackupCompleteMarker(conf, backupRootDir, completeBackupId);

    when(mockConn.getConfiguration()).thenReturn(conf);
    PointInTimeRestoreRequest diskRequest =
      new PointInTimeRestoreRequest.Builder().withBackupRootDir(backupRootDir).build();

    try (MockedConstruction<BackupSystemTable> mockedSysTable =
      mockConstruction(BackupSystemTable.class)) {
      CustomBackupLocationPitrRestoreHandler handler =
        new CustomBackupLocationPitrRestoreHandler(mockConn, diskRequest);
      List<PitrBackupMetadata> result = handler.getBackupMetadata(diskRequest);

      // Verify backup metadata for just COMPLETE backups are returned, and this can happen even
      // if the cluster's BackupSystemTable is empty
      assertEquals(1, result.size());
      assertEquals(completeBackupId, result.get(0).getBackupId());
      assertTrue(mockedSysTable.constructed().isEmpty());
    }
  }
}
