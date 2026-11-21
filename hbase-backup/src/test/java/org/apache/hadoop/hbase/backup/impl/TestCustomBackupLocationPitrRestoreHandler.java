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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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

  @Override
  protected List<PitrBackupMetadata> callGetBackupMetadataWithBackupStates(BackupState... states)
    throws IOException {
    List<BackupImage> images = new ArrayList<>();
    Map<String, BackupInfo> infoMap = new HashMap<>();
    BackupType backupType;
    for (int i = 0; i < states.length; i++) {
      String backupId = "backup_" + i;

      if (i == 0) {
        backupType = BackupType.FULL;
      } else {
        backupType = BackupType.INCREMENTAL;
      }

      images.add(buildImage(backupId, backupType, (i + 1) * 100L, (i + 1) * 100L + 100));
      infoMap.put(backupId, getBackupInfo(states[i]));
    }

    try (
      MockedStatic<HBackupFileSystem> mockedHBFS = mockStatic(HBackupFileSystem.class);
      MockedConstruction<BackupSystemTable> mockedConstruction =
        mockConstruction(BackupSystemTable.class, (mockSysTable, context) -> {
          for (Map.Entry<String, BackupInfo> entry : infoMap.entrySet()) {
            when(mockSysTable.readBackupInfo(entry.getKey())).thenReturn(entry.getValue());
          }
        })) {
      mockedHBFS
        .when(() -> HBackupFileSystem.getAllBackupImages(any(Configuration.class), any()))
        .thenReturn(images);

      CustomBackupLocationPitrRestoreHandler handler =
        new CustomBackupLocationPitrRestoreHandler(mockConn, request);
      return handler.getBackupMetadata(request);
    }
  }

  @Test
  public void testGetBackupMetadataFiltersBackupsWithNullInfo() throws IOException {
    BackupImage imageWithInfo = buildImage("backup_1", BackupType.FULL, 100L, 200L);
    BackupImage imageWithNullInfo = buildImage("backup_2", BackupType.INCREMENTAL, 300L, 400L);

    BackupInfo completeInfo = getBackupInfo(BackupState.COMPLETE);

    try (
      MockedStatic<HBackupFileSystem> mockedHBFS = mockStatic(HBackupFileSystem.class);
      MockedConstruction<BackupSystemTable> mockedConstruction =
        mockConstruction(BackupSystemTable.class, (mockSysTable, context) -> {
          when(mockSysTable.readBackupInfo("backup_1")).thenReturn(completeInfo);
          when(mockSysTable.readBackupInfo("backup_2")).thenReturn(null);
        })) {
      mockedHBFS
        .when(() -> HBackupFileSystem.getAllBackupImages(any(Configuration.class), any()))
        .thenReturn(List.of(imageWithInfo, imageWithNullInfo));

      CustomBackupLocationPitrRestoreHandler handler =
        new CustomBackupLocationPitrRestoreHandler(mockConn, request);

      List<PitrBackupMetadata> result = handler.getBackupMetadata(request);

      assertEquals(1, result.size());
      assertEquals("backup_1", result.get(0).getBackupId());
    }
  }

  @Test
  public void testGetBackupMetadataFiltersBackupsWhenReadThrowsIOException() throws IOException {
    BackupImage completeImage = buildImage("backup_1", BackupType.FULL, 100L, 200L);
    BackupImage errorImage = buildImage("backup_2", BackupType.INCREMENTAL, 300L, 400L);

    BackupInfo completeInfo = getBackupInfo(BackupState.COMPLETE);

    try (
      MockedStatic<HBackupFileSystem> mockedHBFS = mockStatic(HBackupFileSystem.class);
      MockedConstruction<BackupSystemTable> mockedConstruction =
        mockConstruction(BackupSystemTable.class, (mockSysTable, context) -> {
          when(mockSysTable.readBackupInfo("backup_1")).thenReturn(completeInfo);
          when(mockSysTable.readBackupInfo("backup_2"))
            .thenThrow(new IOException("Connection lost"));
        })) {
      mockedHBFS
        .when(() -> HBackupFileSystem.getAllBackupImages(any(Configuration.class), any()))
        .thenReturn(List.of(completeImage, errorImage));

      CustomBackupLocationPitrRestoreHandler handler =
        new CustomBackupLocationPitrRestoreHandler(mockConn, request);

      List<PitrBackupMetadata> result = handler.getBackupMetadata(request);

      assertEquals(1, result.size());
      assertEquals("backup_1", result.get(0).getBackupId());
    }
  }
}
