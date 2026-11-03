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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.PointInTimeRestoreRequest;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

abstract class TestAbstractPitrRestoreHandlerGetBackupMetadata {

  protected static final String TABLE_NAME = "table";
  protected static final String BACKUP_ROOT_DIR = "/backup/root";
  protected static final List<TableName> TABLE_LIST = List.of(TableName.valueOf(TABLE_NAME));

  protected Connection mockConn;
  protected PointInTimeRestoreRequest request;

  @BeforeEach
  void setUp() {
    mockConn = mock(Connection.class);
    when(mockConn.getConfiguration()).thenReturn(new Configuration());
    request = new PointInTimeRestoreRequest.Builder().withBackupRootDir(BACKUP_ROOT_DIR).build();
  }

  protected BackupInfo getBackupInfo(BackupState state) {
    BackupInfo info = new BackupInfo();
    info.setState(state);
    return info;
  }

  protected abstract List<PitrBackupMetadata> callGetBackupMetadataWithBackupStates(
    BackupState... states) throws IOException;

  @Test
  void testGetBackupMetadataFiltersNonCompleteBackups() throws IOException {
    List<PitrBackupMetadata> result =
      callGetBackupMetadataWithBackupStates(BackupState.COMPLETE, BackupState.FAILED,
        BackupState.RUNNING);

    assertEquals(1, result.size());
    assertEquals("backup_0", result.get(0).getBackupId());
  }

  @Test
  void testGetBackupMetadataReturnsEmptyWhenNoCompleteBackups() throws IOException {
    List<PitrBackupMetadata> result =
      callGetBackupMetadataWithBackupStates(BackupState.RUNNING);

    assertTrue(result.isEmpty());
  }

  @Test
  void testGetBackupMetadataReturnsAllWhenAllComplete() throws IOException {
    List<PitrBackupMetadata> result =
      callGetBackupMetadataWithBackupStates(BackupState.COMPLETE, BackupState.COMPLETE);

    assertEquals(2, result.size());
    assertEquals("backup_0", result.get(0).getBackupId());
    assertEquals("backup_1", result.get(1).getBackupId());
  }
}
