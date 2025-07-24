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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Unit tests for {@link BackupAdminImpl}.
 * <p>
 * This class improves test coverage by validating the behavior of key methods in BackupAdminImpl.
 * Some methods are made package-private to enable testing.
 */
@Category(SmallTests.class)
public class TestBackupAdminImpl {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupAdminImpl.class);

  private BackupAdminImpl backupAdminImpl;
  private BackupSystemTable mockTable;

  @Before
  public void setUp() {
    backupAdminImpl = new BackupAdminImpl(null);
    mockTable = mock(BackupSystemTable.class);
  }

  /**
   * Scenario: - The initial incremental table set contains "table1" and "table2" - Only "table1"
   * still exists in backup history Expectation: - The backup system should delete the existing set
   * - Then re-add a filtered set that includes only "table1"
   */
  @Test
  public void testFinalizeDelete_addsRetainedTablesBack() throws IOException {
    String backupRoot = "backupRoot1";
    List<String> backupRoots = Collections.singletonList(backupRoot);

    Set<TableName> initialTableSet =
      new HashSet<>(Arrays.asList(TableName.valueOf("ns:table1"), TableName.valueOf("ns:table2")));

    Map<TableName, List<BackupInfo>> backupHistory = new HashMap<>();
    backupHistory.put(TableName.valueOf("ns:table1"), List.of(new BackupInfo())); // Only table1
                                                                                  // retained

    when(mockTable.getIncrementalBackupTableSet(backupRoot))
      .thenReturn(new HashSet<>(initialTableSet));
    when(mockTable.getBackupHistoryForTableSet(initialTableSet, backupRoot))
      .thenReturn(backupHistory);

    backupAdminImpl.finalizeDelete(backupRoots, mockTable);

    // Always remove existing backup metadata
    verify(mockTable).deleteIncrementalBackupTableSet(backupRoot);

    // Re-add only retained tables (should be just table1)
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Set<TableName>> captor =
      (ArgumentCaptor<Set<TableName>>) (ArgumentCaptor<?>) ArgumentCaptor.forClass(Set.class);
    verify(mockTable).addIncrementalBackupTableSet(captor.capture(), eq(backupRoot));

    Set<TableName> retained = captor.getValue();
    assertEquals(1, retained.size());
    assertTrue(retained.contains(TableName.valueOf("ns:table1")));
  }

  /**
   * Scenario: - The incremental table set has "tableX" - No backups exist for this table
   * Expectation: - Backup metadata should be deleted - Nothing should be re-added since no tables
   * are retained
   */
  @Test
  public void testFinalizeDelete_retainedSetEmpty_doesNotAddBack() throws IOException {
    String backupRoot = "backupRoot2";
    List<String> backupRoots = List.of(backupRoot);

    Set<TableName> initialTableSet = Set.of(TableName.valueOf("ns:tableX"));
    Map<TableName, List<BackupInfo>> backupHistory = Map.of(); // No overlap

    when(mockTable.getIncrementalBackupTableSet(backupRoot))
      .thenReturn(new HashSet<>(initialTableSet));
    when(mockTable.getBackupHistoryForTableSet(initialTableSet, backupRoot))
      .thenReturn(backupHistory);

    backupAdminImpl.finalizeDelete(backupRoots, mockTable);

    // Delete should be called
    verify(mockTable).deleteIncrementalBackupTableSet(backupRoot);
    // No add back since retained set is empty
    verify(mockTable, never()).addIncrementalBackupTableSet(any(), eq(backupRoot));
  }

  /**
   * Scenario: - Two backup roots: - root1: one table with valid backup history → should be retained
   * - root2: one table with no history → should not be retained Expectation: - root1 metadata
   * deleted and re-added - root2 metadata only deleted
   */
  @Test
  public void testFinalizeDelete_multipleRoots() throws IOException {
    String root1 = "root1";
    String root2 = "root2";
    List<String> roots = List.of(root1, root2);

    TableName t1 = TableName.valueOf("ns:table1");
    TableName t2 = TableName.valueOf("ns:table2");

    // root1 setup
    when(mockTable.getIncrementalBackupTableSet(root1)).thenReturn(new HashSet<>(List.of(t1)));
    when(mockTable.getBackupHistoryForTableSet(Set.of(t1), root1))
      .thenReturn(Map.of(t1, List.of(new BackupInfo())));

    // root2 setup
    when(mockTable.getIncrementalBackupTableSet(root2)).thenReturn(new HashSet<>(List.of(t2)));
    when(mockTable.getBackupHistoryForTableSet(Set.of(t2), root2)).thenReturn(Map.of()); // empty
                                                                                         // history

    backupAdminImpl.finalizeDelete(roots, mockTable);

    // root1: should delete and re-add table
    verify(mockTable).deleteIncrementalBackupTableSet(root1);
    verify(mockTable).addIncrementalBackupTableSet(Set.of(t1), root1);

    // root2: delete only
    verify(mockTable).deleteIncrementalBackupTableSet(root2);
    verify(mockTable, never()).addIncrementalBackupTableSet(anySet(), eq(root2));
  }

  /**
   * Verifies that {@code cleanupBackupDir} correctly deletes the target backup directory for a
   * given table and backup ID using the mocked FileSystem.
   * <p>
   * This test ensures: - The correct path is constructed using BackupUtils. - FileSystem#delete is
   * invoked with that path.
   */
  @Test
  public void testCleanupBackupDir_deletesTargetDirSuccessfully() throws Exception {
    // Setup test input
    String backupId = "backup_001";
    String backupRootDir = "/backup/root";
    TableName table = TableName.valueOf("test_table");

    BackupInfo mockBackupInfo = mock(BackupInfo.class);
    when(mockBackupInfo.getBackupRootDir()).thenReturn(backupRootDir);
    when(mockBackupInfo.getBackupId()).thenReturn(backupId);

    Configuration conf = new Configuration();

    // Spy BackupAdminImpl to mock getFileSystem behavior
    backupAdminImpl = spy(backupAdminImpl);

    FileSystem mockFs = mock(FileSystem.class);
    Path expectedPath = new Path(BackupUtils.getTableBackupDir(backupRootDir, backupId, table));

    // Mock getFileSystem to return our mock FileSystem
    doReturn(mockFs).when(backupAdminImpl).getFileSystem(any(Path.class), eq(conf));
    when(mockFs.delete(expectedPath, true)).thenReturn(true);

    // Call the method under test
    backupAdminImpl.cleanupBackupDir(mockBackupInfo, table, conf);

    // Verify the FileSystem delete call with correct path
    verify(mockFs).delete(expectedPath, true);
  }

  /**
   * Verifies that {@code cleanupBackupDir} throws an {@link IOException} if the FileSystem
   * retrieval fails.
   * <p>
   * This test simulates an exception while trying to obtain the FileSystem, and expects the method
   * to propagate the exception.
   */
  @Test(expected = IOException.class)
  public void testCleanupBackupDir_throwsIOException() throws Exception {
    // Setup test input
    String backupId = "backup_003";
    String backupRootDir = "/backup/root";
    TableName table = TableName.valueOf("test_table");

    BackupInfo mockBackupInfo = mock(BackupInfo.class);
    when(mockBackupInfo.getBackupRootDir()).thenReturn(backupRootDir);
    when(mockBackupInfo.getBackupId()).thenReturn(backupId);

    Configuration conf = new Configuration();

    // Spy BackupAdminImpl to inject failure in getFileSystem
    backupAdminImpl = spy(backupAdminImpl);
    doThrow(new IOException("FS error")).when(backupAdminImpl).getFileSystem(any(Path.class),
      eq(conf));

    // Call method and expect IOException
    backupAdminImpl.cleanupBackupDir(mockBackupInfo, table, conf);
  }

  /**
   * Tests that when a current incremental backup is found in the history, all later incremental
   * backups for the same table are returned. This simulates rolling forward from the current backup
   * timestamp, capturing newer incremental backups that depend on it.
   */
  @Test
  public void testGetAffectedBackupSessions() throws IOException {
    BackupInfo current = mock(BackupInfo.class);
    TableName table = TableName.valueOf("test_table");

    when(current.getStartTs()).thenReturn(2000L);
    when(current.getBackupId()).thenReturn("backup_002");
    when(current.getBackupRootDir()).thenReturn("/backup/root");

    BackupInfo b0 = createBackupInfo("backup_000", 500L, BackupType.FULL, table);
    BackupInfo b1 = createBackupInfo("backup_001", 1000L, BackupType.INCREMENTAL, table);
    BackupInfo b2 = createBackupInfo("backup_002", 2000L, BackupType.INCREMENTAL, table); // current
    BackupInfo b3 = createBackupInfo("backup_003", 3000L, BackupType.INCREMENTAL, table);
    BackupInfo b4 = createBackupInfo("backup_004", 4000L, BackupType.INCREMENTAL, table);

    when(mockTable.getBackupHistory("/backup/root")).thenReturn(List.of(b4, b3, b2, b1, b0));

    List<BackupInfo> result = backupAdminImpl.getAffectedBackupSessions(current, table, mockTable);

    assertEquals(2, result.size());
    assertTrue(result.containsAll(List.of(b3, b4)));
  }

  /**
   * Tests that if a full backup appears after the current backup, the affected list is reset and
   * incremental backups following that full backup are not included. This ensures full backups act
   * as a reset boundary.
   */
  @Test
  public void testGetAffectedBackupSessions_resetsOnFullBackup() throws IOException {
    BackupInfo current = mock(BackupInfo.class);
    TableName table = TableName.valueOf("test_table");

    when(current.getStartTs()).thenReturn(1000L);
    when(current.getBackupId()).thenReturn("backup_001");
    when(current.getBackupRootDir()).thenReturn("/backup/root");

    BackupInfo b0 = createBackupInfo("backup_000", 500L, BackupType.FULL, table);
    BackupInfo b1 = createBackupInfo("backup_001", 1000L, BackupType.INCREMENTAL, table); // current
    BackupInfo b2 = createBackupInfo("backup_002", 2000L, BackupType.FULL, table);
    BackupInfo b3 = createBackupInfo("backup_003", 3000L, BackupType.INCREMENTAL, table);

    when(mockTable.getBackupHistory("/backup/root")).thenReturn(List.of(b3, b2, b1, b0));

    List<BackupInfo> result = backupAdminImpl.getAffectedBackupSessions(current, table, mockTable);

    assertTrue(result.isEmpty());
  }

  /**
   * Tests that backups for other tables are ignored, even if they are incremental and fall after
   * the current backup. Only backups affecting the specified table should be considered.
   */
  @Test
  public void testGetAffectedBackupSessions_skipsNonMatchingTable() throws IOException {
    BackupInfo current = mock(BackupInfo.class);
    TableName table = TableName.valueOf("test_table");

    when(current.getStartTs()).thenReturn(1000L);
    when(current.getBackupId()).thenReturn("backup_001");
    when(current.getBackupRootDir()).thenReturn("/backup/root");

    BackupInfo b0 = createBackupInfo("backup_000", 500L, BackupType.FULL, table);
    BackupInfo b1 = createBackupInfo("backup_001", 1000L, BackupType.INCREMENTAL, table); // current
    BackupInfo b2 = createBackupInfo("backup_002", 2000L, BackupType.INCREMENTAL, table);
    BackupInfo b3 = createBackupInfo("backup_003", 3000L, BackupType.INCREMENTAL,
      TableName.valueOf("other_table"));
    BackupInfo b4 = createBackupInfo("backup_004", 4000L, BackupType.INCREMENTAL, table);

    when(mockTable.getBackupHistory("/backup/root")).thenReturn(List.of(b4, b3, b2, b1, b0));

    List<BackupInfo> result = backupAdminImpl.getAffectedBackupSessions(current, table, mockTable);

    assertEquals(2, result.size());
    assertTrue(result.containsAll(List.of(b2, b4)));
  }

  private BackupInfo createBackupInfo(String id, long ts, BackupType type, TableName... tables) {
    BackupInfo info = mock(BackupInfo.class);
    when(info.getBackupId()).thenReturn(id);
    when(info.getStartTs()).thenReturn(ts);
    when(info.getType()).thenReturn(type);
    List<TableName> tableList = Arrays.asList(tables);
    when(info.getTableNames()).thenReturn(tableList);
    when(info.getTableListAsString()).thenReturn(tableList.toString());
    return info;
  }

  /**
   * Tests that when a table is removed from a backup image that still contains other tables, it
   * updates the BackupInfo correctly and does not delete the entire backup metadata.
   */
  @Test
  public void testRemoveTableFromBackupImage() throws IOException {
    // Arrange
    TableName tableToRemove = TableName.valueOf("ns", "t1");
    TableName remainingTable = TableName.valueOf("ns", "t2");

    BackupInfo info = new BackupInfo();
    info.setBackupId("backup_001");
    info.setTables(List.of(tableToRemove, remainingTable));
    info.setBackupRootDir("/backup/root");

    BackupSystemTable sysTable = mock(BackupSystemTable.class);
    Configuration conf = new Configuration();

    Connection mockConn = mock(Connection.class);
    when(mockConn.getConfiguration()).thenReturn(conf);
    backupAdminImpl = spy(new BackupAdminImpl(mockConn));

    doNothing().when(backupAdminImpl).cleanupBackupDir(any(), any(), any());

    try (MockedStatic<BackupUtils> mockedStatic = mockStatic(BackupUtils.class)) {
      mockedStatic.when(() -> BackupUtils.cleanupBackupData(any(), any()))
        .thenAnswer(invocation -> null); // no-op for safety

      // Act
      backupAdminImpl.removeTableFromBackupImage(info, tableToRemove, sysTable);

      // Assert
      assertEquals(1, info.getTableNames().size());
      assertTrue(info.getTableNames().contains(remainingTable));

      verify(sysTable).updateBackupInfo(info);
      verify(sysTable, never()).deleteBackupInfo(any());
      verify(backupAdminImpl).cleanupBackupDir(eq(info), eq(tableToRemove), eq(conf));

      mockedStatic.verifyNoInteractions(); // should not call static cleanup for partial table
                                           // removal
    }
  }

  /**
   * Tests that when the last table in a backup image is removed, the backup metadata is deleted
   * entirely and static cleanup is invoked.
   */
  @Test
  public void testRemoveTableFromBackupImageDeletesWhenLastTableRemoved() throws IOException {
    // Arrange
    TableName onlyTable = TableName.valueOf("ns", "t1");

    BackupInfo info = new BackupInfo();
    info.setBackupId("backup_002");
    info.setTables(new ArrayList<>(List.of(onlyTable)));
    info.setBackupRootDir("/backup/root");

    BackupSystemTable sysTable = mock(BackupSystemTable.class);
    Configuration conf = new Configuration();

    Connection mockConn = mock(Connection.class);
    when(mockConn.getConfiguration()).thenReturn(conf);
    backupAdminImpl = spy(new BackupAdminImpl(mockConn));

    doNothing().when(backupAdminImpl).cleanupBackupDir(any(), any(), any());

    try (MockedStatic<BackupUtils> mockedStatic = mockStatic(BackupUtils.class)) {
      mockedStatic.when(() -> BackupUtils.cleanupBackupData(any(), any()))
        .thenAnswer(invocation -> null); // no-op for static void

      // Act
      backupAdminImpl.removeTableFromBackupImage(info, onlyTable, sysTable);

      // Assert
      verify(sysTable).deleteBackupInfo("backup_002");
      verify(sysTable, never()).updateBackupInfo(any());

      mockedStatic.verify(() -> BackupUtils.cleanupBackupData(info, conf));
    }
  }

  /**
   * Tests that when a backup ID is not found, the method logs a warning and returns 0.
   */
  @Test
  public void testDeleteBackupWhenBackupInfoNotFound() throws IOException {
    String backupId = "backup_missing";
    BackupSystemTable sysTable = mock(BackupSystemTable.class);
    when(sysTable.readBackupInfo(backupId)).thenReturn(null);

    int result = backupAdminImpl.deleteBackup(backupId, sysTable);

    assertEquals(0, result);
    verify(sysTable, never()).deleteBackupInfo(any());
  }

  /**
   * Tests deleting a FULL backup when it is the last session for all its tables. Ensures cleanup is
   * called and metadata is deleted, but no other backups are affected.
   */
  @Test
  public void testDeleteFullBackupWithLastSession() throws IOException {
    TableName table = TableName.valueOf("ns", "t1");
    String backupId = "backup_full_001";
    BackupInfo info = new BackupInfo();
    info.setBackupId(backupId);
    info.setBackupRootDir("/backup/root");
    info.setTables(List.of(table));
    info.setStartTs(1000L);
    info.setType(BackupType.FULL);

    BackupSystemTable sysTable = mock(BackupSystemTable.class);
    Configuration conf = new Configuration();
    Connection mockConn = mock(Connection.class);
    when(mockConn.getConfiguration()).thenReturn(conf);
    backupAdminImpl = spy(new BackupAdminImpl(mockConn));

    when(sysTable.readBackupInfo(backupId)).thenReturn(info);
    when(backupAdminImpl.isLastBackupSession(sysTable, table, 1000L)).thenReturn(true);
    when(sysTable.readBulkLoadedFiles(backupId)).thenReturn(Map.of());

    doNothing().when(sysTable).deleteBackupInfo(backupId);

    try (MockedStatic<BackupUtils> mockedStatic = mockStatic(BackupUtils.class)) {
      mockedStatic.when(() -> BackupUtils.cleanupBackupData(eq(info), eq(conf)))
        .thenAnswer(inv -> null);

      int result = backupAdminImpl.deleteBackup(backupId, sysTable);

      assertEquals(1, result);
      verify(sysTable).deleteBackupInfo(backupId);
      mockedStatic.verify(() -> BackupUtils.cleanupBackupData(eq(info), eq(conf)));
    }
  }

  /**
   * Tests that deleteBackup will update other backups by removing the table when it's not the last
   * session.
   */
  @Test
  public void testDeleteBackupWithAffectedSessions() throws IOException {
    TableName table = TableName.valueOf("ns", "t1");
    String backupId = "backup_inc_001";
    BackupInfo current = new BackupInfo();
    current.setBackupId(backupId);
    current.setBackupRootDir("/backup/root");
    current.setTables(List.of(table));
    current.setStartTs(2000L);
    current.setType(BackupType.INCREMENTAL);

    BackupInfo dependent = new BackupInfo();
    dependent.setBackupId("backup_inc_002");
    dependent.setBackupRootDir("/backup/root");
    dependent.setTables(new ArrayList<>(List.of(table)));
    dependent.setStartTs(3000L);
    dependent.setType(BackupType.INCREMENTAL);

    BackupSystemTable sysTable = mock(BackupSystemTable.class);
    Configuration conf = new Configuration();
    Connection mockConn = mock(Connection.class);
    when(mockConn.getConfiguration()).thenReturn(conf);
    backupAdminImpl = spy(new BackupAdminImpl(mockConn));

    when(sysTable.readBackupInfo(backupId)).thenReturn(current);
    when(sysTable.readBulkLoadedFiles(backupId)).thenReturn(Map.of());
    when(backupAdminImpl.isLastBackupSession(sysTable, table, 2000L)).thenReturn(false);
    doReturn(List.of(dependent)).when(backupAdminImpl).getAffectedBackupSessions(current, table,
      sysTable);

    doNothing().when(backupAdminImpl).removeTableFromBackupImage(eq(dependent), eq(table),
      eq(sysTable));

    try (MockedStatic<BackupUtils> mockedStatic = mockStatic(BackupUtils.class)) {
      mockedStatic.when(() -> BackupUtils.cleanupBackupData(eq(current), eq(conf)))
        .thenAnswer(inv -> null);

      int result = backupAdminImpl.deleteBackup(backupId, sysTable);

      assertEquals(1, result);
      verify(backupAdminImpl).removeTableFromBackupImage(dependent, table, sysTable);
      verify(sysTable).deleteBackupInfo(backupId);
      mockedStatic.verify(() -> BackupUtils.cleanupBackupData(current, conf));
    }
  }

  /**
   * Tests that deleteBackup will remove bulk-loaded files and handle exceptions gracefully.
   */
  @Test
  public void testDeleteBackupWithBulkLoadedFiles() throws Exception {
    // Set up test data
    TableName table = TableName.valueOf("ns", "t1");
    String backupId = "backup_with_bulkload";
    Path dummyPath = new Path("/bulk/load/file1");
    Map<byte[], String> bulkFiles = Map.of("k1".getBytes(), dummyPath.toString());

    // BackupInfo mock
    BackupInfo info = new BackupInfo();
    info.setBackupId(backupId);
    info.setBackupRootDir("/backup/root");
    info.setTables(List.of(table));
    info.setStartTs(1500L);
    info.setType(BackupType.FULL);

    // Create mock objects
    Configuration conf = new Configuration();
    Connection conn = mock(Connection.class);
    when(conn.getConfiguration()).thenReturn(conf);

    BackupSystemTable sysTable = mock(BackupSystemTable.class);
    when(sysTable.readBackupInfo(backupId)).thenReturn(info);
    when(sysTable.readBulkLoadedFiles(backupId)).thenReturn(bulkFiles);

    FileSystem fs = mock(FileSystem.class);

    try (MockedStatic<FileSystem> fsStatic = mockStatic(FileSystem.class)) {
      fsStatic.when(() -> FileSystem.get(conf)).thenReturn(fs);
      when(fs.delete(dummyPath)).thenReturn(true); // Simulate successful delete

      // Create spy on BackupAdminImpl
      BackupAdminImpl backupAdmin = spy(new BackupAdminImpl(conn));
      when(backupAdmin.isLastBackupSession(sysTable, table, 1500L)).thenReturn(true);

      // No-ops for cleanup
      doNothing().when(sysTable).deleteBackupInfo(backupId);
      doNothing().when(sysTable).deleteBulkLoadedRows(any());

      try (MockedStatic<BackupUtils> staticMock = mockStatic(BackupUtils.class)) {
        staticMock.when(() -> BackupUtils.cleanupBackupData(info, conf))
          .thenAnswer(invocation -> null);

        // Execute method
        int result = backupAdmin.deleteBackup(backupId, sysTable);

        // Assertions
        assertEquals(1, result);
        verify(fs).delete(dummyPath);
        verify(sysTable).deleteBulkLoadedRows(any());
        verify(sysTable).deleteBackupInfo(backupId);
        staticMock.verify(() -> BackupUtils.cleanupBackupData(info, conf), times(1));
      }
    }
  }

  /**
   * Verifies that checkIfValidForMerge succeeds with valid INCREMENTAL, COMPLETE images from the
   * same destination and no holes in the backup sequence.
   */
  @Test
  public void testCheckIfValidForMerge_validCase() throws IOException {
    String[] ids = { "b1", "b2" };
    TableName t1 = TableName.valueOf("ns", "t1");
    String dest = "/backup/root";

    BackupInfo b1 =
      createBackupInfo("b1", BackupType.INCREMENTAL, BackupState.COMPLETE, 1000L, dest, t1);
    BackupInfo b2 =
      createBackupInfo("b2", BackupType.INCREMENTAL, BackupState.COMPLETE, 2000L, dest, t1);

    BackupSystemTable table = mock(BackupSystemTable.class);
    when(table.readBackupInfo("b1")).thenReturn(b1);
    when(table.readBackupInfo("b2")).thenReturn(b2);
    when(table.getBackupHistory(eq(-1), any(), any(), any(), any(), any()))
      .thenReturn(List.of(b1, b2));

    new BackupAdminImpl(mock(Connection.class)).checkIfValidForMerge(ids, table);
  }

  /**
   * Verifies that checkIfValidForMerge fails if a FULL backup is included.
   */
  @Test(expected = IOException.class)
  public void testCheckIfValidForMerge_failsWithFullBackup() throws IOException {
    String[] ids = { "b1" };
    TableName t1 = TableName.valueOf("ns", "t1");

    BackupInfo b1 =
      createBackupInfo("b1", BackupType.FULL, BackupState.COMPLETE, 1000L, "/dest", t1);

    BackupSystemTable table = mock(BackupSystemTable.class);
    when(table.readBackupInfo("b1")).thenReturn(b1);

    new BackupAdminImpl(mock(Connection.class)).checkIfValidForMerge(ids, table);
  }

  /**
   * Verifies that checkIfValidForMerge fails if one of the provided backup IDs is not found in the
   * system table (i.e., null is returned).
   */
  @Test(expected = IOException.class)
  public void testCheckIfValidForMerge_failsWhenBackupInfoNotFound() throws IOException {
    String[] ids = { "b_missing" };

    BackupSystemTable table = mock(BackupSystemTable.class);
    when(table.readBackupInfo("b_missing")).thenReturn(null);

    new BackupAdminImpl(mock(Connection.class)).checkIfValidForMerge(ids, table);
  }

  /**
   * Verifies that checkIfValidForMerge fails when backups come from different destinations.
   */
  @Test(expected = IOException.class)
  public void testCheckIfValidForMerge_failsWithDifferentDestinations() throws IOException {
    String[] ids = { "b1", "b2" };
    TableName t1 = TableName.valueOf("ns", "t1");

    BackupInfo b1 =
      createBackupInfo("b1", BackupType.INCREMENTAL, BackupState.COMPLETE, 1000L, "/dest1", t1);
    BackupInfo b2 =
      createBackupInfo("b2", BackupType.INCREMENTAL, BackupState.COMPLETE, 2000L, "/dest2", t1);

    BackupSystemTable table = mock(BackupSystemTable.class);
    when(table.readBackupInfo("b1")).thenReturn(b1);
    when(table.readBackupInfo("b2")).thenReturn(b2);

    new BackupAdminImpl(mock(Connection.class)).checkIfValidForMerge(ids, table);
  }

  /**
   * Verifies that checkIfValidForMerge fails if any backup is not in COMPLETE state.
   */
  @Test(expected = IOException.class)
  public void testCheckIfValidForMerge_failsWithNonCompleteState() throws IOException {
    String[] ids = { "b1" };
    TableName t1 = TableName.valueOf("ns", "t1");

    BackupInfo b1 =
      createBackupInfo("b1", BackupType.INCREMENTAL, BackupState.RUNNING, 1000L, "/dest", t1);

    BackupSystemTable table = mock(BackupSystemTable.class);
    when(table.readBackupInfo("b1")).thenReturn(b1);

    new BackupAdminImpl(mock(Connection.class)).checkIfValidForMerge(ids, table);
  }

  /**
   * Verifies that checkIfValidForMerge fails when there is a "hole" in the backup sequence — i.e.,
   * a required image from the full backup history is missing in the input list.
   */
  @Test(expected = IOException.class)
  public void testCheckIfValidForMerge_failsWhenHoleInImages() throws IOException {
    TableName t1 = TableName.valueOf("ns", "t1");
    String dest = "/backup/root";

    BackupInfo b1 =
      createBackupInfo("b1", BackupType.INCREMENTAL, BackupState.COMPLETE, 1000L, dest, t1);
    BackupInfo b2 =
      createBackupInfo("b2", BackupType.INCREMENTAL, BackupState.COMPLETE, 2000L, dest, t1);
    BackupInfo b3 =
      createBackupInfo("b3", BackupType.INCREMENTAL, BackupState.COMPLETE, 3000L, dest, t1);

    BackupSystemTable table = mock(BackupSystemTable.class);
    when(table.readBackupInfo("b1")).thenReturn(b1);
    when(table.readBackupInfo("b2")).thenReturn(b2);
    when(table.readBackupInfo("b3")).thenReturn(b3);

    when(table.getBackupHistory(eq(-1), any(), any(), any(), any(), any()))
      .thenReturn(List.of(b1, b2, b3));

    // Simulate a "hole" by omitting b2 from images
    String[] idsWithHole = { "b1", "b3" };
    new BackupAdminImpl(mock(Connection.class)).checkIfValidForMerge(idsWithHole, table);
  }

  private BackupInfo createBackupInfo(String id, BackupType type, BackupInfo.BackupState state,
    long ts, String dest, TableName... tables) {
    BackupInfo info = new BackupInfo();
    info.setBackupId(id);
    info.setType(type);
    info.setState(state);
    info.setStartTs(ts);
    info.setBackupRootDir(dest);
    info.setTables(List.of(tables));
    return info;
  }
}
