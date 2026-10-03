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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
@Tag(MiscTests.TAG)
public class TestHFileArchiverFileStatuses {
  private static final TableName TABLE_NAME = TableName.valueOf("testArchiveFileStatuses");
  private static final RegionInfo REGION_INFO = MobUtils.getMobRegionInfo(TABLE_NAME);
  private static final byte[] FAMILY = Bytes.toBytes("family");

  private Configuration conf;
  private FileSystem fs;
  private Path tableDir;
  private Path archiveDir;
  private FileStatus source;
  private Path archivedFile;

  @BeforeEach
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();
    conf.set(HConstants.HBASE_DIR, "file:///hbase");
    tableDir = CommonFSUtils.getTableDir(MobUtils.getMobHome(conf), TABLE_NAME);
    archiveDir = HFileArchiveUtil.getStoreArchivePath(conf, REGION_INFO, tableDir, FAMILY);
    source = new FileStatus(10, false, 1, 1024, 1,
      new Path(MobUtils.getMobFamilyPath(conf, TABLE_NAME, "family"), "file"));
    archivedFile = new Path(archiveDir, source.getPath().getName());
    fs = mock(FileSystem.class);
    when(fs.mkdirs(any())).thenReturn(true);
    when(fs.rename(any(), any())).thenReturn(true);
  }

  private void archive(Collection<FileStatus> statuses) throws IOException {
    HFileArchiver.archiveStoreFileStatuses(conf, fs, REGION_INFO, FAMILY, statuses);
  }

  @Test
  public void testArchiveWithoutFetchingSourceMetadata() throws IOException {
    archive(Collections.singletonList(source));

    verify(fs).rename(source.getPath(), archivedFile);
    verify(fs).setTimes(eq(source.getPath()), anyLong(), eq(-1L));
    // With no archive conflict, the existing status is enough to identify the source file.
    verify(fs, never()).getFileStatus(source.getPath());
    verify(fs, never()).isFile(source.getPath());
  }

  @Test
  public void testEmptyInput() throws IOException {
    archive(Collections.emptyList());
    verifyNoInteractions(fs);
  }

  @Test
  public void testRejectDirectoryBeforeArchivingAnyFiles() {
    FileStatus directory = new FileStatus(0, true, 1, 0, 0, new Path(tableDir, "directory"));
    assertThrows(IOException.class, () -> archive(Arrays.asList(source, directory)));
    verifyNoInteractions(fs);
  }

  @Test
  public void testAlreadyArchived() throws IOException {
    when(fs.exists(archivedFile)).thenReturn(true);
    archive(Collections.singletonList(source));

    verify(fs, never()).rename(any(), any());
    verify(fs, never()).getFileStatus(any());
  }

  @Test
  public void testArchiveConflictWithSameLength() throws IOException {
    when(fs.exists(archivedFile)).thenReturn(true);
    when(fs.exists(source.getPath())).thenReturn(true);
    when(fs.getFileStatus(source.getPath())).thenReturn(source);
    when(fs.getFileStatus(archivedFile))
      .thenReturn(new FileStatus(source.getLen(), false, 1, 1024, 1, archivedFile));

    archive(Collections.singletonList(source));

    verify(fs).rename(eq(archivedFile), argThat(path -> path.getParent().equals(archiveDir)
      && path.getName().startsWith(source.getPath().getName() + ".")));
    verify(fs).rename(source.getPath(), archivedFile);
    verify(fs, never()).delete(any(), eq(false));
  }

  @Test
  public void testArchiveConflictWithDifferentLength() throws IOException {
    when(fs.exists(archivedFile)).thenReturn(true);
    when(fs.exists(source.getPath())).thenReturn(true);
    when(fs.getFileStatus(source.getPath())).thenReturn(source);
    when(fs.getFileStatus(archivedFile))
      .thenReturn(new FileStatus(source.getLen() + 1, false, 1, 1024, 1, archivedFile));

    FailedArchiveException error =
      assertThrows(FailedArchiveException.class, () -> archive(Collections.singletonList(source)));

    assertEquals(Collections.singletonList(source.getPath()), error.getFailedFiles());
    verify(fs, never()).rename(any(), any());
    verify(fs, never()).delete(any(), eq(false));
  }

  @Test
  public void testSourceDisappearsBeforeRename() throws IOException {
    doThrow(new FileNotFoundException()).when(fs).setTimes(eq(source.getPath()), anyLong(),
      eq(-1L));

    archive(Collections.singletonList(source));

    verify(fs, never()).rename(any(), any());
  }

  @Test
  public void testRetryRename() throws IOException {
    when(fs.rename(source.getPath(), archivedFile)).thenReturn(false, true);

    archive(Collections.singletonList(source));

    verify(fs, times(2)).rename(source.getPath(), archivedFile);
  }

  @Test
  public void testFailureDoesNotPreventOtherFilesFromBeingArchived() throws IOException {
    FileStatus other =
      new FileStatus(10, false, 1, 1024, 1, new Path(source.getPath().getParent(), "other"));
    when(fs.rename(source.getPath(), archivedFile)).thenReturn(false);

    FailedArchiveException error =
      assertThrows(FailedArchiveException.class, () -> archive(Arrays.asList(source, other)));

    assertEquals(Collections.singletonList(source.getPath()), error.getFailedFiles());
    verify(fs, atLeast(2)).rename(source.getPath(), archivedFile);
    verify(fs).rename(other.getPath(), new Path(archiveDir, "other"));
  }

  @Test
  public void testCannotCreateArchiveDirectory() throws IOException {
    when(fs.mkdirs(archiveDir)).thenReturn(false);
    assertThrows(IOException.class, () -> archive(Collections.singletonList(source)));
    verify(fs, never()).rename(any(), any());
  }
}
