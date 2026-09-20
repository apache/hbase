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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(MapReduceTests.TAG)
@Tag(SmallTests.TAG)
public class TestWALInputFormat {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void setupClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.createWALRootDir();
  }

  /**
   * Test the primitive start/end time filtering.
   */
  @Test
  public void testAddFile() {
    List<FileStatus> lfss = new ArrayList<>();
    // a plain FileSystem is never reported closed, so nothing is skipped on the startTime side
    FileSystem fs = Mockito.mock(FileSystem.class);
    LocatedFileStatus lfs = Mockito.mock(LocatedFileStatus.class);
    long now = EnvironmentEdgeManager.currentTime();
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name." + now));
    WALInputFormat.addFile(lfss, fs, lfs, now, now);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, fs, lfs, now - 1, now - 1);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, fs, lfs, now - 2, now - 1);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, fs, lfs, now - 2, now);
    assertEquals(2, lfss.size());
    WALInputFormat.addFile(lfss, fs, lfs, Long.MIN_VALUE, now);
    assertEquals(3, lfss.size());
    WALInputFormat.addFile(lfss, fs, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(4, lfss.size());
    WALInputFormat.addFile(lfss, fs, lfs, now, now + 2);
    assertEquals(5, lfss.size());
    // created before startTime, but it may have stayed open and collected in-range entries
    WALInputFormat.addFile(lfss, fs, lfs, now + 1, now + 2);
    assertEquals(6, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name"));
    WALInputFormat.addFile(lfss, fs, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(7, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name.123"));
    WALInputFormat.addFile(lfss, fs, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(8, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name." + now + ".meta"));
    WALInputFormat.addFile(lfss, fs, lfs, now, now);
    assertEquals(9, lfss.size());
  }

  private static boolean isKept(FileSystem fs, long created, long mtime, long start, long end) {
    return isKept(fs, created, mtime, mtime, start, end);
  }

  private static boolean isKept(FileSystem fs, long created, long staleMtime, long refreshedMtime,
    long start, long end) {
    List<FileStatus> result = new ArrayList<>();
    Path path = new Path("/name." + created);
    LocatedFileStatus lfs = Mockito.mock(LocatedFileStatus.class);
    Mockito.when(lfs.getPath()).thenReturn(path);
    Mockito.when(lfs.getModificationTime()).thenReturn(staleMtime);
    try {
      FileStatus refreshed = Mockito.mock(FileStatus.class);
      Mockito.when(refreshed.getModificationTime()).thenReturn(refreshedMtime);
      Mockito.when(fs.getFileStatus(path)).thenReturn(refreshed);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    WALInputFormat.addFile(result, fs, lfs, start, end);
    return !result.isEmpty();
  }

  /**
   * The name of a WAL carries its creation time, which only bounds its entries from below. A WAL
   * stays open until it rolls, so one created before startTime can still hold entries in range.
   * Only a closed file has a final modification time that can rule it out.
   */
  @Test
  public void testAddFileUsesModificationTimeOfClosedFilesOnly() throws Exception {
    long now = EnvironmentEdgeManager.currentTime();

    DistributedFileSystem closed = Mockito.mock(DistributedFileSystem.class);
    Mockito.when(closed.isFileClosed(Mockito.any())).thenReturn(true);
    DistributedFileSystem open = Mockito.mock(DistributedFileSystem.class);
    Mockito.when(open.isFileClosed(Mockito.any())).thenReturn(false);

    // Closed, and its last write predates the window: nothing in it can be in range.
    assertFalse(isKept(closed, now - 100, now - 50, now, now + 100));

    // Same file, but the window opens before it was closed, so it spans the boundary.
    assertTrue(isKept(closed, now - 100, now - 50, now - 60, now + 100));

    // Still open. Its mtime is stuck near the creation time and says nothing about the entries
    // it may yet receive, so it has to be kept even though mtime is before the window.
    assertTrue(isKept(open, now - 100, now - 100, now, now + 100));

    // Created after the window closed: every entry in it is later still.
    assertFalse(isKept(closed, now + 200, now + 200, now, now + 100));

    // Race condition: file closed between listLocatedStatus and isFileClosed. The stale mtime
    // from the listing predates the window, but the refreshed mtime (after close) does not.
    assertTrue(isKept(closed, now - 100, now - 50, now + 10, now, now + 100));

    // ViewDistributedFileSystem wrapping non-HDFS storage: isFileClosed throws
    // UnsupportedOperationException. The file must be kept (same as non-HDFS).
    DistributedFileSystem unsupported = Mockito.mock(DistributedFileSystem.class);
    Mockito.when(unsupported.isFileClosed(Mockito.any()))
      .thenThrow(new UnsupportedOperationException("mounted fs"));
    assertTrue(isKept(unsupported, now - 100, now - 50, now, now + 100));
  }
}
