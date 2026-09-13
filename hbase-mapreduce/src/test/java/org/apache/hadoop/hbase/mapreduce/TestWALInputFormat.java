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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(MapReduceTests.TAG)
@Tag(MediumTests.TAG)
public class TestWALInputFormat {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

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
    List<FileStatus> result = new ArrayList<>();
    LocatedFileStatus lfs = Mockito.mock(LocatedFileStatus.class);
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name." + created));
    Mockito.when(lfs.getModificationTime()).thenReturn(mtime);
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
  }

  @Test
  public void testHandlesArchivedWALFiles() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    JobContext ctx = Mockito.mock(JobContext.class);
    Mockito.when(ctx.getConfiguration()).thenReturn(conf);
    Job job = Job.getInstance(conf);
    TableMapReduceUtil.initCredentialsForCluster(job, conf);
    Mockito.when(ctx.getCredentials()).thenReturn(job.getCredentials());

    // Setup WAL file, then archive it
    HRegionServer rs = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    AbstractFSWAL wal = (AbstractFSWAL) rs.getWALs().get(0);
    Path walPath = wal.getCurrentFileName();
    TEST_UTIL.getConfiguration().set(FileInputFormat.INPUT_DIR, walPath.toString());
    TEST_UTIL.getConfiguration().set(WALPlayer.INPUT_FILES_SEPARATOR_KEY, ";");

    Path rootDir = CommonFSUtils.getWALRootDir(conf);
    Path archiveWal = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    archiveWal = new Path(archiveWal, walPath.getName());
    TEST_UTIL.getTestFileSystem().delete(walPath, true);
    TEST_UTIL.getTestFileSystem().mkdirs(archiveWal.getParent());
    TEST_UTIL.getTestFileSystem().create(archiveWal).close();

    // Test for that we can read from the archived WAL file
    WALInputFormat wif = new WALInputFormat();
    List<InputSplit> splits = wif.getSplits(ctx);
    assertEquals(1, splits.size());
    WALInputFormat.WALSplit split = (WALInputFormat.WALSplit) splits.get(0);
    assertEquals(archiveWal.toString(), split.getLogFileName());
  }

  @Test
  public void testEmptyFileIsIgnoredWhenConfigured() throws IOException, InterruptedException {
    List<InputSplit> splits = getSplitsForEmptyFile(true);
    assertTrue(splits.isEmpty(), "Empty file should be ignored when IGNORE_EMPTY_FILES is true");
  }

  @Test
  public void testEmptyFileIsIncludedWhenNotIgnored() throws IOException, InterruptedException {
    List<InputSplit> splits = getSplitsForEmptyFile(false);
    assertEquals(1, splits.size(),
      "Empty file should be included when IGNORE_EMPTY_FILES is false");
  }

  private List<InputSplit> getSplitsForEmptyFile(boolean ignoreEmptyFiles)
    throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.setBoolean(WALPlayer.IGNORE_EMPTY_FILES, ignoreEmptyFiles);

    JobContext jobContext = Mockito.mock(JobContext.class);
    Mockito.when(jobContext.getConfiguration()).thenReturn(conf);

    LocatedFileStatus emptyFile = Mockito.mock(LocatedFileStatus.class);
    Mockito.when(emptyFile.getLen()).thenReturn(0L);
    Mockito.when(emptyFile.getPath()).thenReturn(new Path("/empty.wal"));

    WALInputFormat inputFormat = new WALInputFormat() {
      @Override
      Path[] getInputPaths(Configuration conf) {
        return new Path[] { new Path("/input") };
      }

      @Override
      List<FileStatus> getFiles(FileSystem fs, Path inputPath, long startTime, long endTime,
        Configuration conf) {
        return Collections.singletonList(emptyFile);
      }
    };

    return inputFormat.getSplits(jobContext, "", "");
  }

  /**
   * Test that an empty WAL file (which causes WALHeaderEOFException) is gracefully handled and
   * skipped rather than causing the job to fail.
   */
  @Test
  public void testHandlesEmptyWALFile() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    // Create an empty WAL file
    Path walRootDir = CommonFSUtils.getWALRootDir(conf);
    Path emptyWalFile =
      new Path(walRootDir, "WALs/empty-wal-test/empty." + EnvironmentEdgeManager.currentTime());
    TEST_UTIL.getTestFileSystem().mkdirs(emptyWalFile.getParent());
    TEST_UTIL.getTestFileSystem().create(emptyWalFile).close();

    try {
      JobContext ctx = Mockito.mock(JobContext.class);
      conf.set(FileInputFormat.INPUT_DIR, emptyWalFile.toString());
      conf.set(WALPlayer.INPUT_FILES_SEPARATOR_KEY, ";");
      Mockito.when(ctx.getConfiguration()).thenReturn(conf);
      Job job = Job.getInstance(conf);
      TableMapReduceUtil.initCredentialsForCluster(job, conf);
      Mockito.when(ctx.getCredentials()).thenReturn(job.getCredentials());

      // Create record reader and verify it handles the empty file gracefully
      try (WALInputFormat.WALKeyRecordReader reader = new WALInputFormat.WALKeyRecordReader()) {
        TaskAttemptContext taskCtx = Mockito.mock(TaskAttemptContext.class);
        Mockito.when(taskCtx.getConfiguration()).thenReturn(conf);

        WALInputFormat wif = new WALInputFormat();
        List<InputSplit> splits = wif.getSplits(ctx);
        assertEquals(1, splits.size());
        WALInputFormat.WALSplit split = (WALInputFormat.WALSplit) splits.get(0);

        // This should not throw WALHeaderEOFException - it should return false for nextKeyValue()
        reader.initialize(split, taskCtx);
        // nextKeyValue() should return false since the file is empty (reader is null)
        assertFalse(reader.nextKeyValue());
      }
    } finally {
      TEST_UTIL.getTestFileSystem().delete(emptyWalFile.getParent(), true);
    }
  }

}
