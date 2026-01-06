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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ MapReduceTests.class, MediumTests.class })
public class TestWALInputFormat {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALInputFormat.class);

  @BeforeClass
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
    LocatedFileStatus lfs = Mockito.mock(LocatedFileStatus.class);
    long now = EnvironmentEdgeManager.currentTime();
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name." + now));
    WALInputFormat.addFile(lfss, lfs, now, now);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now - 1, now - 1);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now - 2, now - 1);
    assertEquals(1, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now - 2, now);
    assertEquals(2, lfss.size());
    WALInputFormat.addFile(lfss, lfs, Long.MIN_VALUE, now);
    assertEquals(3, lfss.size());
    WALInputFormat.addFile(lfss, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(4, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now, now + 2);
    assertEquals(5, lfss.size());
    WALInputFormat.addFile(lfss, lfs, now + 1, now + 2);
    assertEquals(5, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name"));
    WALInputFormat.addFile(lfss, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(6, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name.123"));
    WALInputFormat.addFile(lfss, lfs, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(7, lfss.size());
    Mockito.when(lfs.getPath()).thenReturn(new Path("/name." + now + ".meta"));
    WALInputFormat.addFile(lfss, lfs, now, now);
    assertEquals(8, lfss.size());
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
    assertTrue("Empty file should be ignored when IGNORE_EMPTY_FILES is true", splits.isEmpty());
  }

  @Test
  public void testEmptyFileIsIncludedWhenNotIgnored() throws IOException, InterruptedException {
    List<InputSplit> splits = getSplitsForEmptyFile(false);
    assertEquals("Empty file should be included when IGNORE_EMPTY_FILES is false", 1,
      splits.size());
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
}
