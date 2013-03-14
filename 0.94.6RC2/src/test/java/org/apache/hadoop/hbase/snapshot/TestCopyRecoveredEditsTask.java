/**
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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test that we correctly copy the recovered edits from a directory
 */
@Category(SmallTests.class)
public class TestCopyRecoveredEditsTask {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Test
  public void testCopyFiles() throws Exception {

    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("snapshot").build();
    ForeignExceptionDispatcher monitor = Mockito.mock(ForeignExceptionDispatcher.class);
    FileSystem fs = UTIL.getTestFileSystem();
    Path root = UTIL.getDataTestDir();
    String regionName = "regionA";
    Path regionDir = new Path(root, regionName);
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, root);

    try {
      // doesn't really matter where the region's snapshot directory is, but this is pretty close
      Path snapshotRegionDir = new Path(workingDir, regionName);
      fs.mkdirs(snapshotRegionDir);

      // put some stuff in the recovered.edits directory
      Path edits = HLog.getRegionDirRecoveredEditsDir(regionDir);
      fs.mkdirs(edits);
      // make a file with some data
      Path file1 = new Path(edits, "0000000000000002352");
      FSDataOutputStream out = fs.create(file1);
      byte[] data = new byte[] { 1, 2, 3, 4 };
      out.write(data);
      out.close();
      // make an empty file
      Path empty = new Path(edits, "empty");
      fs.createNewFile(empty);

      CopyRecoveredEditsTask task = new CopyRecoveredEditsTask(snapshot, monitor, fs, regionDir,
          snapshotRegionDir);
      CopyRecoveredEditsTask taskSpy = Mockito.spy(task);
      taskSpy.call();

      Path snapshotEdits = HLog.getRegionDirRecoveredEditsDir(snapshotRegionDir);
      FileStatus[] snapshotEditFiles = FSUtils.listStatus(fs, snapshotEdits);
      assertEquals("Got wrong number of files in the snapshot edits", 1, snapshotEditFiles.length);
      FileStatus file = snapshotEditFiles[0];
      assertEquals("Didn't copy expected file", file1.getName(), file.getPath().getName());

      Mockito.verify(monitor, Mockito.never()).receive(Mockito.any(ForeignException.class));
      Mockito.verify(taskSpy, Mockito.never()).snapshotFailure(Mockito.anyString(),
           Mockito.any(Exception.class));
    } finally {
      // cleanup the working directory
      FSUtils.delete(fs, regionDir, true);
      FSUtils.delete(fs, workingDir, true);
    }
  }

  /**
   * Check that we don't get an exception if there is no recovered edits directory to copy
   * @throws Exception on failure
   */
  @Test
  public void testNoEditsDir() throws Exception {
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("snapshot").build();
    ForeignExceptionDispatcher monitor = Mockito.mock(ForeignExceptionDispatcher.class);
    FileSystem fs = UTIL.getTestFileSystem();
    Path root = UTIL.getDataTestDir();
    String regionName = "regionA";
    Path regionDir = new Path(root, regionName);
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, root);
    try {
      // doesn't really matter where the region's snapshot directory is, but this is pretty close
      Path snapshotRegionDir = new Path(workingDir, regionName);
      fs.mkdirs(snapshotRegionDir);
      Path regionEdits = HLog.getRegionDirRecoveredEditsDir(regionDir);
      assertFalse("Edits dir exists already - it shouldn't", fs.exists(regionEdits));

      CopyRecoveredEditsTask task = new CopyRecoveredEditsTask(snapshot, monitor, fs, regionDir,
          snapshotRegionDir);
      task.call();
    } finally {
      // cleanup the working directory
      FSUtils.delete(fs, regionDir, true);
      FSUtils.delete(fs, workingDir, true);
    }
  }
}
