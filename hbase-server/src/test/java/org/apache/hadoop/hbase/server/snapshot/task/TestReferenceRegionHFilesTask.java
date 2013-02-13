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
package org.apache.hadoop.hbase.server.snapshot.task;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestReferenceRegionHFilesTask {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Test
  public void testRun() throws IOException {
    FileSystem fs = UTIL.getTestFileSystem();
    // setup the region internals
    Path testdir = UTIL.getDataTestDir();
    Path regionDir = new Path(testdir, "region");
    Path family1 = new Path(regionDir, "fam1");
    // make an empty family
    Path family2 = new Path(regionDir, "fam2");
    fs.mkdirs(family2);

    // add some files to family 1
    Path file1 = new Path(family1, "05f99689ae254693836613d1884c6b63");
    fs.createNewFile(file1);
    Path file2 = new Path(family1, "7ac9898bf41d445aa0003e3d699d5d26");
    fs.createNewFile(file2);

    // create the snapshot directory
    Path snapshotRegionDir = new Path(testdir, HConstants.SNAPSHOT_DIR_NAME);
    fs.mkdirs(snapshotRegionDir);

    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("name")
        .setTable("table").build();
    SnapshotExceptionSnare monitor = Mockito.mock(SnapshotExceptionSnare.class);
    ReferenceRegionHFilesTask task = new ReferenceRegionHFilesTask(snapshot, monitor, regionDir,
        fs, snapshotRegionDir);
    task.run();

    // make sure we never get an error
    Mockito.verify(monitor, Mockito.never()).snapshotFailure(Mockito.anyString(),
      Mockito.eq(snapshot));
    Mockito.verify(monitor, Mockito.never()).snapshotFailure(Mockito.anyString(),
      Mockito.eq(snapshot), Mockito.any(Exception.class));

    // verify that all the hfiles get referenced
    List<String> hfiles = new ArrayList<String>(2);
    FileStatus[] regions = FSUtils.listStatus(fs, snapshotRegionDir);
    for (FileStatus region : regions) {
      FileStatus[] fams = FSUtils.listStatus(fs, region.getPath());
      for (FileStatus fam : fams) {
        FileStatus[] files = FSUtils.listStatus(fs, fam.getPath());
        for (FileStatus file : files) {
          hfiles.add(file.getPath().getName());
        }
      }
    }
    assertTrue("Didn't reference :" + file1, hfiles.contains(file1.getName()));
    assertTrue("Didn't reference :" + file1, hfiles.contains(file2.getName()));
  }
}
