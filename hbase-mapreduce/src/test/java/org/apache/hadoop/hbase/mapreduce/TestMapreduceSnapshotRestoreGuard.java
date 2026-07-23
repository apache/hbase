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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.MapreduceHFileArchiver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Guard tests (HBASE-29435) for the MapReduce snapshot-scanning restore path. These exercise the
 * six-argument {@link RestoreSnapshotHelper#copySnapshotForScanner} overload used by the MapReduce
 * paths, injecting the MapReduce-local {@link MapreduceHFileArchiver}. The guard rejects a restore
 * directory that would let a MapReduce job archive (and ultimately delete) production HFiles.
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestMapreduceSnapshotRestoreGuard {

  protected final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  protected Configuration conf;
  protected FileSystem fs;
  protected Path rootDir;

  @BeforeAll
  public static void setupCluster() throws Exception {
    TEST_UTIL.getConfiguration().setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 3);
    TEST_UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDownCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setup() throws Exception {
    rootDir = TEST_UTIL.getDataTestDir("testRestore");
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    CommonFSUtils.setRootDir(conf, rootDir);
    // Turn off balancer so it doesn't cut in and mess up our placements.
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  /**
   * Guard (HBASE-29435): a restore directory exactly equal to the HBase root directory must be
   * rejected before any filesystem work, to avoid archiving/deleting production data.
   */
  @Test
  public void testRejectRestoreDirEqualToRootDir() {
    Path root = new Path("/hbase");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
      () -> RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, root, root, "snap",
        new MapreduceHFileArchiver()));
    assertTrue(e.getMessage().contains("BLOCKED"), e.getMessage());
    assertTrue(e.getMessage().contains("cannot be the HBase root directory"), e.getMessage());
  }

  /**
   * Guard (HBASE-29435): a restore directory nested under the HBase root directory must be rejected
   * by the path check (the sub-directory branch).
   */
  @Test
  public void testRejectRestoreDirUnderRootDir() {
    Path root = new Path("/hbase");
    Path restoreDir = new Path(root, "data/.tmp-restore");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
      () -> RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, root, restoreDir, "snap",
        new MapreduceHFileArchiver()));
    assertTrue(e.getMessage().contains("BLOCKED"), e.getMessage());
    assertTrue(e.getMessage().contains("cannot be the HBase root directory or a sub"),
      e.getMessage());
  }

  /**
   * Guard (HBASE-29435): the operation filesystem must host the HBase root directory. Validates the
   * first filesystem check (the passed-in {@code fs} vs rootDir). A sibling restoreDir is used so
   * only this check can fire.
   */
  @Test
  public void testRejectMismatchedFilesystem() throws IOException {
    Path hdfsRoot = TEST_UTIL.getDefaultRootDirPath();
    FileSystem localFs = FileSystem.getLocal(conf);
    Path restoreDir = new Path(hdfsRoot.getParent(), "mr-restore-fs");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
      () -> RestoreSnapshotHelper.copySnapshotForScanner(conf, localFs, hdfsRoot, restoreDir,
        "snap", new MapreduceHFileArchiver()));
    assertTrue(e.getMessage().contains("does not match the HBase root directory filesystem"),
      e.getMessage());
  }

  /**
   * Guard (HBASE-29435): the restore directory must live on the same filesystem as the HBase root
   * directory, even when the operation filesystem already matches root. Validates the second,
   * distinct filesystem check (restoreDir vs rootDir), which is not covered by the operation-fs
   * check above.
   */
  @Test
  public void testRejectRestoreDirOnDifferentFilesystem() throws IOException {
    Path hdfsRoot = TEST_UTIL.getDefaultRootDirPath();
    FileSystem rootFs = hdfsRoot.getFileSystem(conf);
    // Operation fs matches root, but restoreDir is explicitly on the local filesystem.
    Path localRestore = new Path("file:///tmp/mr-restore-" + System.nanoTime());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
      () -> RestoreSnapshotHelper.copySnapshotForScanner(conf, rootFs, hdfsRoot, localRestore,
        "snap", new MapreduceHFileArchiver()));
    assertTrue(e.getMessage().contains("Filesystems for restore directory"), e.getMessage());
  }

  /**
   * Negative contract: a restore directory that is a sibling of (not under) the HBase root
   * directory must not be blocked by the guard. The snapshot does not exist, so any failure comes
   * from snapshot loading downstream, never from the guard.
   */
  @Test
  public void testSiblingRestoreDirNotBlocked() throws IOException {
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    CommonFSUtils.setRootDir(conf, rootDir);
    fs = rootDir.getFileSystem(conf);
    Path siblingRestore = new Path("/hbase/.tmp-sibling-restore");
    try {
      RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, siblingRestore,
        "nonexistent-snapshot", new MapreduceHFileArchiver());
    } catch (Exception e) {
      String msg = e.getMessage();
      assertFalse(msg != null && msg.contains("BLOCKED"),
        "sibling restoreDir must not be blocked by the guard: " + msg);
    }
  }
}
