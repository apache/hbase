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
package org.apache.hadoop.hbase.master.snapshot;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that we correctly reload the cache, filter directories, etc.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestSnapshotFileCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotFileCache.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotFileCache.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  // don't refresh the cache unless we tell it to
  private static final long PERIOD = Long.MAX_VALUE;
  private static FileSystem fs;
  private static Path rootDir;
  private static Path snapshotDir;

  @BeforeClass
  public static void startCluster() throws Exception {
    UTIL.startMiniDFSCluster(1);
    fs = UTIL.getDFSCluster().getFileSystem();
    rootDir = UTIL.getDefaultRootDirPath();
    snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    UTIL.shutdownMiniDFSCluster();
  }

  @After
  public void cleanupFiles() throws Exception {
    // cleanup the snapshot directory
    fs.delete(snapshotDir, true);
  }

  @Test
  public void testLoadAndDelete() throws IOException {
    SnapshotFileCache cache = new SnapshotFileCache(fs, rootDir, PERIOD, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());

    createAndTestSnapshotV1(cache, "snapshot1a", false, true, false);

    createAndTestSnapshotV2(cache, "snapshot2a", false, true, false);
  }

  @Test
  public void testReloadModifiedDirectory() throws IOException {
    SnapshotFileCache cache = new SnapshotFileCache(fs, rootDir, PERIOD, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());

    createAndTestSnapshotV1(cache, "snapshot1", false, true, false);
    // now delete the snapshot and add a file with a different name
    createAndTestSnapshotV1(cache, "snapshot1", false, false, false);

    createAndTestSnapshotV2(cache, "snapshot2", false, true, false);
    // now delete the snapshot and add a file with a different name
    createAndTestSnapshotV2(cache, "snapshot2", false, false, false);
  }

  @Test
  public void testSnapshotTempDirReload() throws IOException {
    SnapshotFileCache cache =
        new SnapshotFileCache(fs, rootDir, PERIOD, 10000000, "test-snapshot-file-cache-refresh", new SnapshotFiles());

    // Add a new non-tmp snapshot
    createAndTestSnapshotV1(cache, "snapshot0v1", false, false, false);
    createAndTestSnapshotV1(cache, "snapshot0v2", false, false, false);
  }

  @Test
  public void testCacheUpdatedWhenLastModifiedOfSnapDirNotUpdated() throws IOException {
    SnapshotFileCache cache = new SnapshotFileCache(fs, rootDir, PERIOD, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());

    // Add a new non-tmp snapshot
    createAndTestSnapshotV1(cache, "snapshot1v1", false, false, true);
    createAndTestSnapshotV1(cache, "snapshot1v2", false, false, true);

    // Add a new tmp snapshot
    createAndTestSnapshotV2(cache, "snapshot2v1", true, false, true);

    // Add another tmp snapshot
    createAndTestSnapshotV2(cache, "snapshot2v2", true, false, true);
  }

  class SnapshotFiles implements SnapshotFileCache.SnapshotFileInspector {
    @Override
    public Collection<String> filesUnderSnapshot(final Path snapshotDir) throws IOException {
      Collection<String> files =  new HashSet<>();
      files.addAll(SnapshotReferenceUtil.getHFileNames(UTIL.getConfiguration(), fs, snapshotDir));
      return files;
    }
  };

  private SnapshotMock.SnapshotBuilder createAndTestSnapshotV1(final SnapshotFileCache cache,
      final String name, final boolean tmp, final boolean removeOnExit, boolean setFolderTime)
      throws IOException {
    SnapshotMock snapshotMock = new SnapshotMock(UTIL.getConfiguration(), fs, rootDir);
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV1(name, name);
    createAndTestSnapshot(cache, builder, tmp, removeOnExit, setFolderTime);
    return builder;
  }

  private void createAndTestSnapshotV2(final SnapshotFileCache cache, final String name,
      final boolean tmp, final boolean removeOnExit, boolean setFolderTime) throws IOException {
    SnapshotMock snapshotMock = new SnapshotMock(UTIL.getConfiguration(), fs, rootDir);
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2(name, name);
    createAndTestSnapshot(cache, builder, tmp, removeOnExit, setFolderTime);
  }

  private void createAndTestSnapshot(final SnapshotFileCache cache,
      final SnapshotMock.SnapshotBuilder builder,
      final boolean tmp, final boolean removeOnExit, boolean setFolderTime) throws IOException {
    List<Path> files = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      for (Path filePath: builder.addRegion()) {
        files.add(filePath);
      }
    }

    // Finalize the snapshot
    builder.commit();

    if (setFolderTime) {
      fs.setTimes(snapshotDir, 0, -1);
    }

    // Make sure that all files are still present
    for (Path path: files) {
      assertFalse("Cache didn't find " + path, contains(getNonSnapshotFiles(cache, path), path));
    }

    FSUtils.logFileSystemState(fs, rootDir, LOG);
    if (removeOnExit) {
      LOG.debug("Deleting snapshot.");
      fs.delete(builder.getSnapshotsDir(), true);
      FSUtils.logFileSystemState(fs, rootDir, LOG);

      // then trigger a refresh
      cache.triggerCacheRefreshForTesting();
      // and not it shouldn't find those files
      for (Path filePath: files) {
        assertTrue("Cache found '" + filePath + "', but it shouldn't have.",
          contains(getNonSnapshotFiles(cache, filePath), filePath));

      }
    }
  }

  private static boolean contains(Iterable<FileStatus> files, Path filePath) {
    for (FileStatus status: files) {
      LOG.debug("debug in contains, 3.1: " + status.getPath() + " filePath:" + filePath);
      if (filePath.equals(status.getPath())) {
        return true;
      }
    }
    return false;
  }

  private static Iterable<FileStatus> getNonSnapshotFiles(SnapshotFileCache cache, Path storeFile)
      throws IOException {
    return cache.getUnreferencedFiles(
        Arrays.asList(FSUtils.listStatus(fs, storeFile.getParent())), null
    );
  }
}
