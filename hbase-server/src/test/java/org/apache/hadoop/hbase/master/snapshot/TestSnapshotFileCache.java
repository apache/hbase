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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.RegionInfo;
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

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

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
  private static FileSystem fs;
  private static Path rootDir;

  @BeforeClass
  public static void startCluster() throws Exception {
    UTIL.startMiniDFSCluster(1);
    fs = UTIL.getDFSCluster().getFileSystem();
    rootDir = UTIL.getDefaultRootDirPath();
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    UTIL.shutdownMiniDFSCluster();
  }

  @After
  public void cleanupFiles() throws Exception {
    // cleanup the snapshot directory
    Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
    fs.delete(snapshotDir, true);
  }

  @Test
  public void testLoadAndDelete() throws IOException {
    // don't refresh the cache unless we tell it to
    long period = Long.MAX_VALUE;
    SnapshotFileCache cache = new SnapshotFileCache(fs, rootDir, period, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());

    createAndTestSnapshotV1(cache, "snapshot1a", false, true);
    createAndTestSnapshotV1(cache, "snapshot1b", true, true);

    createAndTestSnapshotV2(cache, "snapshot2a", false, true);
    createAndTestSnapshotV2(cache, "snapshot2b", true, true);
  }

  @Test
  public void testReloadModifiedDirectory() throws IOException {
    // don't refresh the cache unless we tell it to
    long period = Long.MAX_VALUE;
    SnapshotFileCache cache = new SnapshotFileCache(fs, rootDir, period, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());

    createAndTestSnapshotV1(cache, "snapshot1", false, true);
    // now delete the snapshot and add a file with a different name
    createAndTestSnapshotV1(cache, "snapshot1", false, false);

    createAndTestSnapshotV2(cache, "snapshot2", false, true);
    // now delete the snapshot and add a file with a different name
    createAndTestSnapshotV2(cache, "snapshot2", false, false);
  }

  @Test
  public void testSnapshotTempDirReload() throws IOException {
    long period = Long.MAX_VALUE;
    // This doesn't refresh cache until we invoke it explicitly
    SnapshotFileCache cache = new SnapshotFileCache(fs, rootDir, period, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());

    // Add a new non-tmp snapshot
    createAndTestSnapshotV1(cache, "snapshot0v1", false, false);
    createAndTestSnapshotV1(cache, "snapshot0v2", false, false);

    // Add a new tmp snapshot
    createAndTestSnapshotV2(cache, "snapshot1", true, false);

    // Add another tmp snapshot
    createAndTestSnapshotV2(cache, "snapshot2", true, false);
  }

  @Test
  public void testWeNeverCacheTmpDirAndLoadIt() throws Exception {

    final AtomicInteger count = new AtomicInteger(0);
    // don't refresh the cache unless we tell it to
    long period = Long.MAX_VALUE;
    SnapshotFileCache cache = new SnapshotFileCache(fs, rootDir, period, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles()) {
      @Override
      List<String> getSnapshotsInProgress(final SnapshotManager snapshotManager)
              throws IOException {
        List<String> result = super.getSnapshotsInProgress(snapshotManager);
        count.incrementAndGet();
        return result;
      }

      @Override public void triggerCacheRefreshForTesting() {
        super.triggerCacheRefreshForTesting();
      }
    };

    SnapshotMock.SnapshotBuilder complete =
        createAndTestSnapshotV1(cache, "snapshot", false, false);

    int countBeforeCheck = count.get();

    FSUtils.logFileSystemState(fs, rootDir, LOG);

    List<FileStatus> allStoreFiles = getStoreFilesForSnapshot(complete);
    Iterable<FileStatus> deletableFiles = cache.getUnreferencedFiles(allStoreFiles, null);
    assertTrue(Iterables.isEmpty(deletableFiles));
    // no need for tmp dir check as all files are accounted for.
    assertEquals(0, count.get() - countBeforeCheck);


    // add a random file to make sure we refresh
    FileStatus randomFile = mockStoreFile(UTIL.getRandomUUID().toString());
    allStoreFiles.add(randomFile);
    deletableFiles = cache.getUnreferencedFiles(allStoreFiles, null);
    assertEquals(randomFile, Iterables.getOnlyElement(deletableFiles));
    assertEquals(1, count.get() - countBeforeCheck); // we check the tmp directory
  }

  private List<FileStatus> getStoreFilesForSnapshot(SnapshotMock.SnapshotBuilder builder)
      throws IOException {
    final List<FileStatus> allStoreFiles = Lists.newArrayList();
    SnapshotReferenceUtil
        .visitReferencedFiles(UTIL.getConfiguration(), fs, builder.getSnapshotsDir(),
            new SnapshotReferenceUtil.SnapshotVisitor() {
              @Override public void storeFile(RegionInfo regionInfo, String familyName,
                  SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile) throws IOException {
                FileStatus status = mockStoreFile(storeFile.getName());
                allStoreFiles.add(status);
              }
            });
    return allStoreFiles;
  }

  private FileStatus mockStoreFile(String storeFileName) {
    FileStatus status = mock(FileStatus.class);
    Path path = mock(Path.class);
    when(path.getName()).thenReturn(storeFileName);
    when(status.getPath()).thenReturn(path);
    return status;
  }

  class SnapshotFiles implements SnapshotFileCache.SnapshotFileInspector {
    @Override
    public Collection<String> filesUnderSnapshot(final Path snapshotDir) throws IOException {
      Collection<String> files =  new HashSet<>();
      files.addAll(SnapshotReferenceUtil.getHFileNames(UTIL.getConfiguration(), fs, snapshotDir));
      return files;
    }
  }

  private SnapshotMock.SnapshotBuilder createAndTestSnapshotV1(final SnapshotFileCache cache,
      final String name, final boolean tmp, final boolean removeOnExit) throws IOException {
    SnapshotMock snapshotMock = new SnapshotMock(UTIL.getConfiguration(), fs, rootDir);
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV1(name, name);
    createAndTestSnapshot(cache, builder, tmp, removeOnExit);
    return builder;
  }

  private void createAndTestSnapshotV2(final SnapshotFileCache cache, final String name,
      final boolean tmp, final boolean removeOnExit) throws IOException {
    SnapshotMock snapshotMock = new SnapshotMock(UTIL.getConfiguration(), fs, rootDir);
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2(name, name);
    createAndTestSnapshot(cache, builder, tmp, removeOnExit);
  }

  private void createAndTestSnapshot(final SnapshotFileCache cache,
      final SnapshotMock.SnapshotBuilder builder,
      final boolean tmp, final boolean removeOnExit) throws IOException {
    List<Path> files = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      for (Path filePath: builder.addRegion()) {
        if (tmp) {
          // We should be able to find all the files while the snapshot creation is in-progress
          FSUtils.logFileSystemState(fs, rootDir, LOG);
          assertFalse("Cache didn't find " + filePath,
            contains(getNonSnapshotFiles(cache, filePath), filePath));
        }
        files.add(filePath);
      }
    }

    // Finalize the snapshot
    if (!tmp) {
      builder.commit();
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
