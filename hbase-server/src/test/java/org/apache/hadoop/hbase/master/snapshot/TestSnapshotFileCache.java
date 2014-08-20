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
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that we correctly reload the cache, filter directories, etc.
 */
@Category(MediumTests.class)
public class TestSnapshotFileCache {

  private static final Log LOG = LogFactory.getLog(TestSnapshotFileCache.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static long sequenceId = 0;
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

  @Test(timeout = 10000000)
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
  public void testJustFindLogsDirectory() throws Exception {
    // don't refresh the cache unless we tell it to
    long period = Long.MAX_VALUE;
    Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
    SnapshotFileCache cache = new SnapshotFileCache(fs, rootDir, period, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFileCache.SnapshotFileInspector() {
            public Collection<String> filesUnderSnapshot(final Path snapshotDir)
                throws IOException {
              return SnapshotReferenceUtil.getHLogNames(fs, snapshotDir);
            }
        });

    // create a file in a 'completed' snapshot
    SnapshotDescription desc = SnapshotDescription.newBuilder().setName("snapshot").build();
    Path snapshot = SnapshotDescriptionUtils.getCompletedSnapshotDir(desc, rootDir);
    SnapshotDescriptionUtils.writeSnapshotInfo(desc, snapshot, fs);
    Path file1 = new Path(new Path(new Path(snapshot, "7e91021"), "fam"), "file1");
    fs.createNewFile(file1);

    // and another file in the logs directory
    Path logs = getSnapshotHLogsDir(snapshot, "server");
    Path log = new Path(logs, "me.hbase.com%2C58939%2C1350424310315.1350424315552");
    fs.createNewFile(log);

    FSUtils.logFileSystemState(fs, rootDir, LOG);

    // then make sure the cache only finds the log files
    assertFalse("Cache found '" + file1 + "', but it shouldn't have.",
      cache.contains(file1.getName()));
    assertTrue("Cache didn't find:" + log, cache.contains(log.getName()));
  }

  /**
   * Get the log directory for a specific snapshot
   * @param snapshotDir directory where the specific snapshot will be store
   * @param serverName name of the parent regionserver for the log files
   * @return path to the log home directory for the archive files.
   */
  public static Path getSnapshotHLogsDir(Path snapshotDir, String serverName) {
    return new Path(snapshotDir, HLogUtil.getHLogDirectoryName(serverName));
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

  class SnapshotFiles implements SnapshotFileCache.SnapshotFileInspector {
    public Collection<String> filesUnderSnapshot(final Path snapshotDir) throws IOException {
      Collection<String> files =  new HashSet<String>();
      files.addAll(SnapshotReferenceUtil.getHLogNames(fs, snapshotDir));
      files.addAll(SnapshotReferenceUtil.getHFileNames(UTIL.getConfiguration(), fs, snapshotDir));
      return files;
    }
  };

  private void createAndTestSnapshotV1(final SnapshotFileCache cache, final String name,
      final boolean tmp, final boolean removeOnExit) throws IOException {
    SnapshotMock snapshotMock = new SnapshotMock(UTIL.getConfiguration(), fs, rootDir);
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV1(name);
    createAndTestSnapshot(cache, builder, tmp, removeOnExit);
  }

  private void createAndTestSnapshotV2(final SnapshotFileCache cache, final String name,
      final boolean tmp, final boolean removeOnExit) throws IOException {
    SnapshotMock snapshotMock = new SnapshotMock(UTIL.getConfiguration(), fs, rootDir);
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2(name);
    createAndTestSnapshot(cache, builder, tmp, removeOnExit);
  }

  private void createAndTestSnapshot(final SnapshotFileCache cache,
      final SnapshotMock.SnapshotBuilder builder,
      final boolean tmp, final boolean removeOnExit) throws IOException {
    List<String> files = new ArrayList<String>();
    for (int i = 0; i < 3; ++i) {
      for (Path filePath: builder.addRegion()) {
        String fileName = filePath.getName();
        if (tmp) {
          // We should be able to find all the files while the snapshot creation is in-progress
          FSUtils.logFileSystemState(fs, rootDir, LOG);
          assertTrue("Cache didn't find " + fileName, cache.contains(fileName));
        }
        files.add(fileName);
      }
    }

    // Finalize the snapshot
    if (!tmp) {
      builder.commit();
    }

    // Make sure that all files are still present
    for (String fileName: files) {
      assertTrue("Cache didn't find " + fileName, cache.contains(fileName));
    }

    FSUtils.logFileSystemState(fs, rootDir, LOG);
    if (removeOnExit) {
      LOG.debug("Deleting snapshot.");
      fs.delete(builder.getSnapshotsDir(), true);
      FSUtils.logFileSystemState(fs, rootDir, LOG);

      // The files should be in cache until next refresh
      for (String fileName: files) {
        assertTrue("Cache didn't find " + fileName, cache.contains(fileName));
      }

      // then trigger a refresh
      cache.triggerCacheRefreshForTesting();
      // and not it shouldn't find those files
      for (String fileName: files) {
        assertFalse("Cache found '" + fileName + "', but it shouldn't have.",
                    cache.contains(fileName));
      }
    }
  }
}
