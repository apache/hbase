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
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test that the snapshot hfile cleaner finds hfiles referenced in a snapshot
 */
@Category({MasterTests.class, SmallTests.class})
public class TestSnapshotHFileCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotHFileCleaner.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLE_NAME_STR = "testSnapshotManifest";
  private static final String SNAPSHOT_NAME_STR = "testSnapshotManifest-snapshot";
  private static Path rootDir;
  private static FileSystem fs;
  private static Configuration conf;

  @Rule
  public TestName name = new TestName();

  /**
   * Setup the test environment
   */
  @BeforeClass
  public static void setup() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    rootDir = CommonFSUtils.getRootDir(conf);
    fs = FileSystem.get(conf);
  }


  @AfterClass
  public static void cleanup() throws IOException {
    // cleanup
    fs.delete(rootDir, true);
  }

  @Test
  public void testFindsSnapshotFilesWhenCleaning() throws IOException {
    CommonFSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path archivedHfileDir = new Path(TEST_UTIL.getDataTestDir(), HConstants.HFILE_ARCHIVE_DIRECTORY);

    FileSystem fs = FileSystem.get(conf);
    SnapshotHFileCleaner cleaner = new SnapshotHFileCleaner();
    cleaner.setConf(conf);

    // write an hfile to the snapshot directory
    String snapshotName = "snapshot";
    byte[] snapshot = Bytes.toBytes(snapshotName);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    HRegionInfo mockRegion = new HRegionInfo(tableName);
    Path regionSnapshotDir = new Path(snapshotDir, mockRegion.getEncodedName());
    Path familyDir = new Path(regionSnapshotDir, "family");
    // create a reference to a supposedly valid hfile
    String hfile = "fd1e73e8a96c486090c5cec07b4894c4";
    Path refFile = new Path(familyDir, hfile);

    // make sure the reference file exists
    fs.create(refFile);

    // create the hfile in the archive
    fs.mkdirs(archivedHfileDir);
    fs.createNewFile(new Path(archivedHfileDir, hfile));

    // make sure that the file isn't deletable
    assertFalse(cleaner.isFileDeletable(fs.getFileStatus(refFile)));
  }

  static class SnapshotFiles implements SnapshotFileCache.SnapshotFileInspector {
    @Override
    public Collection<String> filesUnderSnapshot(final FileSystem workingFs,
      final Path snapshotDir) throws IOException {
      Collection<String> files =  new HashSet<>();
      files.addAll(SnapshotReferenceUtil.getHFileNames(conf, workingFs, snapshotDir));
      return files;
    }
  }

  /**
   * If there is a corrupted region manifest, it should throw out CorruptedSnapshotException,
   * instead of an IOException
   */
  @Test
  public void testCorruptedRegionManifest() throws IOException {
    SnapshotTestingUtils.SnapshotMock
        snapshotMock = new SnapshotTestingUtils.SnapshotMock(conf, fs, rootDir);
    SnapshotTestingUtils.SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2(
        SNAPSHOT_NAME_STR, TABLE_NAME_STR);
    builder.addRegionV2();
    builder.corruptOneRegionManifest();

    long period = Long.MAX_VALUE;
    SnapshotFileCache cache = new SnapshotFileCache(conf, period, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());
    try {
      cache.getSnapshotsInProgress();
    } finally {
      fs.delete(SnapshotDescriptionUtils.getWorkingSnapshotDir(rootDir, conf), true);
    }
  }

  /**
   * If there is a corrupted data manifest, it should throw out CorruptedSnapshotException,
   * instead of an IOException
   */
  @Test
  public void testCorruptedDataManifest() throws IOException {
    SnapshotTestingUtils.SnapshotMock
        snapshotMock = new SnapshotTestingUtils.SnapshotMock(conf, fs, rootDir);
    SnapshotTestingUtils.SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2(
        SNAPSHOT_NAME_STR, TABLE_NAME_STR);
    builder.addRegionV2();
    // consolidate to generate a data.manifest file
    builder.consolidate();
    builder.corruptDataManifest();

    long period = Long.MAX_VALUE;
    SnapshotFileCache cache = new SnapshotFileCache(conf, period, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());
    try {
      cache.getSnapshotsInProgress();
    } finally {
      fs.delete(SnapshotDescriptionUtils.getWorkingSnapshotDir(rootDir,
          TEST_UTIL.getConfiguration()), true);
    }
  }

  @Test
  public void testMissedTmpSnapshot() throws IOException {
    SnapshotTestingUtils.SnapshotMock snapshotMock =
        new SnapshotTestingUtils.SnapshotMock(conf, fs, rootDir);
    SnapshotTestingUtils.SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2(
        SNAPSHOT_NAME_STR, TABLE_NAME_STR);
    builder.addRegionV2();
    builder.missOneRegionSnapshotFile();
    long period = Long.MAX_VALUE;
    SnapshotFileCache cache = new SnapshotFileCache(conf, period, 10000000,
        "test-snapshot-file-cache-refresh", new SnapshotFiles());
    cache.getSnapshotsInProgress();
    assertTrue(fs.exists(builder.getSnapshotsDir()));
  }
}
