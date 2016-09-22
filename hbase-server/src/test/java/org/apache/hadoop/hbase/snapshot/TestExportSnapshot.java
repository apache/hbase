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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

/**
 * Test Export Snapshot Tool
 */
@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestExportSnapshot {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).withLookingForStuckThread(true).build();
  private static final Log LOG = LogFactory.getLog(TestExportSnapshot.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected final static byte[] FAMILY = Bytes.toBytes("cf");

  protected TableName tableName;
  private byte[] emptySnapshotName;
  private byte[] snapshotName;
  private int tableNumFiles;
  private Admin admin;

  public static void setUpBaseConf(Configuration conf) {
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    conf.setInt("mapreduce.map.maxattempts", 10);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpBaseConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Create a table and take a snapshot of the table used by the export test.
   */
  @Before
  public void setUp() throws Exception {
    this.admin = TEST_UTIL.getHBaseAdmin();

    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf("testtb-" + tid);
    snapshotName = Bytes.toBytes("snaptb0-" + tid);
    emptySnapshotName = Bytes.toBytes("emptySnaptb0-" + tid);

    // create Table
    createTable();

    // Take an empty snapshot
    admin.snapshot(emptySnapshotName, tableName);

    // Add some rows
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 50, FAMILY);
    tableNumFiles = admin.getTableRegions(tableName).size();

    // take a snapshot
    admin.snapshot(snapshotName, tableName);
  }

  protected void createTable() throws Exception {
    SnapshotTestingUtils.createPreSplitTable(TEST_UTIL, tableName, 2, FAMILY);
  }

  protected interface RegionPredicate {
    boolean evaluate(final HRegionInfo regionInfo);
  }

  protected RegionPredicate getBypassRegionPredicate() {
    return null;
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(tableName);
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
  }

  /**
   * Verify if exported snapshot and copied files matches the original one.
   */
  @Test
  public void testExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles);
  }

  @Test
  public void testExportFileSystemStateWithSkipTmp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ExportSnapshot.CONF_SKIP_TMP, true);
    try {
      testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles);
    } finally {
      TEST_UTIL.getConfiguration().setBoolean(ExportSnapshot.CONF_SKIP_TMP, false);
    }
  }

  @Test
  public void testEmptyExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, emptySnapshotName, emptySnapshotName, 0);
  }

  @Test
  public void testConsecutiveExports() throws Exception {
    Path copyDir = getLocalDestinationDir();
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles, copyDir, false);
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles, copyDir, true);
    removeExportDir(copyDir);
  }

  @Test
  public void testExportWithTargetName() throws Exception {
    final byte[] targetName = Bytes.toBytes("testExportWithTargetName");
    testExportFileSystemState(tableName, snapshotName, targetName, tableNumFiles);
  }

  private void testExportFileSystemState(final TableName tableName, final byte[] snapshotName,
      final byte[] targetName, int filesExpected) throws Exception {
    testExportFileSystemState(tableName, snapshotName, targetName,
      filesExpected, getHdfsDestinationDir(), false);
  }

  protected void testExportFileSystemState(final TableName tableName,
      final byte[] snapshotName, final byte[] targetName, int filesExpected,
      Path copyDir, boolean overwrite) throws Exception {
    testExportFileSystemState(TEST_UTIL.getConfiguration(), tableName, snapshotName, targetName,
      filesExpected, TEST_UTIL.getDefaultRootDirPath(), copyDir,
      overwrite, getBypassRegionPredicate());
  }

  /**
   * Test ExportSnapshot
   */
  protected static void testExportFileSystemState(final Configuration conf, final TableName tableName,
      final byte[] snapshotName, final byte[] targetName, final int filesExpected,
      final Path sourceDir, Path copyDir, final boolean overwrite,
      final RegionPredicate bypassregionPredicate) throws Exception {
    URI hdfsUri = FileSystem.get(conf).getUri();
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs);

    List<String> opts = new ArrayList<String>();
    opts.add("-snapshot");
    opts.add(Bytes.toString(snapshotName));
    opts.add("-copy-to");
    opts.add(copyDir.toString());
    if (targetName != snapshotName) {
      opts.add("-target");
      opts.add(Bytes.toString(targetName));
    }
    if (overwrite) opts.add("-overwrite");

    // Export Snapshot
    int res = ExportSnapshot.innerMain(conf, opts.toArray(new String[opts.size()]));
    assertEquals(0, res);

    // Verify File-System state
    FileStatus[] rootFiles = fs.listStatus(copyDir);
    assertEquals(filesExpected > 0 ? 2 : 1, rootFiles.length);
    for (FileStatus fileStatus: rootFiles) {
      String name = fileStatus.getPath().getName();
      assertTrue(fileStatus.isDirectory());
      assertTrue(name.equals(HConstants.SNAPSHOT_DIR_NAME) ||
                 name.equals(HConstants.HFILE_ARCHIVE_DIRECTORY));
    }

    // compare the snapshot metadata and verify the hfiles
    final FileSystem hdfs = FileSystem.get(hdfsUri, conf);
    final Path snapshotDir = new Path(HConstants.SNAPSHOT_DIR_NAME, Bytes.toString(snapshotName));
    final Path targetDir = new Path(HConstants.SNAPSHOT_DIR_NAME, Bytes.toString(targetName));
    verifySnapshotDir(hdfs, new Path(sourceDir, snapshotDir),
        fs, new Path(copyDir, targetDir));
    Set<String> snapshotFiles = verifySnapshot(conf, fs, copyDir, tableName,
      Bytes.toString(targetName), bypassregionPredicate);
    assertEquals(filesExpected, snapshotFiles.size());
  }

  /**
   * Check that ExportSnapshot will return a failure if something fails.
   */
  @Test
  public void testExportFailure() throws Exception {
    assertEquals(1, runExportAndInjectFailures(snapshotName, false));
  }

  /**
   * Check that ExportSnapshot will succede if something fails but the retry succede.
   */
  @Test
  public void testExportRetry() throws Exception {
    assertEquals(0, runExportAndInjectFailures(snapshotName, true));
  }

  /*
   * Execute the ExportSnapshot job injecting failures
   */
  private int runExportAndInjectFailures(final byte[] snapshotName, boolean retry)
      throws Exception {
    Path copyDir = getLocalDestinationDir();
    URI hdfsUri = FileSystem.get(TEST_UTIL.getConfiguration()).getUri();
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs);

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(ExportSnapshot.CONF_TEST_FAILURE, true);
    conf.setBoolean(ExportSnapshot.CONF_TEST_RETRY, retry);
    if (!retry) {
      conf.setInt("mapreduce.map.maxattempts", 3);
    }
    // Export Snapshot
    Path sourceDir = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    int res = ExportSnapshot.innerMain(conf, new String[] {
      "-snapshot", Bytes.toString(snapshotName),
      "-copy-from", sourceDir.toString(),
      "-copy-to", copyDir.toString()
    });
    return res;
  }

  /*
   * verify if the snapshot folder on file-system 1 match the one on file-system 2
   */
  protected static void verifySnapshotDir(final FileSystem fs1, final Path root1,
      final FileSystem fs2, final Path root2) throws IOException {
    assertEquals(listFiles(fs1, root1, root1), listFiles(fs2, root2, root2));
  }

  protected Set<String> verifySnapshot(final FileSystem fs, final Path rootDir,
      final TableName tableName, final String snapshotName) throws IOException {
    return verifySnapshot(TEST_UTIL.getConfiguration(), fs, rootDir, tableName,
      snapshotName, getBypassRegionPredicate());
  }

  /*
   * Verify if the files exists
   */
  protected static Set<String> verifySnapshot(final Configuration conf, final FileSystem fs,
      final Path rootDir, final TableName tableName, final String snapshotName,
      final RegionPredicate bypassregionPredicate) throws IOException {
    final Path exportedSnapshot = new Path(rootDir,
      new Path(HConstants.SNAPSHOT_DIR_NAME, snapshotName));
    final Set<String> snapshotFiles = new HashSet<String>();
    final Path exportedArchive = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    SnapshotReferenceUtil.visitReferencedFiles(conf, fs, exportedSnapshot,
          new SnapshotReferenceUtil.SnapshotVisitor() {
        @Override
        public void storeFile(final HRegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          if (bypassregionPredicate != null && bypassregionPredicate.evaluate(regionInfo))
            return;

          String hfile = storeFile.getName();
          snapshotFiles.add(hfile);
          if (storeFile.hasReference()) {
            // Nothing to do here, we have already the reference embedded
          } else {
            verifyNonEmptyFile(new Path(exportedArchive,
              new Path(FSUtils.getTableDir(new Path("./"), tableName),
                  new Path(regionInfo.getEncodedName(), new Path(family, hfile)))));
          }
        }

        private void verifyNonEmptyFile(final Path path) throws IOException {
          assertTrue(path + " should exists", fs.exists(path));
          assertTrue(path + " should not be empty", fs.getFileStatus(path).getLen() > 0);
        }
    });

    // Verify Snapshot description
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, exportedSnapshot);
    assertTrue(desc.getName().equals(snapshotName));
    assertTrue(desc.getTable().equals(tableName.getNameAsString()));
    return snapshotFiles;
  }

  private static Set<String> listFiles(final FileSystem fs, final Path root, final Path dir)
      throws IOException {
    Set<String> files = new HashSet<String>();
    int rootPrefix = root.makeQualified(fs).toString().length();
    FileStatus[] list = FSUtils.listStatus(fs, dir);
    if (list != null) {
      for (FileStatus fstat: list) {
        LOG.debug(fstat.getPath());
        if (fstat.isDirectory()) {
          files.addAll(listFiles(fs, root, fstat.getPath()));
        } else {
          files.add(fstat.getPath().makeQualified(fs).toString().substring(rootPrefix));
        }
      }
    }
    return files;
  }

  private Path getHdfsDestinationDir() {
    Path rootDir = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    Path path = new Path(new Path(rootDir, "export-test"), "export-" + System.currentTimeMillis());
    LOG.info("HDFS export destination path: " + path);
    return path;
  }

  private Path getLocalDestinationDir() {
    Path path = TEST_UTIL.getDataTestDir("local-export-" + System.currentTimeMillis());
    LOG.info("Local export destination path: " + path);
    return path;
  }

  private static void removeExportDir(final Path path) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    fs.delete(path, true);
  }
}
