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
package org.apache.hadoop.hbase.snapshot;

import static org.apache.hadoop.util.ToolRunner.run;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test Export Snapshot Tool
 */
@Ignore // HBASE-24493
@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestExportSnapshot {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExportSnapshot.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestExportSnapshot.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected final static byte[] FAMILY = Bytes.toBytes("cf");

  @Rule
  public final TestName testName = new TestName();

  protected TableName tableName;
  private byte[] emptySnapshotName;
  private byte[] snapshotName;
  private int tableNumFiles;
  private Admin admin;

  public static void setUpBaseConf(Configuration conf) {
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    // If a single node has enough failures (default 3), resource manager will blacklist it.
    // With only 2 nodes and tests injecting faults, we don't want that.
    conf.setInt("mapreduce.job.maxtaskfailures.per.tracker", 100);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpBaseConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Create a table and take a snapshot of the table used by the export test.
   */
  @Before
  public void setUp() throws Exception {
    this.admin = TEST_UTIL.getAdmin();

    tableName = TableName.valueOf("testtb-" + testName.getMethodName());
    snapshotName = Bytes.toBytes("snaptb0-" + testName.getMethodName());
    emptySnapshotName = Bytes.toBytes("emptySnaptb0-" + testName.getMethodName());

    // create Table
    createTable(this.tableName);

    // Take an empty snapshot
    admin.snapshot(emptySnapshotName, tableName);

    // Add some rows
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 50, FAMILY);
    tableNumFiles = admin.getTableRegions(tableName).size();

    // take a snapshot
    admin.snapshot(snapshotName, tableName);
  }

  protected void createTable(TableName tableName) throws Exception {
    SnapshotTestingUtils.createPreSplitTable(TEST_UTIL, tableName, 2, FAMILY);
  }

  protected interface RegionPredicate {
    boolean evaluate(final RegionInfo regionInfo);
  }

  protected RegionPredicate getBypassRegionPredicate() {
    return null;
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(tableName);
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getAdmin());
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
  public void testExportFileSystemStateWithMergeRegion() throws Exception {
    // disable compaction
    admin.compactionSwitch(false,
      admin.getRegionServers().stream().map(a -> a.getServerName()).collect(Collectors.toList()));
    // create Table
    TableName tableName0 = TableName.valueOf("testtb-" + testName.getMethodName() + "-1");
    byte[] snapshotName0 = Bytes.toBytes("snaptb0-" + testName.getMethodName() + "-1");
    admin.createTable(
      TableDescriptorBuilder.newBuilder(tableName0)
          .setColumnFamilies(
            Lists.newArrayList(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build()))
          .build(),
      new byte[][] { Bytes.toBytes("2") });
    // put some data
    try (Table table = admin.getConnection().getTable(tableName0)) {
      table.put(new Put(Bytes.toBytes("1")).addColumn(FAMILY, null, Bytes.toBytes("1")));
      table.put(new Put(Bytes.toBytes("2")).addColumn(FAMILY, null, Bytes.toBytes("2")));
    }
    List<RegionInfo> regions = admin.getRegions(tableName0);
    assertEquals(2, regions.size());
    tableNumFiles = regions.size();
    // merge region
    admin.mergeRegionsAsync(new byte[][] { regions.get(0).getEncodedNameAsBytes(),
        regions.get(1).getEncodedNameAsBytes() },
      true).get();
    // take a snapshot
    admin.snapshot(snapshotName0, tableName0);
    // export snapshot and verify
    testExportFileSystemState(tableName0, snapshotName0, snapshotName0, tableNumFiles);
    // delete table
    TEST_UTIL.deleteTable(tableName0);
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
    Path copyDir = getLocalDestinationDir(TEST_UTIL);
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
      overwrite, getBypassRegionPredicate(), true);
  }

  /**
   * Creates destination directory, runs ExportSnapshot() tool, and runs some verifications.
   */
  protected static void testExportFileSystemState(final Configuration conf,
      final TableName tableName,
      final byte[] snapshotName, final byte[] targetName, final int filesExpected,
      final Path srcDir, Path rawTgtDir, final boolean overwrite,
      final RegionPredicate bypassregionPredicate, boolean success) throws Exception {
    FileSystem tgtFs = rawTgtDir.getFileSystem(conf);
    FileSystem srcFs = srcDir.getFileSystem(conf);
    Path tgtDir = rawTgtDir.makeQualified(tgtFs.getUri(), tgtFs.getWorkingDirectory());
    LOG.info("tgtFsUri={}, tgtDir={}, rawTgtDir={}, srcFsUri={}, srcDir={}",
      tgtFs.getUri(), tgtDir, rawTgtDir, srcFs.getUri(), srcDir);
    List<String> opts = new ArrayList<>();
    opts.add("--snapshot");
    opts.add(Bytes.toString(snapshotName));
    opts.add("--copy-to");
    opts.add(tgtDir.toString());
    if (targetName != snapshotName) {
      opts.add("--target");
      opts.add(Bytes.toString(targetName));
    }
    if (overwrite) {
      opts.add("--overwrite");
    }

    // Export Snapshot
    int res = run(conf, new ExportSnapshot(), opts.toArray(new String[opts.size()]));
    assertEquals("success " + success + ", res=" + res, success ? 0 : 1, res);
    if (!success) {
      final Path targetDir = new Path(HConstants.SNAPSHOT_DIR_NAME, Bytes.toString(targetName));
      assertFalse(tgtDir.toString() + " " + targetDir.toString(),
        tgtFs.exists(new Path(tgtDir, targetDir)));
      return;
    }
    LOG.info("Exported snapshot");

    // Verify File-System state
    FileStatus[] rootFiles = tgtFs.listStatus(tgtDir);
    assertEquals(filesExpected > 0 ? 2 : 1, rootFiles.length);
    for (FileStatus fileStatus: rootFiles) {
      String name = fileStatus.getPath().getName();
      assertTrue(fileStatus.toString(), fileStatus.isDirectory());
      assertTrue(name.toString(), name.equals(HConstants.SNAPSHOT_DIR_NAME) ||
        name.equals(HConstants.HFILE_ARCHIVE_DIRECTORY));
    }
    LOG.info("Verified filesystem state");

    // Compare the snapshot metadata and verify the hfiles
    final Path snapshotDir = new Path(HConstants.SNAPSHOT_DIR_NAME, Bytes.toString(snapshotName));
    final Path targetDir = new Path(HConstants.SNAPSHOT_DIR_NAME, Bytes.toString(targetName));
    verifySnapshotDir(srcFs, new Path(srcDir, snapshotDir), tgtFs, new Path(tgtDir, targetDir));
    Set<String> snapshotFiles = verifySnapshot(conf, tgtFs, tgtDir, tableName,
      Bytes.toString(targetName), bypassregionPredicate);
    assertEquals(filesExpected, snapshotFiles.size());
  }

  /*
   * verify if the snapshot folder on file-system 1 match the one on file-system 2
   */
  protected static void verifySnapshotDir(final FileSystem fs1, final Path root1,
      final FileSystem fs2, final Path root2) throws IOException {
    assertEquals(listFiles(fs1, root1, root1), listFiles(fs2, root2, root2));
  }

  /*
   * Verify if the files exists
   */
  protected static Set<String> verifySnapshot(final Configuration conf, final FileSystem fs,
      final Path rootDir, final TableName tableName, final String snapshotName,
      final RegionPredicate bypassregionPredicate) throws IOException {
    final Path exportedSnapshot = new Path(rootDir,
      new Path(HConstants.SNAPSHOT_DIR_NAME, snapshotName));
    final Set<String> snapshotFiles = new HashSet<>();
    final Path exportedArchive = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    SnapshotReferenceUtil.visitReferencedFiles(conf, fs, exportedSnapshot,
          new SnapshotReferenceUtil.SnapshotVisitor() {
        @Override
        public void storeFile(final RegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          if (bypassregionPredicate != null && bypassregionPredicate.evaluate(regionInfo)) {
            return;
          }

          if (!storeFile.hasReference() && !StoreFileInfo.isReference(storeFile.getName())) {
            String hfile = storeFile.getName();
            snapshotFiles.add(hfile);
            verifyNonEmptyFile(new Path(exportedArchive,
              new Path(CommonFSUtils.getTableDir(new Path("./"), tableName),
                  new Path(regionInfo.getEncodedName(), new Path(family, hfile)))));
          } else {
            Pair<String, String> referredToRegionAndFile =
                StoreFileInfo.getReferredToRegionAndFile(storeFile.getName());
            String region = referredToRegionAndFile.getFirst();
            String hfile = referredToRegionAndFile.getSecond();
            snapshotFiles.add(hfile);
            verifyNonEmptyFile(new Path(exportedArchive,
              new Path(CommonFSUtils.getTableDir(new Path("./"), tableName),
                new Path(region, new Path(family, hfile)))));
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
    Set<String> files = new HashSet<>();
    LOG.debug("List files in {} in root {} at {}", fs, root, dir);
    int rootPrefix = root.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString().length();
    FileStatus[] list = CommonFSUtils.listStatus(fs, dir);
    if (list != null) {
      for (FileStatus fstat: list) {
        LOG.debug(Objects.toString(fstat.getPath()));
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

  static Path getLocalDestinationDir(HBaseTestingUtility htu) {
    Path path = htu.getDataTestDir("local-export-" + System.currentTimeMillis());
    try {
      FileSystem fs = FileSystem.getLocal(htu.getConfiguration());
      LOG.info("Local export destination path: " + path);
      return path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private static void removeExportDir(final Path path) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    fs.delete(path, true);
  }
}
