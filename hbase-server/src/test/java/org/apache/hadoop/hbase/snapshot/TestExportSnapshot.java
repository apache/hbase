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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test Export Snapshot Tool
 */
@Category(MediumTests.class)
public class TestExportSnapshot {
  private final Log LOG = LogFactory.getLog(getClass());

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] emptySnapshotName;
  private byte[] snapshotName;
  private TableName tableName;
  private HBaseAdmin admin;

  public static void setUpBaseConf(Configuration conf) {
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    conf.setInt("mapreduce.map.max.attempts", 10);
    conf.setInt("mapred.map.max.attempts", 10);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpBaseConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
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
    this.admin = TEST_UTIL.getHBaseAdmin();

    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf("testtb-" + tid);
    snapshotName = Bytes.toBytes("snaptb0-" + tid);
    emptySnapshotName = Bytes.toBytes("emptySnaptb0-" + tid);

    // create Table
    SnapshotTestingUtils.createTable(TEST_UTIL, tableName, FAMILY);

    // Take an empty snapshot
    admin.snapshot(emptySnapshotName, tableName);

    // Add some rows
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);

    // take a snapshot
    admin.snapshot(snapshotName, tableName);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(tableName);
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
  }


  /**
   * Verfy the result of getBalanceSplits() method.
   * The result are groups of files, used as input list for the "export" mappers.
   * All the groups should have similar amount of data.
   *
   * The input list is a pair of file path and length.
   * The getBalanceSplits() function sort it by length,
   * and assign to each group a file, going back and forth through the groups.
   */
  @Test
  public void testBalanceSplit() throws Exception {
    // Create a list of files
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<Pair<SnapshotFileInfo, Long>>();
    for (long i = 0; i <= 20; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
        .setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i)
        .build();
      files.add(new Pair<SnapshotFileInfo, Long>(fileInfo, i));
    }

    // Create 5 groups (total size 210)
    //    group 0: 20, 11, 10,  1 (total size: 42)
    //    group 1: 19, 12,  9,  2 (total size: 42)
    //    group 2: 18, 13,  8,  3 (total size: 42)
    //    group 3: 17, 12,  7,  4 (total size: 42)
    //    group 4: 16, 11,  6,  5 (total size: 42)
    List<List<Pair<SnapshotFileInfo, Long>>> splits = ExportSnapshot.getBalancedSplits(files, 5);
    assertEquals(5, splits.size());

    String[] split0 = new String[] {"file-20", "file-11", "file-10", "file-1", "file-0"};
    verifyBalanceSplit(splits.get(0), split0, 42);
    String[] split1 = new String[] {"file-19", "file-12", "file-9",  "file-2"};
    verifyBalanceSplit(splits.get(1), split1, 42);
    String[] split2 = new String[] {"file-18", "file-13", "file-8",  "file-3"};
    verifyBalanceSplit(splits.get(2), split2, 42);
    String[] split3 = new String[] {"file-17", "file-14", "file-7",  "file-4"};
    verifyBalanceSplit(splits.get(3), split3, 42);
    String[] split4 = new String[] {"file-16", "file-15", "file-6",  "file-5"};
    verifyBalanceSplit(splits.get(4), split4, 42);
  }

  private void verifyBalanceSplit(final List<Pair<SnapshotFileInfo, Long>> split,
      final String[] expected, final long expectedSize) {
    assertEquals(expected.length, split.size());
    long totalSize = 0;
    for (int i = 0; i < expected.length; ++i) {
      Pair<SnapshotFileInfo, Long> fileInfo = split.get(i);
      assertEquals(expected[i], fileInfo.getFirst().getHfile());
      totalSize += fileInfo.getSecond();
    }
    assertEquals(expectedSize, totalSize);
  }

  /**
   * Verify if exported snapshot and copied files matches the original one.
   */
  @Test
  public void testExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, snapshotName, snapshotName, 2);
  }

  @Test
  public void testExportFileSystemStateWithSkipTmp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ExportSnapshot.CONF_SKIP_TMP, true);
    testExportFileSystemState(tableName, snapshotName, snapshotName, 2);
  }

  @Test
  public void testEmptyExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, emptySnapshotName, emptySnapshotName, 1);
  }

  @Test
  public void testConsecutiveExports() throws Exception {
    Path copyDir = getLocalDestinationDir();
    testExportFileSystemState(tableName, snapshotName, snapshotName, 2, copyDir, false);
    testExportFileSystemState(tableName, snapshotName, snapshotName, 2, copyDir, true);
    removeExportDir(copyDir);
  }

  @Test
  public void testExportWithTargetName() throws Exception {
    final byte[] targetName = Bytes.toBytes("testExportWithTargetName");
    testExportFileSystemState(tableName, snapshotName, targetName, 2);
  }

  /**
   * Mock a snapshot with files in the archive dir,
   * two regions, and one reference file.
   */
  @Test
  public void testSnapshotWithRefsExportFileSystemState() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    final TableName tableWithRefsName =
        TableName.valueOf("tableWithRefs");
    final String snapshotName = "tableWithRefs";
    final String TEST_FAMILY = Bytes.toString(FAMILY);
    final String TEST_HFILE = "abc";

    final SnapshotDescription sd = SnapshotDescription.newBuilder()
        .setName(snapshotName)
        .setTable(tableWithRefsName.getNameAsString()).build();

    FileSystem fs = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    Path archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);

    // First region, simple with one plain hfile.
    HRegionInfo hri = new HRegionInfo(tableWithRefsName);
    HRegionFileSystem r0fs = HRegionFileSystem.createRegionOnFileSystem(conf,
      fs, FSUtils.getTableDir(archiveDir, hri.getTable()), hri);
    Path storeFile = new Path(rootDir, TEST_HFILE);
    FSDataOutputStream out = fs.create(storeFile);
    out.write(Bytes.toBytes("Test Data"));
    out.close();
    r0fs.commitStoreFile(TEST_FAMILY, storeFile);

    // Second region, used to test the split case.
    // This region contains a reference to the hfile in the first region.
    hri = new HRegionInfo(tableWithRefsName);
    HRegionFileSystem r1fs = HRegionFileSystem.createRegionOnFileSystem(conf,
      fs, new Path(archiveDir, hri.getTable().getNameAsString()), hri);
    storeFile = new Path(rootDir, TEST_HFILE + '.' + r0fs.getRegionInfo().getEncodedName());
    out = fs.create(storeFile);
    out.write(Bytes.toBytes("Test Data"));
    out.close();
    r1fs.commitStoreFile(TEST_FAMILY, storeFile);

    Path tableDir = FSUtils.getTableDir(archiveDir, tableWithRefsName);
    HTableDescriptor htd = new HTableDescriptor(tableWithRefsName);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    new FSTableDescriptors(conf, fs, rootDir)
        .createTableDescriptorForTableDirectory(tableDir, htd, false);

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    FileUtil.copy(fs, tableDir, fs, snapshotDir, false, conf);
    SnapshotDescriptionUtils.writeSnapshotInfo(sd, snapshotDir, fs);

    byte[] name = Bytes.toBytes(snapshotName);
    testExportFileSystemState(tableWithRefsName, name, name, 2);
  }

  private void testExportFileSystemState(final TableName tableName, final byte[] snapshotName,
      final byte[] targetName, int filesExpected) throws Exception {
    Path copyDir = getHdfsDestinationDir();
    testExportFileSystemState(tableName, snapshotName, targetName, filesExpected, copyDir, false);
    removeExportDir(copyDir);
  }

  /**
   * Test ExportSnapshot
   */
  private void testExportFileSystemState(final TableName tableName, final byte[] snapshotName,
      final byte[] targetName, int filesExpected, Path copyDir, boolean overwrite)
      throws Exception {
    URI hdfsUri = FileSystem.get(TEST_UTIL.getConfiguration()).getUri();
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
    int res = ExportSnapshot.innerMain(TEST_UTIL.getConfiguration(),
        opts.toArray(new String[opts.size()]));
    assertEquals(0, res);

    // Verify File-System state
    FileStatus[] rootFiles = fs.listStatus(copyDir);
    assertEquals(filesExpected, rootFiles.length);
    for (FileStatus fileStatus: rootFiles) {
      String name = fileStatus.getPath().getName();
      assertTrue(fileStatus.isDir());
      assertTrue(name.equals(HConstants.SNAPSHOT_DIR_NAME) ||
                 name.equals(HConstants.HFILE_ARCHIVE_DIRECTORY));
    }

    // compare the snapshot metadata and verify the hfiles
    final FileSystem hdfs = FileSystem.get(hdfsUri, TEST_UTIL.getConfiguration());
    final Path snapshotDir = new Path(HConstants.SNAPSHOT_DIR_NAME, Bytes.toString(snapshotName));
    final Path targetDir = new Path(HConstants.SNAPSHOT_DIR_NAME, Bytes.toString(targetName));
    verifySnapshot(hdfs, new Path(TEST_UTIL.getDefaultRootDirPath(), snapshotDir),
        fs, new Path(copyDir, targetDir));
    verifyArchive(fs, copyDir, tableName, Bytes.toString(targetName));
    FSUtils.logFileSystemState(hdfs, snapshotDir, LOG);
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
  private void verifySnapshot(final FileSystem fs1, final Path root1,
      final FileSystem fs2, final Path root2) throws IOException {
    Set<String> s = new HashSet<String>();
    assertEquals(listFiles(fs1, root1, root1), listFiles(fs2, root2, root2));
  }

  /*
   * Verify if the files exists
   */
  private void verifyArchive(final FileSystem fs, final Path rootDir,
      final TableName tableName, final String snapshotName) throws IOException {
    final Path exportedSnapshot = new Path(rootDir,
      new Path(HConstants.SNAPSHOT_DIR_NAME, snapshotName));
    final Path exportedArchive = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    LOG.debug(listFiles(fs, exportedArchive, exportedArchive));
    SnapshotReferenceUtil.visitReferencedFiles(TEST_UTIL.getConfiguration(), fs, exportedSnapshot,
          new SnapshotReferenceUtil.SnapshotVisitor() {
        public void storeFile(final HRegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          String hfile = storeFile.getName();
          verifyNonEmptyFile(new Path(exportedArchive,
            new Path(FSUtils.getTableDir(new Path("./"), tableName),
                new Path(regionInfo.getEncodedName(), new Path(family, hfile)))));
        }

        public void recoveredEdits (final String region, final String logfile)
            throws IOException {
          verifyNonEmptyFile(new Path(exportedSnapshot,
            new Path(tableName.getNameAsString(), new Path(region, logfile))));
        }

        public void logFile (final String server, final String logfile)
            throws IOException {
          verifyNonEmptyFile(new Path(exportedSnapshot, new Path(server, logfile)));
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
  }

  private Set<String> listFiles(final FileSystem fs, final Path root, final Path dir)
      throws IOException {
    Set<String> files = new HashSet<String>();
    int rootPrefix = root.toString().length();
    FileStatus[] list = FSUtils.listStatus(fs, dir);
    if (list != null) {
      for (FileStatus fstat: list) {
        LOG.debug(fstat.getPath());
        if (fstat.isDir()) {
          files.addAll(listFiles(fs, root, fstat.getPath()));
        } else {
          files.add(fstat.getPath().toString().substring(rootPrefix));
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

  private void removeExportDir(final Path path) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    FSUtils.logFileSystemState(fs, path, LOG);
    fs.delete(path, true);
  }
}
