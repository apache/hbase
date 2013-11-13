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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.mapreduce.Job;
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

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] emptySnapshotName;
  private byte[] snapshotName;
  private byte[] tableName;
  private HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
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
    tableName = Bytes.toBytes("testtb-" + tid);
    snapshotName = Bytes.toBytes("snaptb0-" + tid);
    emptySnapshotName = Bytes.toBytes("emptySnaptb0-" + tid);

    // create Table
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(htd, null);

    // Take an empty snapshot
    admin.snapshot(emptySnapshotName, tableName);

    // Add some rows
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    TEST_UTIL.loadTable(table, FAMILY);

    // take a snapshot
    admin.snapshot(snapshotName, tableName);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(tableName);
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
    admin.close();
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
    List<Pair<Path, Long>> files = new ArrayList<Pair<Path, Long>>();
    for (long i = 0; i <= 20; i++) {
      files.add(new Pair<Path, Long>(new Path("file-" + i), i));
    }

    // Create 5 groups (total size 210)
    //    group 0: 20, 11, 10,  1 (total size: 42)
    //    group 1: 19, 12,  9,  2 (total size: 42)
    //    group 2: 18, 13,  8,  3 (total size: 42)
    //    group 3: 17, 12,  7,  4 (total size: 42)
    //    group 4: 16, 11,  6,  5 (total size: 42)
    List<List<Path>> splits = ExportSnapshot.getBalancedSplits(files, 5);
    assertEquals(5, splits.size());
    assertEquals(Arrays.asList(new Path("file-20"), new Path("file-11"),
      new Path("file-10"), new Path("file-1"), new Path("file-0")), splits.get(0));
    assertEquals(Arrays.asList(new Path("file-19"), new Path("file-12"),
      new Path("file-9"), new Path("file-2")), splits.get(1));
    assertEquals(Arrays.asList(new Path("file-18"), new Path("file-13"),
      new Path("file-8"), new Path("file-3")), splits.get(2));
    assertEquals(Arrays.asList(new Path("file-17"), new Path("file-14"),
      new Path("file-7"), new Path("file-4")), splits.get(3));
    assertEquals(Arrays.asList(new Path("file-16"), new Path("file-15"),
      new Path("file-6"), new Path("file-5")), splits.get(4));
  }

  /**
   * Verify if exported snapshot and copied files matches the original one.
   */
  @Test
  public void testExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, snapshotName, 2);
  }

  @Test
  public void testEmptyExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, emptySnapshotName, 1);
  }

  /**
   * Mock a snapshot with files in the archive dir,
   * two regions, and one reference file.
   */
  @Test
  public void testSnapshotWithRefsExportFileSystemState() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    final byte[] tableWithRefsName = Bytes.toBytes("tableWithRefs");
    final String snapshotName = "tableWithRefs";
    final String TEST_FAMILY = Bytes.toString(FAMILY);
    final String TEST_HFILE = "abc";

    final SnapshotDescription sd = SnapshotDescription.newBuilder()
        .setName(snapshotName).setTable(Bytes.toString(tableWithRefsName)).build();

    FileSystem fs = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    Path archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);

    HTableDescriptor htd = new HTableDescriptor(tableWithRefsName);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));

    // First region, simple with one plain hfile.
    HRegion r0 = HRegion.createHRegion(new HRegionInfo(htd.getName()), archiveDir,
        conf, htd, null, true, true);
    Path storeFile = new Path(new Path(r0.getRegionDir(), TEST_FAMILY), TEST_HFILE);
    FSDataOutputStream out = fs.create(storeFile);
    out.write(Bytes.toBytes("Test Data"));
    out.close();
    r0.close();

    // Second region, used to test the split case.
    // This region contains a reference to the hfile in the first region.
    HRegion r1 = HRegion.createHRegion(new HRegionInfo(htd.getName()), archiveDir,
        conf, htd, null, true, true);
    out = fs.create(new Path(new Path(r1.getRegionDir(), TEST_FAMILY),
        storeFile.getName() + '.' + r0.getRegionInfo().getEncodedName()));
    out.write(Bytes.toBytes("Test Data"));
    out.close();
    r1.close();

    Path tableDir = HTableDescriptor.getTableDir(archiveDir, tableWithRefsName);
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    FileUtil.copy(fs, tableDir, fs, snapshotDir, false, conf);
    SnapshotDescriptionUtils.writeSnapshotInfo(sd, snapshotDir, fs);

    testExportFileSystemState(tableWithRefsName, Bytes.toBytes(snapshotName), 2);
  }

  /**
   * Test ExportSnapshot
   */
  private void testExportFileSystemState(final byte[] tableName, final byte[] snapshotName,
      int filesExpected) throws Exception {
    Path copyDir = TEST_UTIL.getDataTestDir("export-" + System.currentTimeMillis());
    URI hdfsUri = FileSystem.get(TEST_UTIL.getConfiguration()).getUri();
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs);

    // Export Snapshot
    int res = ExportSnapshot.innerMain(TEST_UTIL.getConfiguration(), new String[] {
      "-snapshot", Bytes.toString(snapshotName),
      "-copy-to", copyDir.toString()
    });
    assertEquals(0, res);

    // Verify File-System state
    FileStatus[] rootFiles = fs.listStatus(copyDir);
    assertEquals(filesExpected, rootFiles.length);
    for (FileStatus fileStatus: rootFiles) {
      String name = fileStatus.getPath().getName();
      assertTrue(fileStatus.isDir());
      assertTrue(name.equals(".snapshot") || name.equals(".archive"));
    }

    // compare the snapshot metadata and verify the hfiles
    final FileSystem hdfs = FileSystem.get(hdfsUri, TEST_UTIL.getConfiguration());
    final Path snapshotDir = new Path(".snapshot", Bytes.toString(snapshotName));
    verifySnapshot(hdfs, new Path(TEST_UTIL.getDefaultRootDirPath(), snapshotDir),
        fs, new Path(copyDir, snapshotDir));
    verifyArchive(fs, copyDir, tableName, Bytes.toString(snapshotName));
    FSUtils.logFileSystemState(hdfs, snapshotDir, LOG);

    // Remove the exported dir
    fs.delete(copyDir, true);
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
      final byte[] tableName, final String snapshotName) throws IOException {
    final Path exportedSnapshot = new Path(rootDir,
      new Path(HConstants.SNAPSHOT_DIR_NAME, snapshotName));
    final Path exportedArchive = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    LOG.debug(listFiles(fs, exportedArchive, exportedArchive));
    SnapshotReferenceUtil.visitReferencedFiles(fs, exportedSnapshot,
        new SnapshotReferenceUtil.FileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          verifyNonEmptyFile(new Path(exportedArchive,
            new Path(Bytes.toString(tableName), new Path(region, new Path(family, hfile)))));
        }

        public void recoveredEdits (final String region, final String logfile)
            throws IOException {
          verifyNonEmptyFile(new Path(exportedSnapshot,
            new Path(Bytes.toString(tableName), new Path(region, logfile))));
        }

        public void logFile (final String server, final String logfile)
            throws IOException {
          verifyNonEmptyFile(new Path(exportedSnapshot, new Path(server, logfile)));
        }

        private void verifyNonEmptyFile(final Path path) throws IOException {
          assertTrue(path + " should exist", fs.exists(path));
          assertTrue(path + " should not be empty", fs.getFileStatus(path).getLen() > 0);
        }
    });
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
}
