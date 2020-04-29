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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * Test the restore/clone operation from a file-system point of view.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestRestoreSnapshotHelper {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRestoreSnapshotHelper.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRestoreSnapshotHelper.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected final static String TEST_HFILE = "abc";

  protected Configuration conf;
  protected Path archiveDir;
  protected FileSystem fs;
  protected Path rootDir;

  protected void setupConf(Configuration conf) {
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    rootDir = TEST_UTIL.getDataTestDir("testRestore");
    archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    setupConf(conf);
    CommonFSUtils.setRootDir(conf, rootDir);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(TEST_UTIL.getDataTestDir(), true);
  }

  protected SnapshotMock createSnapshotMock() throws IOException {
    return new SnapshotMock(TEST_UTIL.getConfiguration(), fs, rootDir);
  }

  @Test
  public void testRestore() throws IOException {
    restoreAndVerify("snapshot", "testRestore");
  }

  @Test
  public void testRestoreWithNamespace() throws IOException {
    restoreAndVerify("snapshot", "namespace1:testRestoreWithNamespace");
  }

  @Test
  public void testNoHFileLinkInRootDir() throws IOException {
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    CommonFSUtils.setRootDir(conf, rootDir);
    fs = rootDir.getFileSystem(conf);

    TableName tableName = TableName.valueOf("testNoHFileLinkInRootDir");
    String snapshotName = tableName.getNameAsString() + "-snapshot";
    createTableAndSnapshot(tableName, snapshotName);

    Path restoreDir = new Path("/hbase/.tmp-restore");
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
    checkNoHFileLinkInTableDir(tableName);
  }

  @Test
  public void testSkipReplayAndUpdateSeqId() throws Exception {
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    CommonFSUtils.setRootDir(conf, rootDir);
    TableName tableName = TableName.valueOf("testSkipReplayAndUpdateSeqId");
    String snapshotName = "testSkipReplayAndUpdateSeqId";
    createTableAndSnapshot(tableName, snapshotName);
    // put some data in the table
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    TEST_UTIL.loadTable(table, Bytes.toBytes("A"));

    Configuration conf = TEST_UTIL.getConfiguration();
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path restoreDir = new Path("/hbase/.tmp-restore/testScannerWithRestoreScanner2");
    // restore snapshot.
    final RestoreSnapshotHelper.RestoreMetaChanges meta =
      RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
    TableDescriptor htd = meta.getTableDescriptor();
    final List<RegionInfo> restoredRegions = meta.getRegionsToAdd();
    for (RegionInfo restoredRegion : restoredRegions) {
      // open restored region
      HRegion region = HRegion.newHRegion(CommonFSUtils.getTableDir(restoreDir, tableName), null,
        fs, conf, restoredRegion, htd, null);
      // set restore flag
      region.setRestoredRegion(true);
      region.initialize();
      Path recoveredEdit =
        CommonFSUtils.getWALRegionDir(conf, tableName, region.getRegionInfo().getEncodedName());
      long maxSeqId = WALSplitUtil.getMaxRegionSequenceId(fs, recoveredEdit);

      // open restored region without set restored flag
      HRegion region2 = HRegion.newHRegion(CommonFSUtils.getTableDir(restoreDir, tableName), null,
        fs, conf, restoredRegion, htd, null);
      region2.initialize();
      long maxSeqId2 = WALSplitUtil.getMaxRegionSequenceId(fs, recoveredEdit);
      Assert.assertTrue(maxSeqId2 > maxSeqId);
    }
  }

  protected void createTableAndSnapshot(TableName tableName, String snapshotName)
      throws IOException {
    byte[] column = Bytes.toBytes("A");
    Table table = TEST_UTIL.createTable(tableName, column, 2);
    TEST_UTIL.loadTable(table, column);
    TEST_UTIL.getAdmin().snapshot(snapshotName, tableName);
  }

  private void checkNoHFileLinkInTableDir(TableName tableName) throws IOException {
    Path[] tableDirs = new Path[] { CommonFSUtils.getTableDir(rootDir, tableName),
        CommonFSUtils.getTableDir(new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY), tableName),
        CommonFSUtils.getTableDir(MobUtils.getMobHome(rootDir), tableName) };
    for (Path tableDir : tableDirs) {
      Assert.assertFalse(hasHFileLink(tableDir));
    }
  }

  private boolean hasHFileLink(Path tableDir) throws IOException {
    if (fs.exists(tableDir)) {
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(tableDir, true);
      while (iterator.hasNext()) {
        LocatedFileStatus fileStatus = iterator.next();
        if (fileStatus.isFile() && HFileLink.isHFileLink(fileStatus.getPath())) {
          return true;
        }
      }
    }
    return false;
  }

  private void restoreAndVerify(final String snapshotName, final String tableName) throws IOException {
    // Test Rolling-Upgrade like Snapshot.
    // half machines writing using v1 and the others using v2 format.
    SnapshotMock snapshotMock = createSnapshotMock();
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2("snapshot", tableName);
    builder.addRegionV1();
    builder.addRegionV2();
    builder.addRegionV2();
    builder.addRegionV1();
    Path snapshotDir = builder.commit();
    TableDescriptor htd = builder.getTableDescriptor();
    SnapshotDescription desc = builder.getSnapshotDescription();

    // Test clone a snapshot
    TableDescriptor htdClone = snapshotMock.createHtd("testtb-clone");
    testRestore(snapshotDir, desc, htdClone);
    verifyRestore(rootDir, htd, htdClone);

    // Test clone a clone ("link to link")
    SnapshotDescription cloneDesc = SnapshotDescription.newBuilder()
        .setName("cloneSnapshot")
        .setTable("testtb-clone")
        .build();
    Path cloneDir = CommonFSUtils.getTableDir(rootDir, htdClone.getTableName());
    TableDescriptor htdClone2 = snapshotMock.createHtd("testtb-clone2");
    testRestore(cloneDir, cloneDesc, htdClone2);
    verifyRestore(rootDir, htd, htdClone2);
  }

  private void verifyRestore(final Path rootDir, final TableDescriptor sourceHtd,
      final TableDescriptor htdClone) throws IOException {
    List<String> files = SnapshotTestingUtils.listHFileNames(fs,
      CommonFSUtils.getTableDir(rootDir, htdClone.getTableName()));
    assertEquals(12, files.size());
    for (int i = 0; i < files.size(); i += 2) {
      String linkFile = files.get(i);
      String refFile = files.get(i+1);
      assertTrue(linkFile + " should be a HFileLink", HFileLink.isHFileLink(linkFile));
      assertTrue(refFile + " should be a Referene", StoreFileInfo.isReference(refFile));
      assertEquals(sourceHtd.getTableName(), HFileLink.getReferencedTableName(linkFile));
      Path refPath = getReferredToFile(refFile);
      LOG.debug("get reference name for file " + refFile + " = " + refPath);
      assertTrue(refPath.getName() + " should be a HFileLink", HFileLink.isHFileLink(refPath.getName()));
      assertEquals(linkFile, refPath.getName());
    }
  }

  /**
   * Execute the restore operation
   * @param snapshotDir The snapshot directory to use as "restore source"
   * @param sd The snapshot descriptor
   * @param htdClone The HTableDescriptor of the table to restore/clone.
   */
  private void testRestore(final Path snapshotDir, final SnapshotDescription sd,
      final TableDescriptor htdClone) throws IOException {
    LOG.debug("pre-restore table=" + htdClone.getTableName() + " snapshot=" + snapshotDir);
    CommonFSUtils.logFileSystemState(fs, rootDir, LOG);

    new FSTableDescriptors(conf).createTableDescriptor(htdClone);
    RestoreSnapshotHelper helper = getRestoreHelper(rootDir, snapshotDir, sd, htdClone);
    helper.restoreHdfsRegions();

    LOG.debug("post-restore table=" + htdClone.getTableName() + " snapshot=" + snapshotDir);
    CommonFSUtils.logFileSystemState(fs, rootDir, LOG);
  }

  /**
   * Initialize the restore helper, based on the snapshot and table information provided.
   */
  private RestoreSnapshotHelper getRestoreHelper(final Path rootDir, final Path snapshotDir,
      final SnapshotDescription sd, final TableDescriptor htdClone) throws IOException {
    ForeignExceptionDispatcher monitor = Mockito.mock(ForeignExceptionDispatcher.class);
    MonitoredTask status = Mockito.mock(MonitoredTask.class);

    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, sd);
    return new RestoreSnapshotHelper(conf, fs, manifest,
      htdClone, rootDir, monitor, status);
  }

  private Path getReferredToFile(final String referenceName) {
    Path fakeBasePath = new Path(new Path("table", "region"), "cf");
    return StoreFileInfo.getReferredToFile(new Path(fakeBasePath, referenceName));
  }
}
