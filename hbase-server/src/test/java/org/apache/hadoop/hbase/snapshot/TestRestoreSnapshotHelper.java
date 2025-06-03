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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
@Category({ RegionServerTests.class, MediumTests.class })
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
    TEST_UTIL.getConfiguration().setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 3);
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
    // Turn off balancer so it doesn't cut in and mess up our placements.
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
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

  @Test
  public void testCopyExpiredSnapshotForScanner() throws IOException, InterruptedException {
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    CommonFSUtils.setRootDir(conf, rootDir);
    TableName tableName = TableName.valueOf("testCopyExpiredSnapshotForScanner");
    String snapshotName = tableName.getNameAsString() + "-snapshot";
    Path restoreDir = new Path("/hbase/.tmp-expired-snapshot/copySnapshotDest");
    // create table and put some data into the table
    byte[] columnFamily = Bytes.toBytes("A");
    Table table = TEST_UTIL.createTable(tableName, columnFamily);
    TEST_UTIL.loadTable(table, columnFamily);
    // create snapshot with ttl = 10 sec
    Map<String, Object> properties = new HashMap<>();
    properties.put("TTL", 10);
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotDesc =
      new org.apache.hadoop.hbase.client.SnapshotDescription(snapshotName, tableName,
        SnapshotType.FLUSH, null, EnvironmentEdgeManager.currentTime(), -1, properties);
    TEST_UTIL.getAdmin().snapshot(snapshotDesc);
    boolean isExist = TEST_UTIL.getAdmin().listSnapshots().stream()
      .anyMatch(ele -> snapshotName.equals(ele.getName()));
    assertTrue(isExist);
    int retry = 6;
    while (
      !SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDesc.getTtl(),
        snapshotDesc.getCreationTime(), EnvironmentEdgeManager.currentTime()) && retry > 0
    ) {
      retry--;
      Thread.sleep(10 * 1000);
    }
    boolean isExpiredSnapshot = SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDesc.getTtl(),
      snapshotDesc.getCreationTime(), EnvironmentEdgeManager.currentTime());
    assertTrue(isExpiredSnapshot);
    assertThrows(SnapshotTTLExpiredException.class, () -> RestoreSnapshotHelper
      .copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName));
  }

  /**
   * Test scenario for HBASE-29346, which addresses the issue where restoring snapshots after region
   * merge operations could lead to missing store file references, potentially resulting in data
   * loss.
   * <p>
   * This test performs the following steps:
   * </p>
   * <ol>
   * <li>Creates a table with multiple regions.</li>
   * <li>Inserts data into each region and flushes to create store files.</li>
   * <li>Takes snapshot of the table and performs restore.</li>
   * <li>Disable compactions, merge regions, create a new snapshot, and restore that snapshot on the
   * same restore path.</li>
   * <li>Verifies data integrity by scanning all data post region re-open.</li>
   * </ol>
   */
  @Test
  public void testMultiSnapshotRestoreWithMerge() throws IOException, InterruptedException {
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    CommonFSUtils.setRootDir(conf, rootDir);
    TableName tableName = TableName.valueOf("testMultiSnapshotRestoreWithMerge");
    Path restoreDir = new Path("/hbase/.tmp-snapshot/restore-snapshot-dest");

    byte[] columnFamily = Bytes.toBytes("A");
    Table table = TEST_UTIL.createTable(tableName, new byte[][] { columnFamily },
      new byte[][] { new byte[] { 'b' }, new byte[] { 'd' } });
    Put put1 = new Put(Bytes.toBytes("a")); // Region 1: [-∞, b)
    put1.addColumn(columnFamily, Bytes.toBytes("q"), Bytes.toBytes("val1"));
    table.put(put1);
    Put put2 = new Put(Bytes.toBytes("b")); // Region 2: [b, d)
    put2.addColumn(columnFamily, Bytes.toBytes("q"), Bytes.toBytes("val2"));
    table.put(put2);
    Put put3 = new Put(Bytes.toBytes("d")); // Region 3: [d, +∞)
    put3.addColumn(columnFamily, Bytes.toBytes("q"), Bytes.toBytes("val3"));
    table.put(put3);

    TEST_UTIL.getAdmin().flush(tableName);

    String snapshotOne = tableName.getNameAsString() + "-snapshot-one";
    createAndAssertSnapshot(tableName, snapshotOne);
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotOne);
    flipCompactions(false);
    mergeRegions(tableName, 2);
    String snapshotTwo = tableName.getNameAsString() + "-snapshot-two";
    createAndAssertSnapshot(tableName, snapshotTwo);
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotTwo);
    flipCompactions(true);

    TEST_UTIL.getAdmin().disableTable(tableName);
    TEST_UTIL.getAdmin().enableTable(tableName);
    try (ResultScanner scanner = table.getScanner(new Scan())) {
      assertEquals(3, scanner.next(4).length);
    }
    String snapshotThree = tableName.getNameAsString() + "-snapshot-three";
    createAndAssertSnapshot(tableName, snapshotThree);
  }

  private void createAndAssertSnapshot(TableName tableName, String snapshotName)
    throws SnapshotCreationException, IllegalArgumentException, IOException {
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotDescOne =
      new org.apache.hadoop.hbase.client.SnapshotDescription(snapshotName, tableName,
        SnapshotType.FLUSH, null, EnvironmentEdgeManager.currentTime(), -1);
    TEST_UTIL.getAdmin().snapshot(snapshotDescOne);
    boolean isExist = TEST_UTIL.getAdmin().listSnapshots().stream()
      .anyMatch(ele -> snapshotName.equals(ele.getName()));
    assertTrue(isExist);

  }

  private void flipCompactions(boolean isEnable) {
    int numLiveRegionServers = TEST_UTIL.getHBaseCluster().getNumLiveRegionServers();
    for (int serverNumber = 0; serverNumber < numLiveRegionServers; serverNumber++) {
      HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(serverNumber);
      regionServer.getCompactSplitThread().setCompactionsEnabled(isEnable);
    }

  }

  private void mergeRegions(TableName tableName, int mergeCount) throws IOException {
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tableName);
    int originalRegionCount = ris.size();
    assertTrue(originalRegionCount > mergeCount);
    RegionInfo[] regionsToMerge = ris.subList(0, mergeCount).toArray(new RegionInfo[] {});
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    MergeTableRegionsProcedure proc =
      new MergeTableRegionsProcedure(procExec.getEnvironment(), regionsToMerge, true);
    long procId = procExec.submitProcedure(proc);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    MetaTableAccessor.fullScanMetaAndPrint(TEST_UTIL.getConnection());
    assertEquals(originalRegionCount - mergeCount + 1,
      MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tableName).size());
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
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

  private void restoreAndVerify(final String snapshotName, final String tableName)
    throws IOException {
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
    SnapshotDescription cloneDesc =
      SnapshotDescription.newBuilder().setName("cloneSnapshot").setTable("testtb-clone").build();
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
      String refFile = files.get(i + 1);
      assertTrue(linkFile + " should be a HFileLink", HFileLink.isHFileLink(linkFile));
      assertTrue(refFile + " should be a Referene", StoreFileInfo.isReference(refFile));
      assertEquals(sourceHtd.getTableName(), HFileLink.getReferencedTableName(linkFile));
      Path refPath = getReferredToFile(refFile);
      LOG.debug("get reference name for file " + refFile + " = " + refPath);
      assertTrue(refPath.getName() + " should be a HFileLink",
        HFileLink.isHFileLink(refPath.getName()));
      assertEquals(linkFile, refPath.getName());
    }
  }

  /**
   * Execute the restore operation
   * @param snapshotDir The snapshot directory to use as "restore source"
   * @param sd          The snapshot descriptor
   * @param htdClone    The HTableDescriptor of the table to restore/clone.
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
    return new RestoreSnapshotHelper(conf, fs, manifest, htdClone, rootDir, monitor, status);
  }

  private Path getReferredToFile(final String referenceName) {
    Path fakeBasePath = new Path(new Path("table", "region"), "cf");
    return StoreFileInfo.getReferredToFile(new Path(fakeBasePath, referenceName));
  }
}
