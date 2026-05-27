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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseTestingUtil;
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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * Test the restore/clone operation from a file-system point of view.
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestMapreduceRestoreSnapshotHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TestMapreduceRestoreSnapshotHelper.class);

  protected final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  protected final static String TEST_HFILE = "abc";

  protected Configuration conf;
  protected Path archiveDir;
  protected FileSystem fs;
  protected Path rootDir;

  protected void setupConf(Configuration conf) {
  }

  @BeforeAll
  public static void setupCluster() throws Exception {
    TEST_UTIL.getConfiguration().setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 3);
    TEST_UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDownCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeEach
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

  @AfterEach
  public void tearDown() throws Exception {
    fs.delete(TEST_UTIL.getDataTestDir(), true);
  }

  protected SnapshotMock createSnapshotMock() throws IOException {
    return new SnapshotMock(TEST_UTIL.getConfiguration(), fs, rootDir);
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
    MapreduceRestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
    checkNoHFileLinkInTableDir(tableName);
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
    assertThrows(SnapshotTTLExpiredException.class, () -> MapreduceRestoreSnapshotHelper
      .copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName));
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
      assertFalse(hasHFileLink(tableDir));
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

  private void verifyRestore(final Path rootDir, final TableDescriptor sourceHtd,
    final TableDescriptor htdClone) throws IOException {
    List<String> files = SnapshotTestingUtils.listHFileNames(fs,
      CommonFSUtils.getTableDir(rootDir, htdClone.getTableName()));
    assertEquals(12, files.size());
    for (int i = 0; i < files.size(); i += 2) {
      String linkFile = files.get(i);
      String refFile = files.get(i + 1);
      assertTrue(HFileLink.isHFileLink(linkFile), linkFile + " should be a HFileLink");
      assertTrue(StoreFileInfo.isReference(refFile), refFile + " should be a Reference");
      assertEquals(sourceHtd.getTableName(), HFileLink.getReferencedTableName(linkFile));
      Path refPath = getReferredToFile(refFile);
      LOG.debug("get reference name for file " + refFile + " = " + refPath);
      assertTrue(HFileLink.isHFileLink(refPath.getName()),
        refPath.getName() + " should be a HFileLink");
      assertEquals(linkFile, refPath.getName());
    }
  }

  private Path getReferredToFile(final String referenceName) {
    Path fakeBasePath = new Path(new Path("table", "region"), "cf");
    return StoreFileInfo.getReferredToFile(new Path(fakeBasePath, referenceName));
  }
}
