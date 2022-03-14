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
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.HFileLinkCleaner;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

/**
 * Test basic snapshot manager functionality
 */
@Category({MasterTests.class, SmallTests.class})
public class TestSnapshotManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotManager.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  MasterServices services = Mockito.mock(MasterServices.class);
  ProcedureCoordinator coordinator = Mockito.mock(ProcedureCoordinator.class);
  ExecutorService pool = Mockito.mock(ExecutorService.class);
  MasterFileSystem mfs = Mockito.mock(MasterFileSystem.class);
  FileSystem fs;
  {
    try {
      fs = UTIL.getTestFileSystem();
    } catch (IOException e) {
      throw new RuntimeException("Couldn't get test filesystem", e);
    }
  }

   private SnapshotManager getNewManager() throws IOException, KeeperException {
    return getNewManager(UTIL.getConfiguration());
  }

  private SnapshotManager getNewManager(Configuration conf) throws IOException, KeeperException {
    return getNewManager(conf, 1);
  }

  private SnapshotManager getNewManager(Configuration conf, int intervalSeconds)
      throws IOException, KeeperException {
    Mockito.reset(services);
    Mockito.when(services.getConfiguration()).thenReturn(conf);
    Mockito.when(services.getMasterFileSystem()).thenReturn(mfs);
    Mockito.when(mfs.getFileSystem()).thenReturn(fs);
    Mockito.when(mfs.getRootDir()).thenReturn(UTIL.getDataTestDir());
    return new SnapshotManager(services, coordinator, pool, intervalSeconds);
  }

  @Test
  public void testCleanFinishedHandler() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    Configuration conf = UTIL.getConfiguration();
    try {
      conf.setLong(SnapshotManager.HBASE_SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLIS, 5 * 1000L);
      SnapshotManager manager = getNewManager(conf, 1);
      TakeSnapshotHandler handler = Mockito.mock(TakeSnapshotHandler.class);
      assertFalse("Manager is in process when there is no current handler",
        manager.isTakingSnapshot(tableName));
      manager.setSnapshotHandlerForTesting(tableName, handler);
      Mockito.when(handler.isFinished()).thenReturn(false);
      assertTrue(manager.isTakingAnySnapshot());
      assertTrue("Manager isn't in process when handler is running",
        manager.isTakingSnapshot(tableName));
      Mockito.when(handler.isFinished()).thenReturn(true);
      assertFalse("Manager is process when handler isn't running",
        manager.isTakingSnapshot(tableName));
      assertTrue(manager.isTakingAnySnapshot());
      Thread.sleep(6 * 1000);
      assertFalse(manager.isTakingAnySnapshot());
    } finally {
      conf.unset(SnapshotManager.HBASE_SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLIS);
    }
  }

  @Test
  public void testInProcess() throws KeeperException, IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    SnapshotManager manager = getNewManager();
    TakeSnapshotHandler handler = Mockito.mock(TakeSnapshotHandler.class);
    assertFalse("Manager is in process when there is no current handler",
        manager.isTakingSnapshot(tableName));
    manager.setSnapshotHandlerForTesting(tableName, handler);
    Mockito.when(handler.isFinished()).thenReturn(false);
    assertTrue("Manager isn't in process when handler is running",
        manager.isTakingSnapshot(tableName));
    Mockito.when(handler.isFinished()).thenReturn(true);
    assertFalse("Manager is process when handler isn't running",
        manager.isTakingSnapshot(tableName));
  }

  /**
   * Verify the snapshot support based on the configuration.
   */
  @Test
  public void testSnapshotSupportConfiguration() throws Exception {
    // No configuration (no cleaners, not enabled): snapshot feature disabled
    Configuration conf = new Configuration();
    SnapshotManager manager = getNewManager(conf);
    assertFalse("Snapshot should be disabled with no configuration", isSnapshotSupported(manager));

    // force snapshot feature to be enabled
    conf = new Configuration();
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    manager = getNewManager(conf);
    assertTrue("Snapshot should be enabled", isSnapshotSupported(manager));

    // force snapshot feature to be disabled
    conf = new Configuration();
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, false);
    manager = getNewManager(conf);
    assertFalse("Snapshot should be disabled", isSnapshotSupported(manager));

    // force snapshot feature to be disabled, even if cleaners are present
    conf = new Configuration();
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
      SnapshotHFileCleaner.class.getName(), HFileLinkCleaner.class.getName());
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, false);
    manager = getNewManager(conf);
    assertFalse("Snapshot should be disabled", isSnapshotSupported(manager));

    // cleaners are present, but missing snapshot enabled property
    conf = new Configuration();
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
      SnapshotHFileCleaner.class.getName(), HFileLinkCleaner.class.getName());
    manager = getNewManager(conf);
    assertTrue("Snapshot should be enabled, because cleaners are present",
      isSnapshotSupported(manager));

    // Create a "test snapshot"
    Path rootDir = UTIL.getDataTestDir();
    Path testSnapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
      "testSnapshotSupportConfiguration", rootDir);
    fs.mkdirs(testSnapshotDir);
    try {
      // force snapshot feature to be disabled, but snapshots are present
      conf = new Configuration();
      conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, false);
      manager = getNewManager(conf);
      fail("Master should not start when snapshot is disabled, but snapshots are present");
    } catch (UnsupportedOperationException e) {
      // expected
    } finally {
      fs.delete(testSnapshotDir, true);
    }
  }

  @Test
  public void testDisableSnapshotAndNotDeleteBackReference() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, false);
    SnapshotManager manager = getNewManager(conf);
    String cleaners = conf.get(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
    assertTrue(cleaners != null && cleaners.contains(HFileLinkCleaner.class.getName()));
    Path rootDir = UTIL.getDataTestDir();
    CommonFSUtils.setRootDir(conf, rootDir);

    TableName tableName = TableName.valueOf(name.getMethodName());
    TableName tableLinkName = TableName.valueOf(name.getMethodName() + "-link");
    String hfileName = "1234567890";
    String familyName = "cf";
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).build();
    RegionInfo hriLink = RegionInfoBuilder.newBuilder(tableLinkName).build();
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    Path archiveStoreDir = HFileArchiveUtil.getStoreArchivePath(conf,
      tableName, hri.getEncodedName(), familyName);

    // Create hfile /hbase/table-link/region/cf/getEncodedName.HFILE(conf);
    Path familyPath = getFamilyDirPath(archiveDir, tableName, hri.getEncodedName(), familyName);
    Path hfilePath = new Path(familyPath, hfileName);
    fs.createNewFile(hfilePath);
    // Create link to hfile
    Path familyLinkPath =
      getFamilyDirPath(rootDir, tableLinkName, hriLink.getEncodedName(), familyName);
    HFileLink.create(conf, fs, familyLinkPath, hri, hfileName);
    Path linkBackRefDir = HFileLink.getBackReferencesDir(archiveStoreDir, hfileName);
    assertTrue(fs.exists(linkBackRefDir));
    FileStatus[] backRefs = fs.listStatus(linkBackRefDir);
    assertEquals(1, backRefs.length);
    Path linkBackRef = backRefs[0].getPath();

    // Initialize cleaner
    HFileCleaner cleaner = new HFileCleaner(10000, Mockito.mock(Stoppable.class), conf, fs,
        archiveDir, new DirScanPool(UTIL.getConfiguration()));
    // Link backref and HFile cannot be removed
    cleaner.choreForTesting();
    assertTrue(fs.exists(linkBackRef));
    assertTrue(fs.exists(hfilePath));

    fs.rename(CommonFSUtils.getTableDir(rootDir, tableLinkName),
      CommonFSUtils.getTableDir(archiveDir, tableLinkName));
    // Link backref can be removed
    cleaner.choreForTesting();
    assertFalse("Link should be deleted", fs.exists(linkBackRef));
    // HFile can be removed
    cleaner.choreForTesting();
    assertFalse("HFile should be deleted", fs.exists(hfilePath));
  }

  private Path getFamilyDirPath(final Path rootDir, final TableName table, final String region,
      final String family) {
    return new Path(new Path(CommonFSUtils.getTableDir(rootDir, table), region), family);
  }

  private boolean isSnapshotSupported(final SnapshotManager manager) {
    try {
      manager.checkSnapshotSupport();
      return true;
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }
}
