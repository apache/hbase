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
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.HFileLinkCleaner;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test basic snapshot manager functionality
 */
@Category({MasterTests.class, SmallTests.class})
public class TestSnapshotManager {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  MasterServices services = Mockito.mock(MasterServices.class);
  MetricsMaster metrics = Mockito.mock(MetricsMaster.class);
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

  private SnapshotManager getNewManager(final Configuration conf)
      throws IOException, KeeperException {
    Mockito.reset(services);
    Mockito.when(services.getConfiguration()).thenReturn(conf);
    Mockito.when(services.getMasterFileSystem()).thenReturn(mfs);
    Mockito.when(mfs.getFileSystem()).thenReturn(fs);
    Mockito.when(mfs.getRootDir()).thenReturn(UTIL.getDataTestDir());
    return new SnapshotManager(services, metrics, coordinator, pool);
  }

  @Test
  public void testInProcess() throws KeeperException, IOException {
    TableName tableName = TableName.valueOf("testTable");
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
    conf.set(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS, SnapshotLogCleaner.class.getName());
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, false);
    manager = getNewManager(conf);
    assertFalse("Snapshot should be disabled", isSnapshotSupported(manager));

    // cleaners are present, but missing snapshot enabled property
    conf = new Configuration();
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
      SnapshotHFileCleaner.class.getName(), HFileLinkCleaner.class.getName());
    conf.set(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS, SnapshotLogCleaner.class.getName());
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

  private boolean isSnapshotSupported(final SnapshotManager manager) {
    try {
      manager.checkSnapshotSupport();
      return true;
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }
}
