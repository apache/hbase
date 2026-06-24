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
package org.apache.hadoop.hbase.security.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ActiveClusterSuffix;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ReadOnlyTransitionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.NoOpScanPolicyObserver;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CoprocessorConfigurationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(SecurityTests.TAG)
@Tag(MediumTests.TAG)
public class TestReadOnlyManageActiveClusterFile {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestReadOnlyManageActiveClusterFile.class);
  private final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final int NUM_RS = 1;
  private static Configuration conf;
  HMaster master;
  HRegionServer regionServer;

  MasterFileSystem mfs;
  Path rootDir;
  FileSystem fs;
  Path activeClusterFile;

  @BeforeEach
  public void setup() throws Exception {
    conf = TEST_UTIL.getConfiguration();

    // Set up test class with Read-Only mode disabled so a table can be created
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    // Start the test cluster
    TEST_UTIL.startMiniCluster(NUM_RS);
    master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    regionServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);

    mfs = master.getMasterFileSystem();
    rootDir = mfs.getRootDir();
    fs = mfs.getFileSystem();
    activeClusterFile = new Path(rootDir, HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME);
  }

  @AfterEach
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void setReadOnlyMode(boolean enabled) {
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, enabled);
    master.getConfigurationManager().notifyAllObservers(conf);
    regionServer.getConfigurationManager().notifyAllObservers(conf);
  }

  private void restartCluster() throws IOException, InterruptedException {
    TEST_UTIL.getMiniHBaseCluster().shutdown();
    TEST_UTIL.restartHBaseCluster(NUM_RS);
    TEST_UTIL.waitUntilNoRegionsInTransition();

    master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    regionServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);

    MasterFileSystem mfs = master.getMasterFileSystem();
    fs = mfs.getFileSystem();
    activeClusterFile = new Path(mfs.getRootDir(), HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME);
  }

  private void overwriteExistingFile() throws IOException {
    try (FSDataOutputStream out = fs.create(activeClusterFile, true)) {
      out.writeBytes("newClusterId");
    }
  }

  private boolean activeClusterIdFileExists() throws IOException {
    return fs.exists(activeClusterFile);
  }

  @Test
  public void testActiveClusterIdFileCreationWhenReadOnlyDisabled()
    throws IOException, InterruptedException {
    setReadOnlyMode(false);
    assertTrue(activeClusterIdFileExists());
  }

  @Test
  public void testActiveClusterIdFileDeletionWhenReadOnlyEnabled()
    throws IOException, InterruptedException {
    setReadOnlyMode(true);
    assertFalse(activeClusterIdFileExists());
  }

  @Test
  public void testDeleteActiveClusterIdFileWhenSwitchingToReadOnlyIfOwnedByCluster()
    throws IOException, InterruptedException {
    // At the start cluster is in active mode hence set readonly mode and restart
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    // Restart the cluster to trigger the deletion of the active cluster ID file
    restartCluster();
    // Should delete the active cluster ID file since it is owned by the cluster
    assertFalse(activeClusterIdFileExists());
  }

  @Test
  public void testDoNotDeleteActiveClusterIdFileWhenSwitchingToReadOnlyIfNotOwnedByCluster()
    throws IOException, InterruptedException {
    // Change the content of Active Cluster file to simulate the scenario where the file is not
    // owned by the cluster and then set readonly mode and restart
    overwriteExistingFile();
    // At the start cluster is in active mode hence set readonly mode and restart
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    // Restart the cluster to trigger the deletion of the active cluster ID file
    restartCluster();
    // As Active cluster file is not owned by the cluster, it should not be deleted even when
    // switching to readonly mode
    assertTrue(activeClusterIdFileExists());
  }

  @Test
  public void testCannotDisableReadOnlyWhenAnotherClusterIsActive() throws Exception {
    // First enable read-only mode (simulating a replica cluster)
    setReadOnlyMode(true);
    assertFalse(activeClusterIdFileExists());

    // Now write an active cluster file with a DIFFERENT cluster's data (simulating another active
    // cluster owning the storage)
    overwriteExistingFile();
    assertTrue(activeClusterIdFileExists());

    // Attempt to disable read-only mode, but get an exception because another active cluster
    // already exists.
    master.getConfiguration().setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);

    // Master's updateConfiguration should throw because another cluster is active
    assertThrows(ReadOnlyTransitionException.class, () -> master.updateConfiguration());

    // Verify read-only coprocessors are still loaded on the master
    assertTrue(CoprocessorConfigurationUtil.areReadOnlyCoprocessorsLoaded(master.getConfiguration(),
      CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY));

    // Verify read-only coprocessors are still loaded on the region server
    assertTrue(CoprocessorConfigurationUtil.areReadOnlyCoprocessorsLoaded(
      regionServer.getConfiguration(), CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY));
  }

  @Test
  public void testCanDisableReadOnlyWhenOwnClusterIsActive() throws Exception {
    // Enable read-only mode
    setReadOnlyMode(true);
    assertFalse(activeClusterIdFileExists());

    // Verify read-only coprocessors are loaded
    assertTrue(CoprocessorConfigurationUtil.areReadOnlyCoprocessorsLoaded(master.getConfiguration(),
      CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY));

    // Disable read-only mode (our own cluster, no conflicting active cluster file)
    setReadOnlyMode(false);

    // Active cluster file should be recreated
    assertTrue(activeClusterIdFileExists());

    // Verify read-only coprocessors are removed
    assertFalse(CoprocessorConfigurationUtil.areReadOnlyCoprocessorsLoaded(
      master.getConfiguration(), CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY));
  }

  @Test
  public void testRegionCoprocessorsStillLoadWhenReadOnlyTransitionBlocked() throws Exception {
    // Create a table so we have a region to work with
    TableName tableName = TableName.valueOf("testCoprocessorLoadTable");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();
    TEST_UTIL.getAdmin().createTable(desc);
    List<HRegion> regions = regionServer.getRegions(tableName);
    assertFalse(regions.isEmpty());
    HRegion region = regions.get(0);

    // Enable read-only mode to load ReadOnly coprocessors on the region
    Configuration readOnlyConf = new Configuration(conf);
    readOnlyConf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    region.onConfigurationChange(readOnlyConf);

    // Verify ReadOnly coprocessors are loaded
    RegionCoprocessorHost regionCPHost = region.getCoprocessorHost();
    assertNotNull(regionCPHost.findCoprocessor(RegionReadOnlyController.class.getName()),
      "RegionReadOnlyController should be loaded after enabling read-only mode");

    // Simulate another active cluster by writing a foreign cluster ID to the active cluster file
    overwriteExistingFile();
    assertTrue(activeClusterIdFileExists());

    // Build a config that attempts to disable read-only AND adds new coprocessors
    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);

    // Add a system and user region coprocessors
    newConf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, SimpleRegionObserver.class.getName());
    newConf.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      NoOpScanPolicyObserver.class.getName());

    // Trigger dynamic configuration change on the region
    region.onConfigurationChange(newConf);

    // Verify ReadOnly coprocessors are still loaded since the read-only transition was blocked
    regionCPHost = region.getCoprocessorHost();
    assertTrue(
      CoprocessorConfigurationUtil.areReadOnlyCoprocessorsLoaded(region.getConfiguration(),
        CoprocessorHost.REGION_COPROCESSOR_CONF_KEY),
      "ReadOnly coprocessors should remain in the region configuration");

    // Verify new system and user coprocessors were loaded despite the blocked read-only transition
    assertNotNull(regionCPHost.findCoprocessor(SimpleRegionObserver.class.getName()),
      "SimpleRegionObserver should be loaded even when read-only transition is blocked");
    assertNotNull(regionCPHost.findCoprocessor(NoOpScanPolicyObserver.class.getName()),
      "NoOpScanPolicyObserver should be loaded even when read-only transition is blocked");
  }

  @Test
  public void testRegionServerCannotDisableReadOnlyWhenAnotherClusterIsActive() throws Exception {
    // First enable read-only mode
    setReadOnlyMode(true);
    assertFalse(activeClusterIdFileExists());

    // Write an active cluster file with a different cluster's data
    overwriteExistingFile();
    assertTrue(activeClusterIdFileExists());

    // Attempt to disable read-only mode, but get an exception because another active cluster
    // already exists
    regionServer.getConfiguration().setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);

    // RegionServer's updateConfiguration should throw because another cluster is active
    assertThrows(ReadOnlyTransitionException.class, () -> regionServer.updateConfiguration());

    // Verify read-only coprocessors are still loaded on the region server
    assertTrue(CoprocessorConfigurationUtil.areReadOnlyCoprocessorsLoaded(
      regionServer.getConfiguration(), CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY));
  }

  @Test
  public void testBlockedThenSuccessfulReadOnlyTransition() throws Exception {
    // Put cluster in read-only mode with a foreign active cluster file
    setReadOnlyMode(true);
    assertFalse(activeClusterIdFileExists());
    overwriteExistingFile();
    assertTrue(activeClusterIdFileExists());

    // Attempt to disable read-only mode. This should fail because another cluster is active.
    // We need to run updateConfiguration() to ensure the readOnlyTransitionBlocked boolean in
    // HBaseServerBase gets set.
    master.getConfiguration().setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    assertThrows(ReadOnlyTransitionException.class, () -> master.updateConfiguration());
    regionServer.getConfiguration().setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    assertThrows(ReadOnlyTransitionException.class, () -> regionServer.updateConfiguration());

    // Try to create a table. This should fail because the cluster is still in read-only mode
    TableName tableName = TableName.valueOf("testBlockedTransitionTable");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();
    assertThrows(IOException.class, () -> TEST_UTIL.getAdmin().createTable(desc));

    // Delete the foreign active cluster file
    fs.delete(activeClusterFile, false);
    assertFalse(activeClusterIdFileExists());

    // Disable read-only mode again. This should succeed now that no foreign active cluster file
    // exists. We need to run updateConfiguration() to ensure the readOnlyTransitionBlocked boolean
    // in HBaseServerBase was reset.
    master.getConfiguration().setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    master.updateConfiguration();
    regionServer.getConfiguration().setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    regionServer.updateConfiguration();

    // Verify the active.cluster.suffix.id file contains this cluster's ID
    assertTrue(activeClusterIdFileExists());
    try (FSDataInputStream in = fs.open(activeClusterFile)) {
      ActiveClusterSuffix actual = ActiveClusterSuffix.parseFrom(in.readAllBytes());
      ActiveClusterSuffix expected = ActiveClusterSuffix.fromConfig(conf, mfs.getClusterId());
      assertEquals(expected, actual);
    }

    // Create a table and add a row to verify read-only mode has been disabled
    TEST_UTIL.getAdmin().createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("value1"));
      table.put(put);
    }
  }
}
