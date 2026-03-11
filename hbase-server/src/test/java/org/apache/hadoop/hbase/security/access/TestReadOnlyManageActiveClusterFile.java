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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ SecurityTests.class, MediumTests.class })
public class TestReadOnlyManageActiveClusterFile {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyManageActiveClusterFile.class);

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

  @Before
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

  @After
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
}
