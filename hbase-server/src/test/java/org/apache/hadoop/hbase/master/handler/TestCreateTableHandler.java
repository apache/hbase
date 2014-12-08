/**
 *
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
package org.apache.hadoop.hbase.master.handler;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCreateTableHandler {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestCreateTableHandler.class);
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static boolean throwException = false;
  

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    throwException = false;
  }

  @Test (timeout=300000)
  public void testCreateTableCalledTwiceAndFirstOneInProgress() throws Exception {
    final byte[] tableName = Bytes.toBytes("testCreateTableCalledTwiceAndFirstOneInProgress");
    final MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    final HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    final HRegionInfo[] hRegionInfos = new HRegionInfo[] { new HRegionInfo(desc.getTableName(), null,
        null) };
    CustomCreateTableHandler handler = new CustomCreateTableHandler(m, m.getMasterFileSystem(),
        desc, cluster.getConfiguration(), hRegionInfos, m);
    handler.prepare();
    throwException = true;
    handler.process();
    throwException = false;
    CustomCreateTableHandler handler1 = new CustomCreateTableHandler(m, m.getMasterFileSystem(),
        desc, cluster.getConfiguration(), hRegionInfos, m);
    handler1.prepare();
    handler1.process();
    for (int i = 0; i < 100; i++) {
      if (!TEST_UTIL.getHBaseAdmin().isTableAvailable(tableName)) {
        Thread.sleep(200);
      }
    }
    assertTrue(TEST_UTIL.getHBaseAdmin().isTableEnabled(tableName));
  }

  @Test (timeout=300000)
  public void testCreateTableWithSplitRegion() throws Exception {
    final byte[] tableName = Bytes.toBytes("testCreateTableWithSplitRegion");
    final MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    final HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    byte[] splitPoint = Bytes.toBytes("split-point");
    long ts = System.currentTimeMillis();
    HRegionInfo d1 = new HRegionInfo(desc.getTableName(), null, splitPoint, false, ts);
    HRegionInfo d2 = new HRegionInfo(desc.getTableName(), splitPoint, null, false, ts + 1);
    HRegionInfo parent = new HRegionInfo(desc.getTableName(), null, null, true, ts + 2);
    parent.setOffline(true);

    Path tempdir = m.getMasterFileSystem().getTempDir();
    FileSystem fs = m.getMasterFileSystem().getFileSystem();
    Path tempTableDir = FSUtils.getTableDir(tempdir, desc.getTableName());
    fs.delete(tempTableDir, true); // Clean up temp table dir if exists

    final HRegionInfo[] hRegionInfos = new HRegionInfo[] {d1, d2, parent};
    CreateTableHandler handler = new CreateTableHandler(m, m.getMasterFileSystem(),
      desc, cluster.getConfiguration(), hRegionInfos, m);
    handler.prepare();
    handler.process();
    for (int i = 0; i < 100; i++) {
      if (!TEST_UTIL.getHBaseAdmin().isTableAvailable(tableName)) {
        Thread.sleep(300);
      }
    }
    assertTrue(TEST_UTIL.getHBaseAdmin().isTableEnabled(tableName));
    assertTrue(TEST_UTIL.getHBaseAdmin().isTableAvailable(tableName));
    assertTrue(TEST_UTIL.getHBaseAdmin().isTableAvailable(tableName, new byte[][] { splitPoint }));
    RegionStates regionStates = m.getAssignmentManager().getRegionStates();
    assertTrue("Parent should be in SPLIT state",
      regionStates.isRegionInState(parent, State.SPLIT));
  }

  @Test (timeout=60000)
  public void testMasterRestartAfterEnablingNodeIsCreated() throws Exception {
    byte[] tableName = Bytes.toBytes("testMasterRestartAfterEnablingNodeIsCreated");
    final MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    final HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    final HRegionInfo[] hRegionInfos = new HRegionInfo[] { new HRegionInfo(desc.getTableName(), null,
        null) };
    CustomCreateTableHandler handler = new CustomCreateTableHandler(m, m.getMasterFileSystem(),
        desc, cluster.getConfiguration(), hRegionInfos, m);
    handler.prepare();
    throwException = true;
    handler.process();
    abortAndStartNewMaster(cluster);
    assertTrue(cluster.getLiveMasterThreads().size() == 1);
  }

  private void abortAndStartNewMaster(final MiniHBaseCluster cluster) throws IOException {
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    LOG.info("Starting new master");
    cluster.startMaster();
    LOG.info("Waiting for master to become active.");
    cluster.waitForActiveAndReadyMaster();
  }

  private static class CustomCreateTableHandler extends CreateTableHandler {
    public CustomCreateTableHandler(Server server, MasterFileSystem fileSystemManager,
        HTableDescriptor hTableDescriptor, Configuration conf, HRegionInfo[] newRegions,
        MasterServices masterServices) {
      super(server, fileSystemManager, hTableDescriptor, conf, newRegions, masterServices);
    }

    @Override
    protected List<HRegionInfo> handleCreateHdfsRegions(Path tableRootDir,
        TableName tableName) throws IOException {
      if (throwException) {
        throw new IOException("Test throws exceptions.");
      }
      return super.handleCreateHdfsRegions(tableRootDir, tableName);
    }
  }
}
