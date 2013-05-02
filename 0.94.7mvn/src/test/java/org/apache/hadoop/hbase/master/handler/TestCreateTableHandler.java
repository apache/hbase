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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TestMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCreateTableHandler {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestCreateTableHandler.class);
  private static final byte[] TABLENAME = Bytes.toBytes("TestCreateTableHandler");
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static boolean throwException = false;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    throwException = true;
  }

  @Test
  public void testCreateTableHandlerIfCalledTwoTimesAndFirstOneIsUnderProgress() throws Exception {
    final MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    final HTableDescriptor desc = new HTableDescriptor(TABLENAME);
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    final HRegionInfo[] hRegionInfos = new HRegionInfo[] { new HRegionInfo(desc.getName(), null,
        null) };
    CustomCreateTableHandler handler = new CustomCreateTableHandler(m, m.getMasterFileSystem(),
        m.getServerManager(), desc, cluster.getConfiguration(), hRegionInfos,
        m.getCatalogTracker(), m.getAssignmentManager());
    throwException = true;
    handler.process();
    throwException = false;
    CustomCreateTableHandler handler1 = new CustomCreateTableHandler(m, m.getMasterFileSystem(),
        m.getServerManager(), desc, cluster.getConfiguration(), hRegionInfos,
        m.getCatalogTracker(), m.getAssignmentManager());
    handler1.process();
    for (int i = 0; i < 100; i++) {
      if (!TEST_UTIL.getHBaseAdmin().isTableAvailable(TABLENAME)) {
        Thread.sleep(200);
      }
    }
    assertTrue(TEST_UTIL.getHBaseAdmin().isTableEnabled(TABLENAME));

  }
  
  @Test (timeout=60000)
  public void testMasterRestartAfterEnablingNodeIsCreated() throws Exception {
    byte[] tableName = Bytes.toBytes("testMasterRestartAfterEnablingNodeIsCreated");
    final MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    final HRegionInfo[] hRegionInfos = new HRegionInfo[] { new HRegionInfo(desc.getName(), null,
        null) };
    CustomCreateTableHandler handler = new CustomCreateTableHandler(m, m.getMasterFileSystem(),
        m.getServerManager(), desc, cluster.getConfiguration(), hRegionInfos,
        m.getCatalogTracker(), m.getAssignmentManager());
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
        ServerManager sm, HTableDescriptor hTableDescriptor, Configuration conf,
        HRegionInfo[] newRegions, CatalogTracker ct, AssignmentManager am)
        throws NotAllMetaRegionsOnlineException, TableExistsException, IOException {
      super(server, fileSystemManager, sm, hTableDescriptor, conf, newRegions, ct, am);
    }

    @Override
    protected List<HRegionInfo> handleCreateHdfsRegions(Path tableRootDir, String tableName)
        throws IOException {
      if (throwException) {
        throw new IOException("Test throws exceptions.");
      }
      return super.handleCreateHdfsRegions(tableRootDir, tableName);
    }
  }
}
