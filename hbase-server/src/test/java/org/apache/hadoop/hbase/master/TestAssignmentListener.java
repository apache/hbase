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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestAssignmentListener {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAssignmentListener.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignmentListener.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  static class DummyListener {
    protected AtomicInteger modified = new AtomicInteger(0);

    public void awaitModifications(int count) throws InterruptedException {
      while (!modified.compareAndSet(count, 0)) {
        Thread.sleep(100);
      }
    }
  }

  static class DummyAssignmentListener extends DummyListener implements AssignmentListener {
    private AtomicInteger closeCount = new AtomicInteger(0);
    private AtomicInteger openCount = new AtomicInteger(0);

    public DummyAssignmentListener() {
    }

    @Override
    public void regionOpened(final RegionInfo regionInfo, final ServerName serverName) {
      LOG.info("Assignment open region=" + regionInfo + " server=" + serverName);
      openCount.incrementAndGet();
      modified.incrementAndGet();
    }

    @Override
    public void regionClosed(final RegionInfo regionInfo) {
      LOG.info("Assignment close region=" + regionInfo);
      closeCount.incrementAndGet();
      modified.incrementAndGet();
    }

    public void reset() {
      openCount.set(0);
      closeCount.set(0);
    }

    public int getLoadCount() {
      return openCount.get();
    }

    public int getCloseCount() {
      return closeCount.get();
    }
  }

  static class DummyServerListener extends DummyListener implements ServerListener {
    private AtomicInteger removedCount = new AtomicInteger(0);
    private AtomicInteger addedCount = new AtomicInteger(0);

    public DummyServerListener() {
    }

    @Override
    public void serverAdded(final ServerName serverName) {
      LOG.info("Server added " + serverName);
      addedCount.incrementAndGet();
      modified.incrementAndGet();
    }

    @Override
    public void serverRemoved(final ServerName serverName) {
      LOG.info("Server removed " + serverName);
      removedCount.incrementAndGet();
      modified.incrementAndGet();
    }

    public void reset() {
      addedCount.set(0);
      removedCount.set(0);
    }

    public int getAddedCount() {
      return addedCount.get();
    }

    public int getRemovedCount() {
      return removedCount.get();
    }
  }

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testServerListener() throws IOException, InterruptedException {
    ServerManager serverManager = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();

    DummyServerListener listener = new DummyServerListener();
    serverManager.registerListener(listener);
    try {
      MiniHBaseCluster miniCluster = TEST_UTIL.getMiniHBaseCluster();

      // Start a new Region Server
      miniCluster.startRegionServer();
      listener.awaitModifications(1);
      assertEquals(1, listener.getAddedCount());
      assertEquals(0, listener.getRemovedCount());

      // Start another Region Server
      listener.reset();
      miniCluster.startRegionServer();
      listener.awaitModifications(1);
      assertEquals(1, listener.getAddedCount());
      assertEquals(0, listener.getRemovedCount());

      int nrs = miniCluster.getRegionServerThreads().size();

      // Stop a Region Server
      listener.reset();
      miniCluster.stopRegionServer(nrs - 1);
      listener.awaitModifications(1);
      assertEquals(0, listener.getAddedCount());
      assertEquals(1, listener.getRemovedCount());

      // Stop another Region Server
      listener.reset();
      miniCluster.stopRegionServer(nrs - 2);
      listener.awaitModifications(1);
      assertEquals(0, listener.getAddedCount());
      assertEquals(1, listener.getRemovedCount());
    } finally {
      serverManager.unregisterListener(listener);
    }
  }

  @Test
  public void testAssignmentListener() throws IOException, InterruptedException {
    AssignmentManager am = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    Admin admin = TEST_UTIL.getAdmin();

    DummyAssignmentListener listener = new DummyAssignmentListener();
    am.registerListener(listener);
    try {
      final TableName tableName = TableName.valueOf(name.getMethodName());
      final byte[] FAMILY = Bytes.toBytes("cf");

      // Create a new table, with a single region
      LOG.info("Create Table");
      TEST_UTIL.createTable(tableName, FAMILY);
      listener.awaitModifications(1);
      assertEquals(1, listener.getLoadCount());
      assertEquals(0, listener.getCloseCount());

      // Add some data
      Table table = TEST_UTIL.getConnection().getTable(tableName);
      try {
        for (int i = 0; i < 10; ++i) {
          byte[] key = Bytes.toBytes("row-" + i);
          Put put = new Put(key);
          put.addColumn(FAMILY, null, key);
          table.put(put);
        }
      } finally {
        table.close();
      }

      // Split the table in two
      LOG.info("Split Table");
      listener.reset();
      admin.split(tableName, Bytes.toBytes("row-3"));
      listener.awaitModifications(3);
      assertEquals(2, listener.getLoadCount());     // daughters added
      assertEquals(1, listener.getCloseCount());    // parent removed

      // Wait for the Regions to be mergeable
      MiniHBaseCluster miniCluster = TEST_UTIL.getMiniHBaseCluster();
      int mergeable = 0;
      while (mergeable < 2) {
        Thread.sleep(100);
        admin.majorCompact(tableName);
        mergeable = 0;
        for (JVMClusterUtil.RegionServerThread regionThread: miniCluster.getRegionServerThreads()) {
          for (Region region: regionThread.getRegionServer().getRegions(tableName)) {
            mergeable += ((HRegion)region).isMergeable() ? 1 : 0;
          }
        }
      }

      // Merge the two regions
      LOG.info("Merge Regions");
      listener.reset();
      List<RegionInfo> regions = admin.getRegions(tableName);
      assertEquals(2, regions.size());
      boolean sameServer = areAllRegionsLocatedOnSameServer(tableName);
      // If the regions are located by different server, we need to move
      // regions to same server before merging. So the expected modifications
      // will increaes to 5. (open + close)
      final int expectedModifications = sameServer ? 3 : 5;
      final int expectedLoadCount = sameServer ? 1 : 2;
      final int expectedCloseCount = sameServer ? 2 : 3;
      admin.mergeRegionsAsync(regions.get(0).getEncodedNameAsBytes(),
        regions.get(1).getEncodedNameAsBytes(), true);
      listener.awaitModifications(expectedModifications);
      assertEquals(1, admin.getRegions(tableName).size());
      assertEquals(expectedLoadCount, listener.getLoadCount());     // new merged region added
      assertEquals(expectedCloseCount, listener.getCloseCount());    // daughters removed

      // Delete the table
      LOG.info("Drop Table");
      listener.reset();
      TEST_UTIL.deleteTable(tableName);
      listener.awaitModifications(1);
      assertEquals(0, listener.getLoadCount());
      assertEquals(1, listener.getCloseCount());
    } finally {
      am.unregisterListener(listener);
    }
  }

  private boolean areAllRegionsLocatedOnSameServer(TableName TABLE_NAME) {
    MiniHBaseCluster miniCluster = TEST_UTIL.getMiniHBaseCluster();
    int serverCount = 0;
    for (JVMClusterUtil.RegionServerThread regionThread: miniCluster.getRegionServerThreads()) {
      if (!regionThread.getRegionServer().getRegions(TABLE_NAME).isEmpty()) {
        ++serverCount;
      }
      if (serverCount > 1) {
        return false;
      }
    }
    return serverCount == 1;
  }
}
