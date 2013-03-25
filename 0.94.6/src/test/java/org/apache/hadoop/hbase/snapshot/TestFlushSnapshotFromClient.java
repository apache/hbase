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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test creating/using/deleting snapshots from the client
 * <p>
 * This is an end-to-end test for the snapshot utility
 *
 * TODO This is essentially a clone of TestSnapshotFromClient.  This is worth refactoring this
 * because there will be a few more flavors of snapshots that need to run these tests.
 */
@Category(LargeTests.class)
public class TestFlushSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestFlushSnapshotFromClient.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);

  /**
   * Setup the config for the cluster
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // drop the number of attempts for the hbase admin
    conf.setInt("hbase.client.retries.number", 1);
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    // prevent aggressive region split
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);
    // and cleanup the archive directory
    try {
      UTIL.getTestFileSystem().delete(new Path(UTIL.getDefaultRootDirPath(), ".archive"), true);
    } catch (IOException e) {
      LOG.warn("Failure to delete archive directory", e);
    }
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  /**
   * Test simple flush snapshotting a table that is online
   * @throws Exception
   */
  @Test
  public void testFlushTableSnapshot() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    HTable table = new HTable(UTIL.getConfiguration(), TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM);

    // get the name of all the regionservers hosting the snapshotted table
    Set<String> snapshotServers = new HashSet<String>();
    List<RegionServerThread> servers = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads();
    for (RegionServerThread server : servers) {
      if (server.getRegionServer().getOnlineRegions(TABLE_NAME).size() > 0) {
        snapshotServers.add(server.getRegionServer().getServerName().toString());
      }
    }

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the enabled table
    String snapshotString = "offlineTableSnapshot";
    byte[] snapshot = Bytes.toBytes(snapshotString);
    admin.snapshot(snapshotString, STRING_TABLE_NAME, SnapshotDescription.Type.FLUSH);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertOneSnapshotThatMatches(admin,
      snapshot, TABLE_NAME);

    // make sure its a valid snapshot
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    LOG.debug("FS state after snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), TABLE_NAME, TEST_FAM, rootDir,
      admin, fs, false, new Path(rootDir, HConstants.HREGION_LOGDIR_NAME), snapshotServers);

    admin.deleteSnapshot(snapshot);
    snapshots = admin.listSnapshots();
    SnapshotTestingUtils.assertNoSnapshots(admin);
  }

  @Test
  public void testSnapshotFailsOnNonExistantTable() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    String tableName = "_not_a_table";

    // make sure the table doesn't exist
    boolean fail = false;
    do {
    try {
      admin.getTableDescriptor(Bytes.toBytes(tableName));
      fail = true;
          LOG.error("Table:" + tableName + " already exists, checking a new name");
      tableName = tableName+"!";
    } catch (TableNotFoundException e) {
      fail = false;
      }
    } while (fail);

    // snapshot the non-existant table
    try {
      admin.snapshot("fail", tableName, SnapshotDescription.Type.FLUSH);
      fail("Snapshot succeeded even though there is not table.");
    } catch (SnapshotCreationException e) {
      LOG.info("Correctly failed to snapshot a non-existant table:" + e.getMessage());
    }
  }

  @Test(timeout = 15000)
  public void testAsyncFlushSnapshot() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("asyncSnapshot")
        .setTable(STRING_TABLE_NAME).setType(SnapshotDescription.Type.FLUSH).build();

    // take the snapshot async
    admin.takeSnapshotAsync(snapshot);

    // constantly loop, looking for the snapshot to complete
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    SnapshotTestingUtils.waitForSnapshotToComplete(master, new HSnapshotDescription(snapshot), 200);
    LOG.info(" === Async Snapshot Completed ===");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    // make sure we get the snapshot
    SnapshotTestingUtils.assertOneSnapshotThatMatches(admin, snapshot);

    // test that we can delete the snapshot
    admin.deleteSnapshot(snapshot.getName());
    LOG.info(" === Async Snapshot Deleted ===");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    // make sure we don't have any snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    LOG.info(" === Async Snapshot Test Completed ===");

  }

  /**
   * Basic end-to-end test of simple-flush-based snapshots
   */
  @Test
  public void testFlushCreateListDestroy() throws Exception {
    LOG.debug("------- Starting Snapshot test -------------");
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // load the table so we have some data
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);
    // and wait until everything stabilizes
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    List<HRegion> onlineRegions = rs.getOnlineRegions(TABLE_NAME);
    for (HRegion region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }
    String snapshotName = "flushSnapshotCreateListDestroy";
    // test creating the snapshot
    admin.snapshot(snapshotName, STRING_TABLE_NAME, SnapshotDescription.Type.FLUSH);
    logFSTree(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));

    // make sure we only have 1 matching snapshot
    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertOneSnapshotThatMatches(admin,
      snapshotName, STRING_TABLE_NAME);

    // check the directory structure
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshots.get(0), rootDir);
    assertTrue(fs.exists(snapshotDir));
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(), snapshotDir, LOG);
    Path snapshotinfo = new Path(snapshotDir, SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
    assertTrue(fs.exists(snapshotinfo));

    // check the table info
    HTableDescriptor desc = FSTableDescriptors.getTableDescriptor(fs, rootDir, TABLE_NAME);
    HTableDescriptor snapshotDesc = FSTableDescriptors.getTableDescriptor(fs,
      SnapshotDescriptionUtils.getSnapshotsDir(rootDir), Bytes.toBytes(snapshotName));
    assertEquals(desc, snapshotDesc);

    // check the region snapshot for all the regions
    List<HRegionInfo> regions = admin.getTableRegions(TABLE_NAME);
    for (HRegionInfo info : regions) {
      String regionName = info.getEncodedName();
      Path regionDir = new Path(snapshotDir, regionName);
      HRegionInfo snapshotRegionInfo = HRegion.loadDotRegionInfoFileContent(fs, regionDir);
      assertEquals(info, snapshotRegionInfo);
      // check to make sure we have the family
      Path familyDir = new Path(regionDir, Bytes.toString(TEST_FAM));
      assertTrue(fs.exists(familyDir));
      // make sure we have some file references
      assertTrue(fs.listStatus(familyDir).length > 0);
    }

    // test that we can delete the snapshot
    admin.deleteSnapshot(snapshotName);
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // make sure we don't have any snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    LOG.debug("------- Flush-Snapshot Create List Destroy-------------");
  }

  /**
   * Demonstrate that we reject snapshot requests if there is a snapshot currently running.
   */
  @Test(timeout=60000)
  public void testConcurrentSnapshottingAttempts() throws IOException, InterruptedException {
    int ssNum = 10;
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // load the table so we have some data
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);
    // and wait until everything stabilizes
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    List<HRegion> onlineRegions = rs.getOnlineRegions(TABLE_NAME);
    for (HRegion region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }

    // build descriptions
    SnapshotDescription[] descs = new SnapshotDescription[ssNum];
    for (int i = 0; i < ssNum; i++) {
      SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
      builder.setTable(STRING_TABLE_NAME);
      builder.setName("ss"+i);
      builder.setType(SnapshotDescription.Type.FLUSH);
      descs[i] = builder.build();
    }

    final CountDownLatch toBeSubmitted = new CountDownLatch(ssNum);
    // We'll have one of these per thread
    class SSRunnable implements Runnable {
      SnapshotDescription ss;
      SSRunnable(SnapshotDescription ss) {
        this.ss = ss;
      }

      @Override
      public void run() {
        try {
          HBaseAdmin admin = UTIL.getHBaseAdmin();
          LOG.info("Submitting snapshot request: " + SnapshotDescriptionUtils.toString(ss));
          admin.takeSnapshotAsync(ss);
        } catch (Exception e) {
          LOG.info("Exception during snapshot request: " + SnapshotDescriptionUtils.toString(ss)
              + ".  This is ok, we expect some", e);
        }
        LOG.info("Submitted snapshot request: " + SnapshotDescriptionUtils.toString(ss));
        toBeSubmitted.countDown();
      }
    };

    // kick each off its own thread
    for (int i=0 ; i < ssNum; i++) {
      new Thread(new SSRunnable(descs[i])).start();
    }

    // wait until all have been submitted
    toBeSubmitted.await();

    // loop until all are done.
    while (true) {
      int doneCount = 0;
      for (SnapshotDescription ss : descs) {
        try {
          if (admin.isSnapshotFinished(ss)) {
            doneCount++;
          }
        } catch (Exception e) {
          LOG.warn("Got an exception when checking for snapshot " + ss.getName(), e);
          doneCount++;
        }
      }
      if (doneCount == descs.length) {
        break;
      }
      Thread.sleep(100);
    }

    // dump for debugging
    logFSTree(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));

    List<SnapshotDescription> taken = admin.listSnapshots();
    int takenSize = taken.size();
    LOG.info("Taken " + takenSize + " snapshots:  " + taken);
    assertTrue("We expect at least 1 request to be rejected because of we concurrently" +
        " issued many requests", takenSize < ssNum && takenSize > 0);
    // delete snapshots so subsequent tests are clean.
    for (SnapshotDescription ss : taken) {
      admin.deleteSnapshot(ss.getName());
    }
  }

  private void logFSTree(Path root) throws IOException {
    FSUtils.logFileSystemState(UTIL.getDFSCluster().getFileSystem(), root, LOG);
  }
}
