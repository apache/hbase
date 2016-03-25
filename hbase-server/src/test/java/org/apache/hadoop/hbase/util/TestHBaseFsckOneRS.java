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

package org.apache.hadoop.hbase.util;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.hfile.TestHFile;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.SplitTransactionImpl;
import org.apache.hadoop.hbase.regionserver.SplitTransactionFactory;
import org.apache.hadoop.hbase.regionserver.TestEndToEndSplitTransaction;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.*;
import static org.junit.Assert.*;

@Category({MiscTests.class, LargeTests.class})
public class TestHBaseFsckOneRS extends BaseTestHBaseFsck {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        MasterSyncObserver.class.getName());

    conf.setInt("hbase.regionserver.handler.count", 2);
    conf.setInt("hbase.regionserver.metahandler.count", 30);

    conf.setInt("hbase.htable.threads.max", POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.max", 2 * POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.core", POOL_SIZE);
    conf.setInt("hbase.hbck.close.timeout", 2 * REGION_ONLINE_TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 8 * REGION_ONLINE_TIMEOUT);
    TEST_UTIL.startMiniCluster(1);

    tableExecutorService = new ThreadPoolExecutor(1, POOL_SIZE, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("testhbck"));

    hbfsckExecutorService = new ScheduledThreadPoolExecutor(POOL_SIZE);

    AssignmentManager assignmentManager =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    regionStates = assignmentManager.getRegionStates();

    connection = (ClusterConnection) TEST_UTIL.getConnection();

    admin = connection.getAdmin();
    admin.setBalancerRunning(false, true);

    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);
    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.NAMESPACE_TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    tableExecutorService.shutdown();
    hbfsckExecutorService.shutdown();
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() {
    EnvironmentEdgeManager.reset();
  }


  /**
   * This creates a clean table and confirms that the table is clean.
   */
  @Test(timeout=180000)
  public void testHBaseFsckClean() throws Exception {
    assertNoErrors(doFsck(conf, false));
    TableName table = TableName.valueOf("tableClean");
    try {
      HBaseFsck hbck = doFsck(conf, false);
      assertNoErrors(hbck);

      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // We created 1 table, should be fine
      hbck = doFsck(conf, false);
      assertNoErrors(hbck);
      assertEquals(0, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test thread pooling in the case where there are more regions than threads
   */
  @Test (timeout=180000)
  public void testHbckThreadpooling() throws Exception {
    TableName table =
        TableName.valueOf("tableDupeStartKey");
    try {
      // Create table with 4 regions
      setupTable(table);

      // limit number of threads to 1.
      Configuration newconf = new Configuration(conf);
      newconf.setInt("hbasefsck.numthreads", 1);
      assertNoErrors(doFsck(newconf, false));

      // We should pass without triggering a RejectedExecutionException
    } finally {
      cleanupTable(table);
    }
  }

  @Test (timeout=180000)
  public void testTableWithNoRegions() throws Exception {
    // We might end up with empty regions in a table
    // see also testNoHdfsTable()
    TableName table =
        TableName.valueOf(name.getMethodName());
    try {
      // create table with one region
      HTableDescriptor desc = new HTableDescriptor(table);
      HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
      desc.addFamily(hcd); // If a table has no CF's it doesn't get checked
      createTable(TEST_UTIL, desc, null);
      tbl = connection.getTable(table, tableExecutorService);

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW, false, false, true);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS });

      doFsck(conf, true);

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  @Test (timeout=180000)
  public void testHbckFixOrphanTable() throws Exception {
    TableName table = TableName.valueOf("tableInfo");
    FileSystem fs = null;
    Path tableinfo = null;
    try {
      setupTable(table);

      Path hbaseTableDir = FSUtils.getTableDir(
          FSUtils.getRootDir(conf), table);
      fs = hbaseTableDir.getFileSystem(conf);
      FileStatus status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      tableinfo = status.getPath();
      fs.rename(tableinfo, new Path("/.tableinfo"));

      //to report error if .tableinfo is missing.
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NO_TABLEINFO_FILE });

      // fix OrphanTable with default .tableinfo (htd not yet cached on master)
      hbck = doFsck(conf, true);
      assertNoErrors(hbck);
      status = null;
      status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      assertNotNull(status);

      HTableDescriptor htd = admin.getTableDescriptor(table);
      htd.setValue("NOT_DEFAULT", "true");
      admin.disableTable(table);
      admin.modifyTable(table, htd);
      admin.enableTable(table);
      fs.delete(status.getPath(), true);

      // fix OrphanTable with cache
      htd = admin.getTableDescriptor(table); // warms up cached htd on master
      hbck = doFsck(conf, true);
      assertNoErrors(hbck);
      status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      assertNotNull(status);
      htd = admin.getTableDescriptor(table);
      assertEquals(htd.getValue("NOT_DEFAULT"), "true");
    } finally {
      if (fs != null) {
        fs.rename(new Path("/.tableinfo"), tableinfo);
      }
      cleanupTable(table);
    }
  }

  @Test (timeout=180000)
  public void testReadOnlyProperty() throws Exception {
    HBaseFsck hbck = doFsck(conf, false);
    Assert.assertEquals("shouldIgnorePreCheckPermission", true,
        hbck.shouldIgnorePreCheckPermission());

    hbck = doFsck(conf, true);
    Assert.assertEquals("shouldIgnorePreCheckPermission", false,
        hbck.shouldIgnorePreCheckPermission());

    hbck = doFsck(conf, true);
    hbck.setIgnorePreCheckPermission(true);
    Assert.assertEquals("shouldIgnorePreCheckPermission", true,
        hbck.shouldIgnorePreCheckPermission());
  }

  /**
   * This creates and fixes a bad table where a region is completely contained
   * by another region, and there is a hole (sort of like a bad split)
   */
  @Test (timeout=180000)
  public void testOverlapAndOrphan() throws Exception {
    TableName table =
        TableName.valueOf("tableOverlapAndOrphan");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("B"), true,
          true, false, true, HRegionInfo.DEFAULT_REPLICA_ID);
      admin.enableTable(table);

      HRegionInfo hriOverlap =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);
      ServerName server = regionStates.getRegionServerOfRegion(hriOverlap);
      TEST_UTIL.assertRegionOnServer(hriOverlap, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
          new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.ORPHAN_HDFS_REGION, HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
              HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where a region overlaps two regions --
   * a start key contained in another region and its end key is contained in
   * yet another region.
   */
  @Test (timeout=180000)
  public void testCoveredStartKey() throws Exception {
    TableName table =
        TableName.valueOf("tableCoveredStartKey");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      HRegionInfo hriOverlap =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A2"), Bytes.toBytes("B2"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);
      ServerName server = regionStates.getRegionServerOfRegion(hriOverlap);
      TEST_UTIL.assertRegionOnServer(hriOverlap, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.OVERLAP_IN_REGION_CHAIN,
          HBaseFsck.ErrorReporter.ERROR_CODE.OVERLAP_IN_REGION_CHAIN });
      assertEquals(3, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf, false);
      assertErrors(hbck2, new HBaseFsck.ErrorReporter.ERROR_CODE[0]);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region -- hole in meta
   * and data missing in the fs.
   */
  @Test (timeout=180000)
  public void testRegionHole() throws Exception {
    TableName table =
        TableName.valueOf("tableRegionHole");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), true,
          true, true);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length - 2, countRows()); // lost a region so lost a row
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * The region is not deployed when the table is disabled.
   */
  @Test (timeout=180000)
  public void testRegionShouldNotBeDeployed() throws Exception {
    TableName table =
        TableName.valueOf("tableRegionShouldNotBeDeployed");
    try {
      LOG.info("Starting testRegionShouldNotBeDeployed.");
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      assertTrue(cluster.waitForActiveAndReadyMaster());


      byte[][] SPLIT_KEYS = new byte[][] { new byte[0], Bytes.toBytes("aaa"),
          Bytes.toBytes("bbb"), Bytes.toBytes("ccc"), Bytes.toBytes("ddd") };
      HTableDescriptor htdDisabled = new HTableDescriptor(table);
      htdDisabled.addFamily(new HColumnDescriptor(FAM));

      // Write the .tableinfo
      FSTableDescriptors fstd = new FSTableDescriptors(conf);
      fstd.createTableDescriptor(htdDisabled);
      List<HRegionInfo> disabledRegions =
          TEST_UTIL.createMultiRegionsInMeta(conf, htdDisabled, SPLIT_KEYS);

      // Let's just assign everything to first RS
      HRegionServer hrs = cluster.getRegionServer(0);

      // Create region files.
      admin.disableTable(table);
      admin.enableTable(table);

      // Disable the table and close its regions
      admin.disableTable(table);
      HRegionInfo region = disabledRegions.remove(0);
      byte[] regionName = region.getRegionName();

      // The region should not be assigned currently
      assertTrue(cluster.getServerWith(regionName) == -1);

      // Directly open a region on a region server.
      // If going through AM/ZK, the region won't be open.
      // Even it is opened, AM will close it which causes
      // flakiness of this test.
      HRegion r = HRegion.openHRegion(
          region, htdDisabled, hrs.getWAL(region), conf);
      hrs.addToOnlineRegions(r);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.SHOULD_NOT_BE_DEPLOYED });

      // fix this fault
      doFsck(conf, true);

      // check result
      assertNoErrors(doFsck(conf, false));
    } finally {
      admin.enableTable(table);
      cleanupTable(table);
    }
  }

  /**
   * This test makes sure that parallel instances of Hbck is disabled.
   *
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testParallelHbck() throws Exception {
    final ExecutorService service;
    final Future<HBaseFsck> hbck1,hbck2;

    class RunHbck implements Callable<HBaseFsck> {
      boolean fail = true;
      @Override
      public HBaseFsck call(){
        Configuration c = new Configuration(conf);
        c.setInt("hbase.hbck.lockfile.attempts", 1);
        // HBASE-13574 found that in HADOOP-2.6 and later, the create file would internally retry.
        // To avoid flakiness of the test, set low max wait time.
        c.setInt("hbase.hbck.lockfile.maxwaittime", 3);
        try{
          return doFsck(c, true); // Exclusive hbck only when fixing
        } catch(Exception e){
          if (e.getMessage().contains("Duplicate hbck")) {
            fail = false;
          }
        }
        // If we reach here, then an exception was caught
        if (fail) fail();
        return null;
      }
    }
    service = Executors.newFixedThreadPool(2);
    hbck1 = service.submit(new RunHbck());
    hbck2 = service.submit(new RunHbck());
    service.shutdown();
    //wait for 15 seconds, for both hbck calls finish
    service.awaitTermination(15, TimeUnit.SECONDS);
    HBaseFsck h1 = hbck1.get();
    HBaseFsck h2 = hbck2.get();
    // Make sure only one of the calls was successful
    assert(h1 == null || h2 == null);
    if (h1 != null) {
      assert(h1.getRetCode() >= 0);
    }
    if (h2 != null) {
      assert(h2.getRetCode() >= 0);
    }
  }

  /**
   * This test makes sure that with enough retries both parallel instances
   * of hbck will be completed successfully.
   *
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testParallelWithRetriesHbck() throws Exception {
    final ExecutorService service;
    final Future<HBaseFsck> hbck1,hbck2;

    // With the ExponentialBackoffPolicyWithLimit (starting with 200 milliseconds sleep time, and
    // max sleep time of 5 seconds), we can retry around 15 times within 80 seconds before bail out.
    //
    // Note: the reason to use 80 seconds is that in HADOOP-2.6 and later, the create file would
    // retry up to HdfsConstants.LEASE_SOFTLIMIT_PERIOD (60 seconds).  See HBASE-13574 for more
    // details.
    final int timeoutInSeconds = 80;
    final int sleepIntervalInMilliseconds = 200;
    final int maxSleepTimeInMilliseconds = 6000;
    final int maxRetryAttempts = 15;

    class RunHbck implements Callable<HBaseFsck>{

      @Override
      public HBaseFsck call() throws Exception {
        // Increase retry attempts to make sure the non-active hbck doesn't get starved
        Configuration c = new Configuration(conf);
        c.setInt("hbase.hbck.lockfile.maxwaittime", timeoutInSeconds);
        c.setInt("hbase.hbck.lockfile.attempt.sleep.interval", sleepIntervalInMilliseconds);
        c.setInt("hbase.hbck.lockfile.attempt.maxsleeptime", maxSleepTimeInMilliseconds);
        c.setInt("hbase.hbck.lockfile.attempts", maxRetryAttempts);
        return doFsck(c, false);
      }
    }

    service = Executors.newFixedThreadPool(2);
    hbck1 = service.submit(new RunHbck());
    hbck2 = service.submit(new RunHbck());
    service.shutdown();
    //wait for some time, for both hbck calls finish
    service.awaitTermination(timeoutInSeconds * 2, TimeUnit.SECONDS);
    HBaseFsck h1 = hbck1.get();
    HBaseFsck h2 = hbck2.get();
    // Both should be successful
    assertNotNull(h1);
    assertNotNull(h2);
    assert(h1.getRetCode() >= 0);
    assert(h2.getRetCode() >= 0);

  }

  @Test (timeout = 180000)
  public void testRegionBoundariesCheck() throws Exception {
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck); // no errors
    try {
      hbck.connect(); // need connection to have access to META
      hbck.checkRegionBoundaries();
    } catch (IllegalArgumentException e) {
      if (e.getMessage().endsWith("not a valid DFS filename.")) {
        fail("Table directory path is not valid." + e.getMessage());
      }
    } finally {
      hbck.close();
    }
  }

  /**
   * test region boundaries and make sure store file had been created.
   * @throws Exception
   */
  @Test(timeout = 180000)
  public void testRegionBoundariesCheckWithFlushTable() throws Exception {
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck); // no errors
    TableName table = TableName.valueOf("testRegionBoundariesCheckWithFlushTable");
    try {
      setupTable(table);
      admin.flush(table);
      hbck.connect(); // need connection to have access to META
      hbck.checkRegionBoundaries();
      assertNoErrors(hbck); // no errors
    } catch (IllegalArgumentException e) {
      if (e.getMessage().endsWith("not a valid DFS filename.")) {
        fail("Table directory path is not valid." + e.getMessage());
      }
    } finally {
      hbck.close();
    }
  }
  
  @Test (timeout=180000)
  public void testHbckAfterRegionMerge() throws Exception {
    TableName table = TableName.valueOf("testMergeRegionFilesInHdfs");
    Table meta = null;
    try {
      // disable CatalogJanitor
      TEST_UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      try(RegionLocator rl = connection.getRegionLocator(tbl.getName())) {
        // make sure data in regions, if in wal only there is no data loss
        admin.flush(table);
        HRegionInfo region1 = rl.getRegionLocation(Bytes.toBytes("A")).getRegionInfo();
        HRegionInfo region2 = rl.getRegionLocation(Bytes.toBytes("B")).getRegionInfo();

        int regionCountBeforeMerge = rl.getAllRegionLocations().size();

        assertNotEquals(region1, region2);

        // do a region merge
        admin.mergeRegions(region1.getEncodedNameAsBytes(), region2.getEncodedNameAsBytes(), false);

        // wait until region merged
        long timeout = System.currentTimeMillis() + 30 * 1000;
        while (true) {
          if (rl.getAllRegionLocations().size() < regionCountBeforeMerge) {
            break;
          } else if (System.currentTimeMillis() > timeout) {
            fail("Time out waiting on region " + region1.getEncodedName() + " and " + region2
                .getEncodedName() + " be merged");
          }
          Thread.sleep(10);
        }

        assertEquals(ROWKEYS.length, countRows());

        HBaseFsck hbck = doFsck(conf, false);
        assertNoErrors(hbck); // no errors
      }

    } finally {
      TEST_UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(true);
      cleanupTable(table);
      IOUtils.closeQuietly(meta);
    }
  }
  /**
   * This creates entries in hbase:meta with no hdfs data.  This should cleanly
   * remove the table.
   */
  @Test (timeout=180000)
  public void testNoHdfsTable() throws Exception {
    TableName table = TableName.valueOf("NoHdfsTable");
    setupTable(table);
    assertEquals(ROWKEYS.length, countRows());

    // make sure data in regions, if in wal only there is no data loss
    admin.flush(table);

    // Mess it up by deleting hdfs dirs
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes(""),
        Bytes.toBytes("A"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("C"),
        Bytes.toBytes(""), false, false, true); // don't rm meta

    // also remove the table directory in hdfs
    deleteTableDir(table);

    HBaseFsck hbck = doFsck(conf, false);
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS,
        HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS, HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS,
        HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS, HBaseFsck.ErrorReporter.ERROR_CODE.ORPHAN_TABLE_STATE, });
    // holes are separate from overlap groups
    assertEquals(0, hbck.getOverlapGroups(table).size());

    // fix hole
    doFsck(conf, true); // detect dangling regions and remove those

    // check that hole fixed
    assertNoErrors(doFsck(conf,false));
    assertFalse("Table " + table + " should have been deleted", admin.tableExists(table));
  }

  /**
   * when the hbase.version file missing, It is fix the fault.
   */
  @Test (timeout=180000)
  public void testNoVersionFile() throws Exception {
    // delete the hbase.version file
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path versionFile = new Path(rootDir, HConstants.VERSION_FILE_NAME);
    fs.delete(versionFile, true);

    // test
    HBaseFsck hbck = doFsck(conf, false);
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NO_VERSION_FILE });
    // fix hbase.version missing
    doFsck(conf, true);

    // no version file fixed
    assertNoErrors(doFsck(conf, false));
  }

  @Test (timeout=180000)
  public void testNoTableState() throws Exception {
    // delete the hbase.version file
    TableName table =
        TableName.valueOf("testNoTableState");
    try {
      setupTable(table);
      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);

      MetaTableAccessor.deleteTableState(TEST_UTIL.getConnection(), table);

      // test
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NO_TABLE_STATE });
      // fix table state missing
      doFsck(conf, true);

      assertNoErrors(doFsck(conf, false));
      assertTrue(TEST_UTIL.getHBaseAdmin().isTableEnabled(table));
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates two tables and mess both of them and fix them one by one
   */
  @Test (timeout=180000)
  public void testFixByTable() throws Exception {
    TableName table1 =
        TableName.valueOf("testFixByTable1");
    TableName table2 =
        TableName.valueOf("testFixByTable2");
    try {
      setupTable(table1);
      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table1);
      // Mess them up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), false, false, true); // don't rm meta

      setupTable(table2);
      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table2);
      // Mess them up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), false,
          false, true); // don't rm meta

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS, HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS });

      // fix hole in table 1
      doFsck(conf, true, table1);
      // check that hole in table 1 fixed
      assertNoErrors(doFsck(conf, false, table1));
      // check that hole in table 2 still there
      assertErrors(doFsck(conf, false, table2), new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS });

      // fix hole in table 2
      doFsck(conf, true, table2);
      // check that hole in both tables fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length - 2, countRows());
    } finally {
      cleanupTable(table1);
      cleanupTable(table2);
    }
  }
  /**
   * A split parent in meta, in hdfs, and not deployed
   */
  @Test (timeout=180000)
  public void testLingeringSplitParent() throws Exception {
    TableName table =
        TableName.valueOf("testLingeringSplitParent");
    Table meta = null;
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);

      HRegionLocation location;
      try(RegionLocator rl = connection.getRegionLocator(tbl.getName())) {
        location = rl.getRegionLocation(Bytes.toBytes("B"));
      }

      // Delete one region from meta, but not hdfs, unassign it.
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), true, true, false);

      // Create a new meta entry to fake it as a split parent.
      meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
      HRegionInfo hri = location.getRegionInfo();

      HRegionInfo a = new HRegionInfo(tbl.getName(),
          Bytes.toBytes("B"), Bytes.toBytes("BM"));
      HRegionInfo b = new HRegionInfo(tbl.getName(),
          Bytes.toBytes("BM"), Bytes.toBytes("C"));

      hri.setOffline(true);
      hri.setSplit(true);

      MetaTableAccessor.addRegionToMeta(meta, hri, a, b);
      meta.close();
      admin.flush(TableName.META_TABLE_NAME);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.LINGERING_SPLIT_PARENT, HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // regular repair cannot fix lingering split parent
      hbck = doFsck(conf, true);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.LINGERING_SPLIT_PARENT, HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      assertFalse(hbck.shouldRerun());
      hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.LINGERING_SPLIT_PARENT, HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // fix lingering split parent
      hbck = new HBaseFsck(conf, hbfsckExecutorService);
      hbck.connect();
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      hbck.setTimeLag(0);
      hbck.setFixSplitParents(true);
      hbck.onlineHbck();
      assertTrue(hbck.shouldRerun());
      hbck.close();

      Get get = new Get(hri.getRegionName());
      Result result = meta.get(get);
      assertTrue(result.getColumnCells(HConstants.CATALOG_FAMILY,
          HConstants.SPLITA_QUALIFIER).isEmpty());
      assertTrue(result.getColumnCells(HConstants.CATALOG_FAMILY,
          HConstants.SPLITB_QUALIFIER).isEmpty());
      admin.flush(TableName.META_TABLE_NAME);

      // fix other issues
      doFsck(conf, true);

      // check that all are fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
      IOUtils.closeQuietly(meta);
    }
  }

  /**
   * Tests that LINGERING_SPLIT_PARENT is not erroneously reported for
   * valid cases where the daughters are there.
   */
  @Test (timeout=180000)
  public void testValidLingeringSplitParent() throws Exception {
    TableName table =
        TableName.valueOf("testLingeringSplitParent");
    Table meta = null;
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);

      try(RegionLocator rl = connection.getRegionLocator(tbl.getName())) {
        HRegionLocation location = rl.getRegionLocation(Bytes.toBytes("B"));

        meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
        HRegionInfo hri = location.getRegionInfo();

        // do a regular split
        byte[] regionName = location.getRegionInfo().getRegionName();
        admin.splitRegion(location.getRegionInfo().getRegionName(), Bytes.toBytes("BM"));
        TestEndToEndSplitTransaction.blockUntilRegionSplit(conf, 60000, regionName, true);

        // TODO: fixHdfsHoles does not work against splits, since the parent dir lingers on
        // for some time until children references are deleted. HBCK erroneously sees this as
        // overlapping regions
        HBaseFsck hbck = doFsck(conf, true, true, false, false, false, true, true, true, false,
            false, false, null);
        assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {}); //no LINGERING_SPLIT_PARENT reported

        // assert that the split hbase:meta entry is still there.
        Get get = new Get(hri.getRegionName());
        Result result = meta.get(get);
        assertNotNull(result);
        assertNotNull(MetaTableAccessor.getHRegionInfo(result));

        assertEquals(ROWKEYS.length, countRows());

        // assert that we still have the split regions
        assertEquals(rl.getStartKeys().length, SPLITS.length + 1 + 1); //SPLITS + 1 is # regions
        // pre-split.
        assertNoErrors(doFsck(conf, false));
      }
    } finally {
      cleanupTable(table);
      IOUtils.closeQuietly(meta);
    }
  }

  /**
   * Split crashed after write to hbase:meta finished for the parent region, but
   * failed to write daughters (pre HBASE-7721 codebase)
   */
  @Test(timeout=75000)
  public void testSplitDaughtersNotInMeta() throws Exception {
    TableName table = TableName.valueOf("testSplitdaughtersNotInMeta");
    Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);

      try(RegionLocator rl = connection.getRegionLocator(tbl.getName())) {
        HRegionLocation location = rl.getRegionLocation(Bytes.toBytes("B"));

        HRegionInfo hri = location.getRegionInfo();

        // Disable CatalogJanitor to prevent it from cleaning up the parent region
        // after split.
        admin.enableCatalogJanitor(false);

        // do a regular split
        byte[] regionName = location.getRegionInfo().getRegionName();
        admin.splitRegion(location.getRegionInfo().getRegionName(), Bytes.toBytes("BM"));
        TestEndToEndSplitTransaction.blockUntilRegionSplit(conf, 60000, regionName, true);

        PairOfSameType<HRegionInfo> daughters = MetaTableAccessor.getDaughterRegions(
            meta.get(new Get(regionName)));

        // Delete daughter regions from meta, but not hdfs, unassign it.

        ServerName firstSN =
            rl.getRegionLocation(daughters.getFirst().getStartKey()).getServerName();
        ServerName secondSN =
            rl.getRegionLocation(daughters.getSecond().getStartKey()).getServerName();

        undeployRegion(connection, firstSN, daughters.getFirst());
        undeployRegion(connection, secondSN, daughters.getSecond());

        List<Delete> deletes = new ArrayList<>();
        deletes.add(new Delete(daughters.getFirst().getRegionName()));
        deletes.add(new Delete(daughters.getSecond().getRegionName()));
        meta.delete(deletes);

        // Remove daughters from regionStates
        RegionStates regionStates = TEST_UTIL.getMiniHBaseCluster().getMaster().
            getAssignmentManager().getRegionStates();
        regionStates.deleteRegion(daughters.getFirst());
        regionStates.deleteRegion(daughters.getSecond());

        HBaseFsck hbck = doFsck(conf, false);
        assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
            HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
            HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
            HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN }); //no LINGERING_SPLIT_PARENT

        // now fix it. The fix should not revert the region split, but add daughters to META
        hbck = doFsck(conf, true, true, false, false, false, false, false, false, false,
            false, false, null);
        assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
            HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
            HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
            HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });

        // assert that the split hbase:meta entry is still there.
        Get get = new Get(hri.getRegionName());
        Result result = meta.get(get);
        assertNotNull(result);
        assertNotNull(MetaTableAccessor.getHRegionInfo(result));

        assertEquals(ROWKEYS.length, countRows());

        // assert that we still have the split regions
        assertEquals(rl.getStartKeys().length, SPLITS.length + 1 + 1); //SPLITS + 1 is # regions
        // pre-split.
        assertNoErrors(doFsck(conf, false)); //should be fixed by now
      }
    } finally {
      admin.enableCatalogJanitor(true);
      meta.close();
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region which is the 1st region -- hole in
   * meta and data missing in the fs.
   */
  @Test(timeout=120000)
  public void testMissingFirstRegion() throws Exception {
    TableName table = TableName.valueOf("testMissingFirstRegion");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes(""), Bytes.toBytes("A"), true,
          true, true);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.FIRST_REGION_STARTKEY_NOT_EMPTY });
      // fix hole
      doFsck(conf, true);
      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region which is the 1st region -- hole in
   * meta and data missing in the fs.
   */
  @Test(timeout=120000)
  public void testRegionDeployedNotInHdfs() throws Exception {
    TableName table =
        TableName.valueOf("testSingleRegionDeployedNotInHdfs");
    try {
      setupTable(table);
      admin.flush(table);

      // Mess it up by deleting region dir
      deleteRegion(conf, tbl.getTableDescriptor(),
          HConstants.EMPTY_START_ROW, Bytes.toBytes("A"), false,
          false, true);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS });
      // fix hole
      doFsck(conf, true);
      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with missing last region -- hole in meta and data missing in
   * the fs.
   */
  @Test(timeout=120000)
  public void testMissingLastRegion() throws Exception {
    TableName table =
        TableName.valueOf("testMissingLastRegion");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("C"), Bytes.toBytes(""), true,
          true, true);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.LAST_REGION_ENDKEY_NOT_EMPTY });
      // fix hole
      doFsck(conf, true);
      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test -noHdfsChecking option can detect and fix assignments issue.
   */
  @Test (timeout=180000)
  public void testFixAssignmentsAndNoHdfsChecking() throws Exception {
    TableName table =
        TableName.valueOf("testFixAssignmentsAndNoHdfsChecking");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by closing a region
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("B"), true,
          false, false, false, HRegionInfo.DEFAULT_REPLICA_ID);

      // verify there is no other errors
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
          new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_DEPLOYED, HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // verify that noHdfsChecking report the same errors
      HBaseFsck fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck,
          new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_DEPLOYED, HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      fsck.close();

      // verify that fixAssignments works fine with noHdfsChecking
      fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixAssignments(true);
      fsck.onlineHbck();
      assertTrue(fsck.shouldRerun());
      fsck.onlineHbck();
      assertNoErrors(fsck);

      assertEquals(ROWKEYS.length, countRows());

      fsck.close();
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test -noHdfsChecking option can detect region is not in meta but deployed.
   * However, it can not fix it without checking Hdfs because we need to get
   * the region info from Hdfs in this case, then to patch the meta.
   */
  @Test (timeout=180000)
  public void testFixMetaNotWorkingWithNoHdfsChecking() throws Exception {
    TableName table =
        TableName.valueOf("testFixMetaNotWorkingWithNoHdfsChecking");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by deleting a region from the metadata
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
          Bytes.toBytes("B"), false, true, false, false, HRegionInfo.DEFAULT_REPLICA_ID);

      // verify there is no other errors
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
          new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META, HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // verify that noHdfsChecking report the same errors
      HBaseFsck fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck,
          new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META, HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      fsck.close();

      // verify that fixMeta doesn't work with noHdfsChecking
      fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixAssignments(true);
      fsck.setFixMeta(true);
      fsck.onlineHbck();
      assertFalse(fsck.shouldRerun());
      assertErrors(fsck,
          new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META, HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      fsck.close();

      // fix the cluster so other tests won't be impacted
      fsck = doFsck(conf, true);
      assertTrue(fsck.shouldRerun());
      fsck = doFsck(conf, true);
      assertNoErrors(fsck);
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test -fixHdfsHoles doesn't work with -noHdfsChecking option,
   * and -noHdfsChecking can't detect orphan Hdfs region.
   */
  @Test (timeout=180000)
  public void testFixHdfsHolesNotWorkingWithNoHdfsChecking() throws Exception {
    TableName table =
        TableName.valueOf("testFixHdfsHolesNotWorkingWithNoHdfsChecking");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("B"), true,
          true, false, true, HRegionInfo.DEFAULT_REPLICA_ID);
      admin.enableTable(table);

      HRegionInfo hriOverlap =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);
      ServerName server = regionStates.getRegionServerOfRegion(hriOverlap);
      TEST_UTIL.assertRegionOnServer(hriOverlap, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.ORPHAN_HDFS_REGION, HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that noHdfsChecking can't detect ORPHAN_HDFS_REGION
      HBaseFsck fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      fsck.close();

      // verify that fixHdfsHoles doesn't work with noHdfsChecking
      fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixHdfsHoles(true);
      fsck.setFixHdfsOverlaps(true);
      fsck.setFixHdfsOrphans(true);
      fsck.onlineHbck();
      assertFalse(fsck.shouldRerun());
      assertErrors(fsck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      fsck.close();
    } finally {
      if (admin.isTableDisabled(table)) {
        admin.enableTable(table);
      }
      cleanupTable(table);
    }
  }

  /**
   * This creates a table and then corrupts an hfile.  Hbck should quarantine the file.
   */
  @Test(timeout=180000)
  public void testQuarantineCorruptHFile() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());
      admin.flush(table); // flush is async.

      FileSystem fs = FileSystem.get(conf);
      Path hfile = getFlushedHFile(fs, table);

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);

      // create new corrupt file called deadbeef (valid hfile name)
      Path corrupt = new Path(hfile.getParent(), "deadbeef");
      TestHFile.truncateFile(fs, hfile, corrupt);
      LOG.info("Created corrupted file " + corrupt);
      HBaseFsck.debugLsr(conf, FSUtils.getRootDir(conf));

      // we cannot enable here because enable never finished due to the corrupt region.
      HBaseFsck res = HbckTestingUtil.doHFileQuarantine(conf, table);
      assertEquals(res.getRetCode(), 0);
      HFileCorruptionChecker hfcc = res.getHFilecorruptionChecker();
      assertEquals(hfcc.getHFilesChecked(), 5);
      assertEquals(hfcc.getCorrupted().size(), 1);
      assertEquals(hfcc.getFailures().size(), 0);
      assertEquals(hfcc.getQuarantined().size(), 1);
      assertEquals(hfcc.getMissing().size(), 0);

      // Its been fixed, verify that we can enable.
      admin.enableTable(table);
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed an hfile after the corruption checker learned about it.
   */
  @Test(timeout=180000)
  public void testQuarantineMissingHFile() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());

    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, hbfsckExecutorService) {
      @Override
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles)
          throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          AtomicBoolean attemptedFirstHFile = new AtomicBoolean(false);
          @Override
          protected void checkHFile(Path p) throws IOException {
            if (attemptedFirstHFile.compareAndSet(false, true)) {
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkHFile(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 4, 0, 0, 0, 1); // 4 attempted, but 1 missing.
    hbck.close();
  }

  /**
   * This creates and fixes a bad table with regions that has startkey == endkey
   */
  @Test (timeout=180000)
  public void testDegenerateRegions() throws Exception {
    TableName table = TableName.valueOf("tableDegenerateRegions");
    try {
      setupTable(table);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      HRegionInfo hriDupe =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriDupe);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriDupe);
      ServerName server = regionStates.getRegionServerOfRegion(hriDupe);
      TEST_UTIL.assertRegionOnServer(hriDupe, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf,false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.DEGENERATE_REGION, HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS,
          HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS });
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the degenerate region.
      doFsck(conf, true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test mission REGIONINFO_QUALIFIER in hbase:meta
   */
  @Test (timeout=180000)
  public void testMissingRegionInfoQualifier() throws Exception {
    Connection connection = ConnectionFactory.createConnection(conf);
    TableName table = TableName.valueOf("testMissingRegionInfoQualifier");
    try {
      setupTable(table);

      // Mess it up by removing the RegionInfo for one region.
      final List<Delete> deletes = new LinkedList<Delete>();
      Table meta = connection.getTable(TableName.META_TABLE_NAME, hbfsckExecutorService);
      MetaTableAccessor.fullScanRegions(connection, new MetaTableAccessor.Visitor() {

        @Override
        public boolean visit(Result rowResult) throws IOException {
          HRegionInfo hri = MetaTableAccessor.getHRegionInfo(rowResult);
          if (hri != null && !hri.getTable().isSystemTable()) {
            Delete delete = new Delete(rowResult.getRow());
            delete.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
            deletes.add(delete);
          }
          return true;
        }
      });
      meta.delete(deletes);

      // Mess it up by creating a fake hbase:meta entry with no associated RegionInfo
      meta.put(new Put(Bytes.toBytes(table + ",,1361911384013.810e28f59a57da91c66"))
          .addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
              Bytes.toBytes("node1:60020")));
      meta.put(new Put(Bytes.toBytes(table + ",,1361911384013.810e28f59a57da91c66"))
          .addColumn(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
              Bytes.toBytes(1362150791183L)));
      meta.close();

      HBaseFsck hbck = doFsck(conf, false);
      assertTrue(hbck.getErrors().getErrorList().contains(HBaseFsck.ErrorReporter.ERROR_CODE.EMPTY_META_CELL));

      // fix reference file
      hbck = doFsck(conf, true);

      // check that reference file fixed
      assertFalse(hbck.getErrors().getErrorList().contains(HBaseFsck.ErrorReporter.ERROR_CODE.EMPTY_META_CELL));
    } finally {
      cleanupTable(table);
    }
    connection.close();
  }

  /**
   * Test pluggable error reporter. It can be plugged in
   * from system property or configuration.
   */
  @Test (timeout=180000)
  public void testErrorReporter() throws Exception {
    try {
      MockErrorReporter.calledCount = 0;
      doFsck(conf, false);
      assertEquals(MockErrorReporter.calledCount, 0);

      conf.set("hbasefsck.errorreporter", MockErrorReporter.class.getName());
      doFsck(conf, false);
      assertTrue(MockErrorReporter.calledCount > 20);
    } finally {
      conf.set("hbasefsck.errorreporter",
          HBaseFsck.PrintingErrorReporter.class.getName());
      MockErrorReporter.calledCount = 0;
    }
  }

  @Test(timeout=180000)
  public void testCheckTableLocks() throws Exception {
    IncrementingEnvironmentEdge edge = new IncrementingEnvironmentEdge(0);
    EnvironmentEdgeManager.injectEdge(edge);
    // check no errors
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck);

    ServerName mockName = ServerName.valueOf("localhost", 60000, 1);
    final TableName tableName = TableName.valueOf("foo");

    // obtain one lock
    final TableLockManager tableLockManager =
        TableLockManager.createTableLockManager(conf, TEST_UTIL.getZooKeeperWatcher(), mockName);
    TableLockManager.TableLock
        writeLock = tableLockManager.writeLock(tableName, "testCheckTableLocks");
    writeLock.acquire();
    hbck = doFsck(conf, false);
    assertNoErrors(hbck); // should not have expired, no problems

    edge.incrementTime(conf.getLong(TableLockManager.TABLE_LOCK_EXPIRE_TIMEOUT,
        TableLockManager.DEFAULT_TABLE_LOCK_EXPIRE_TIMEOUT_MS)); // let table lock expire

    hbck = doFsck(conf, false);
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {HBaseFsck.ErrorReporter.ERROR_CODE.EXPIRED_TABLE_LOCK});

    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override
      public void run() {
        TableLockManager.TableLock
            readLock = tableLockManager.writeLock(tableName, "testCheckTableLocks");
        try {
          latch.countDown();
          readLock.acquire();
        } catch (IOException ex) {
          fail();
        } catch (IllegalStateException ex) {
          return; // expected, since this will be reaped under us.
        }
        fail("should not have come here");
      };
    }.start();

    latch.await(); // wait until thread starts
    Threads.sleep(300); // wait some more to ensure writeLock.acquire() is called

    hbck = doFsck(conf, false);
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
        HBaseFsck.ErrorReporter.ERROR_CODE.EXPIRED_TABLE_LOCK}); // still one expired, one not-expired

    edge.incrementTime(conf.getLong(TableLockManager.TABLE_LOCK_EXPIRE_TIMEOUT,
        TableLockManager.DEFAULT_TABLE_LOCK_EXPIRE_TIMEOUT_MS)); // let table lock expire

    hbck = doFsck(conf, false);
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {HBaseFsck.ErrorReporter.ERROR_CODE.EXPIRED_TABLE_LOCK,
        HBaseFsck.ErrorReporter.ERROR_CODE.EXPIRED_TABLE_LOCK}); // both are expired

    conf.setLong(TableLockManager.TABLE_LOCK_EXPIRE_TIMEOUT, 1);
    // reaping from ZKInterProcessWriteLock uses znode cTime,
    // which is not injectable through EnvironmentEdge

    Threads.sleep(10);
    hbck = doFsck(conf, true); // now fix both cases

    hbck = doFsck(conf, false);
    assertNoErrors(hbck);

    // ensure that locks are deleted
    writeLock = tableLockManager.writeLock(tableName, "should acquire without blocking");
    writeLock.acquire(); // this should not block.
    writeLock.release(); // release for clean state
    tableLockManager.tableDeleted(tableName);
  }

  @Test(timeout=180000)
  public void testCheckReplication() throws Exception {
    // check no errors
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck);
    
    // create peer
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf);
    Assert.assertEquals(0, replicationAdmin.getPeersCount());
    int zkPort = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT,
      HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey("127.0.0.1:" + zkPort + ":/hbase");
    replicationAdmin.addPeer("1", rpc, null);
    replicationAdmin.getPeersCount();
    Assert.assertEquals(1, replicationAdmin.getPeersCount());
    
    // create replicator
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "Test Hbase Fsck", connection);
    ReplicationQueues repQueues =
        ReplicationFactory.getReplicationQueues(zkw, conf, connection);
    repQueues.init("server1");
    // queues for current peer, no errors
    repQueues.addLog("1", "file1");
    repQueues.addLog("1-server2", "file1");
    Assert.assertEquals(2, repQueues.getAllQueues().size());
    hbck = doFsck(conf, false);
    assertNoErrors(hbck);
    
    // queues for removed peer
    repQueues.addLog("2", "file1");
    repQueues.addLog("2-server2", "file1");
    Assert.assertEquals(4, repQueues.getAllQueues().size());
    hbck = doFsck(conf, false);
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
        HBaseFsck.ErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE,
        HBaseFsck.ErrorReporter.ERROR_CODE.UNDELETED_REPLICATION_QUEUE });
    
    // fix the case
    hbck = doFsck(conf, true);
    hbck = doFsck(conf, false);
    assertNoErrors(hbck);
    // ensure only "2" is deleted
    Assert.assertEquals(2, repQueues.getAllQueues().size());
    Assert.assertNull(repQueues.getLogsInQueue("2"));
    Assert.assertNull(repQueues.getLogsInQueue("2-sever2"));
    
    replicationAdmin.removePeer("1");
    repQueues.removeAllQueues();
    zkw.close();
    replicationAdmin.close();
  }

  /**
   * This creates and fixes a bad table with a missing region -- hole in meta
   * and data present but .regioninfo missing (an orphan hdfs region)in the fs.
   */
  @Test(timeout=180000)
  public void testHDFSRegioninfoMissing() throws Exception {
    TableName table = TableName.valueOf("tableHDFSRegioninfoMissing");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), true,
        true, false, true, HRegionInfo.DEFAULT_REPLICA_ID);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
        new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.ORPHAN_HDFS_REGION,
          HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is missing meta and
   * not assigned to a region server.
   */
  @Test (timeout=180000)
  public void testNotInMetaOrDeployedHole() throws Exception {
    TableName table =
      TableName.valueOf("tableNotInMetaOrDeployedHole");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), true,
        true, false); // don't rm from fs
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
        new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(doFsck(conf, true),
        new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  @Test (timeout=180000)
  public void testCleanUpDaughtersNotInMetaAfterFailedSplit() throws Exception {
    TableName table = TableName.valueOf("testCleanUpDaughtersNotInMetaAfterFailedSplit");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
      createTable(TEST_UTIL, desc, null);

      tbl = connection.getTable(desc.getTableName());
      for (int i = 0; i < 5; i++) {
        Put p1 = new Put(("r" + i).getBytes());
        p1.addColumn(Bytes.toBytes("f"), "q1".getBytes(), "v".getBytes());
        tbl.put(p1);
      }
      admin.flush(desc.getTableName());
      List<HRegion> regions = cluster.getRegions(desc.getTableName());
      int serverWith = cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(serverWith);
      cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      SplitTransactionImpl st = (SplitTransactionImpl)
        new SplitTransactionFactory(TEST_UTIL.getConfiguration())
          .create(regions.get(0), Bytes.toBytes("r3"));
      st.prepare();
      st.stepsBeforePONR(regionServer, regionServer, false);
      AssignmentManager am = cluster.getMaster().getAssignmentManager();
      Map<String, RegionState> regionsInTransition = am.getRegionStates().getRegionsInTransition();
      for (RegionState state : regionsInTransition.values()) {
        am.regionOffline(state.getRegion());
      }
      Map<HRegionInfo, ServerName> regionsMap = new HashMap<HRegionInfo, ServerName>();
      regionsMap.put(regions.get(0).getRegionInfo(), regionServer.getServerName());
      am.assign(regionsMap);
      am.waitForAssignment(regions.get(0).getRegionInfo());
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
        HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
        HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(
        doFsck(conf, false, true, false, false, false, false, false, false, false, false, false,
          null),
        new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED });

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(5, countRows());
    } finally {
      if (tbl != null) {
        tbl.close();
        tbl = null;
      }
      cleanupTable(table);
    }
  }

  /**
   * This creates fixes a bad table with a hole in meta.
   */
  @Test (timeout=180000)
  public void testNotInMetaHole() throws Exception {
    TableName table =
      TableName.valueOf("tableNotInMetaHole");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), false,
        true, false); // don't rm from fs
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
        new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(doFsck(conf, true),
        new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is in meta but has
   * no deployment or data hdfs
   */
  @Test (timeout=180000)
  public void testNotInHdfs() throws Exception {
    TableName table =
      TableName.valueOf("tableNotInHdfs");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);

      // Mess it up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), false,
        false, true); // don't rm meta

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
        HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length - 2, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed an colfam dir before the corruption checker got to it.
   */
  // Disabled because fails sporadically.  Is this test right?  Timing-wise, there could be no
  // files in a column family on initial creation -- as suggested by Matteo.
  @Ignore
  @Test(timeout=180000)
  public void testQuarantineMissingFamdir() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, hbfsckExecutorService) {
      @Override
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles)
        throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          AtomicBoolean attemptedFirstHFile = new AtomicBoolean(false);
          @Override
          protected void checkColFamDir(Path p) throws IOException {
            if (attemptedFirstHFile.compareAndSet(false, true)) {
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkColFamDir(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 3, 0, 0, 0, 1);
    hbck.close();
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed a region dir before the corruption checker got to it.
   */
  @Test(timeout=180000)
  public void testQuarantineMissingRegionDir() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, hbfsckExecutorService) {
      @Override
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles)
        throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          AtomicBoolean attemptedFirstHFile = new AtomicBoolean(false);
          @Override
          protected void checkRegionDir(Path p) throws IOException {
            if (attemptedFirstHFile.compareAndSet(false, true)) {
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkRegionDir(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 3, 0, 0, 0, 1);
    hbck.close();
  }
}
