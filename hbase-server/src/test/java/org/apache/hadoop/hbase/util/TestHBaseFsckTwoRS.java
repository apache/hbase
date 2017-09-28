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

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertNoErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@Ignore // Until after HBASE-14614 goes in.
@Category({MiscTests.class, LargeTests.class})
public class TestHBaseFsckTwoRS extends BaseTestHBaseFsck {
  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        MasterSyncCoprocessor.class.getName());

    conf.setInt("hbase.regionserver.handler.count", 2);
    conf.setInt("hbase.regionserver.metahandler.count", 30);

    conf.setInt("hbase.htable.threads.max", POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.max", 2 * POOL_SIZE);
    conf.setInt("hbase.hbck.close.timeout", 2 * REGION_ONLINE_TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 8 * REGION_ONLINE_TIMEOUT);
    TEST_UTIL.startMiniCluster(2);

    tableExecutorService = new ThreadPoolExecutor(1, POOL_SIZE, 60, TimeUnit.SECONDS,
        new SynchronousQueue<>(), Threads.newDaemonThreadFactory("testhbck"));

    hbfsckExecutorService = new ScheduledThreadPoolExecutor(POOL_SIZE);

    assignmentManager = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
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

  @Test(timeout=180000)
  public void testFixAssignmentsWhenMETAinTransition() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    admin.unassign(RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName(), true);
    assignmentManager.offlineRegion(RegionInfoBuilder.FIRST_META_REGIONINFO);
    new MetaTableLocator().deleteMetaLocation(cluster.getMaster().getZooKeeper());
    assertFalse(regionStates.isRegionOnline(RegionInfoBuilder.FIRST_META_REGIONINFO));
    HBaseFsck hbck = doFsck(conf, true);
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.UNKNOWN, HBaseFsck.ErrorReporter.ERROR_CODE.NO_META_REGION,
        HBaseFsck.ErrorReporter.ERROR_CODE.NULL_META_REGION });
    assertNoErrors(doFsck(conf, false));
  }

  /**
   * This create and fixes a bad table with regions that have a duplicate
   * start key
   */
  @Test (timeout=180000)
  public void testDupeStartKey() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTable(tableName);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      RegionInfo hriDupe =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("A2"));
      TEST_UTIL.assignRegion(hriDupe);

      ServerName server = regionStates.getRegionServerOfRegion(hriDupe);
      TEST_UTIL.assertRegionOnServer(hriDupe, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS, HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS });
      assertEquals(2, hbck.getOverlapGroups(tableName).size());
      assertEquals(ROWKEYS.length, countRows()); // seems like the "bigger" region won.

      // fix the degenerate region.
      doFsck(conf, true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(tableName).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(tableName);
    }
  }

  /**
   * This create and fixes a bad table with regions that have a duplicate
   * start key
   */
  @Test (timeout=180000)
  public void testDupeRegion() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTable(tableName);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      RegionInfo hriDupe =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("B"));
      TEST_UTIL.assignRegion(hriDupe);

      ServerName server = regionStates.getRegionServerOfRegion(hriDupe);
      TEST_UTIL.assertRegionOnServer(hriDupe, server, REGION_ONLINE_TIMEOUT);

      // Yikes! The assignment manager can't tell between diff between two
      // different regions with the same start/endkeys since it doesn't
      // differentiate on ts/regionId!  We actually need to recheck
      // deployments!
      while (findDeployedHSI(getDeployedHRIs(admin), hriDupe) == null) {
        Thread.sleep(250);
      }

      LOG.debug("Finished assignment of dupe region");

      // TODO why is dupe region different from dupe start keys?
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS, HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS });
      assertEquals(2, hbck.getOverlapGroups(tableName).size());
      assertEquals(ROWKEYS.length, countRows()); // seems like the "bigger" region won.

      // fix the degenerate region.
      doFsck(conf, true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(tableName).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(tableName);
    }
  }


  /**
   * This creates and fixes a bad table where a region is completely contained
   * by another region.
   */
  @Test (timeout=180000)
  public void testContainedRegionOverlap() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTable(tableName);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      RegionInfo hriOverlap =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.assignRegion(hriOverlap);

      ServerName server = regionStates.getRegionServerOfRegion(hriOverlap);
      TEST_UTIL.assertRegionOnServer(hriOverlap, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.OVERLAP_IN_REGION_CHAIN });
      assertEquals(2, hbck.getOverlapGroups(tableName).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(tableName).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(tableName);
    }
  }

  /**
   * Test fixing lingering reference file.
   */
  @Test (timeout=180000)
  public void testLingeringReferenceFile() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTable(tableName);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating a fake reference file
      FileSystem fs = FileSystem.get(conf);
      Path tableDir= FSUtils.getTableDir(FSUtils.getRootDir(conf), tableName);
      Path regionDir = FSUtils.getRegionDirs(fs, tableDir).get(0);
      Path famDir = new Path(regionDir, FAM_STR);
      Path fakeReferenceFile = new Path(famDir, "fbce357483ceea.12144538");
      fs.create(fakeReferenceFile);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.LINGERING_REFERENCE_HFILE });
      // fix reference file
      doFsck(conf, true);
      // check that reference file fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(tableName);
    }
  }

  /**
   * Test fixing lingering HFileLinks.
   */
  @Test(timeout = 180000)
  public void testLingeringHFileLinks() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTable(tableName);

      FileSystem fs = FileSystem.get(conf);
      Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(conf), tableName);
      Path regionDir = FSUtils.getRegionDirs(fs, tableDir).get(0);
      String regionName = regionDir.getName();
      Path famDir = new Path(regionDir, FAM_STR);
      String HFILE_NAME = "01234567abcd";
      Path hFilePath = new Path(famDir, HFILE_NAME);

      // creating HFile
      HFileContext context = new HFileContextBuilder().withIncludesTags(false).build();
      HFile.Writer w =
          HFile.getWriterFactoryNoCache(conf).withPath(fs, hFilePath).withFileContext(context)
              .create();
      w.close();

      HFileLink.create(conf, fs, famDir, tableName, regionName, HFILE_NAME);

      // should report no error
      HBaseFsck hbck = doFsck(conf, false);
      assertNoErrors(hbck);

      // Delete linked file
      fs.delete(hFilePath, true);

      // Check without fix should show the error
      hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.LINGERING_HFILELINK });

      // Fixing the error
      hbck = doFsck(conf, true);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.LINGERING_HFILELINK });

      // Fix should sideline these files, thus preventing the error
      hbck = doFsck(conf, false);
      assertNoErrors(hbck);
    } finally {
      cleanupTable(tableName);
    }
  }

  @Test(timeout = 180000)
  public void testCorruptLinkDirectory() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTable(tableName);
      FileSystem fs = FileSystem.get(conf);

      Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(conf), tableName);
      Path regionDir = FSUtils.getRegionDirs(fs, tableDir).get(0);
      Path famDir = new Path(regionDir, FAM_STR);
      String regionName = regionDir.getName();
      String HFILE_NAME = "01234567abcd";
      String link = HFileLink.createHFileLinkName(tableName, regionName, HFILE_NAME);

      // should report no error
      HBaseFsck hbck = doFsck(conf, false);
      assertNoErrors(hbck);

      // creating a directory with file instead of the HFileLink file
      fs.mkdirs(new Path(famDir, link));
      fs.create(new Path(new Path(famDir, link), "somefile"));

      // Check without fix should show the error
      hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.LINGERING_HFILELINK });

      // Fixing the error
      hbck = doFsck(conf, true);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {
          HBaseFsck.ErrorReporter.ERROR_CODE.LINGERING_HFILELINK });

      // Fix should sideline these files, thus preventing the error
      hbck = doFsck(conf, false);
      assertNoErrors(hbck);
    } finally {
      cleanupTable(tableName);
    }
  }

  @Test (timeout=180000)
  public void testMetaOffline() throws Exception {
    // check no errors
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck);
    deleteMetaRegion(conf, true, false, false);
    hbck = doFsck(conf, false);
    // ERROR_CODE.UNKNOWN is coming because we reportError with a message for the hbase:meta
    // inconsistency and whether we will be fixing it or not.
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NO_META_REGION, HBaseFsck.ErrorReporter.ERROR_CODE.UNKNOWN });
    hbck = doFsck(conf, true);
    assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NO_META_REGION, HBaseFsck.ErrorReporter.ERROR_CODE.UNKNOWN });
    hbck = doFsck(conf, false);
    assertNoErrors(hbck);
  }

  /**
   * This creates and fixes a bad table where an overlap group of
   * 3 regions. Set HBaseFsck.maxMerge to 2 to trigger sideline overlapped
   * region. Mess around the meta data so that closeRegion/offlineRegion
   * throws exceptions.
   */
  @Test (timeout=180000)
  public void testSidelineOverlapRegion() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTable(tableName);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      RegionInfo hriOverlap1 =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("AB"));
      TEST_UTIL.assignRegion(hriOverlap1);

      RegionInfo hriOverlap2 =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("AB"), Bytes.toBytes("B"));
      TEST_UTIL.assignRegion(hriOverlap2);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS,
          HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS, HBaseFsck.ErrorReporter.ERROR_CODE.OVERLAP_IN_REGION_CHAIN});
      assertEquals(3, hbck.getOverlapGroups(tableName).size());
      assertEquals(ROWKEYS.length, countRows());

      // mess around the overlapped regions, to trigger NotServingRegionException
      Multimap<byte[], HBaseFsck.HbckInfo> overlapGroups = hbck.getOverlapGroups(tableName);
      ServerName serverName = null;
      byte[] regionName = null;
      for (HBaseFsck.HbckInfo hbi: overlapGroups.values()) {
        if ("A".equals(Bytes.toString(hbi.getStartKey()))
            && "B".equals(Bytes.toString(hbi.getEndKey()))) {
          regionName = hbi.getRegionName();

          // get an RS not serving the region to force bad assignment info in to META.
          int k = cluster.getServerWith(regionName);
          for (int i = 0; i < 3; i++) {
            if (i != k) {
              HRegionServer rs = cluster.getRegionServer(i);
              serverName = rs.getServerName();
              break;
            }
          }

          HBaseFsckRepair.closeRegionSilentlyAndWait(connection,
              cluster.getRegionServer(k).getServerName(), hbi.getHdfsHRI());
          admin.offline(regionName);
          break;
        }
      }

      assertNotNull(regionName);
      assertNotNull(serverName);
      try (Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService)) {
        Put put = new Put(regionName);
        put.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
            Bytes.toBytes(serverName.getAddress().toString()));
        meta.put(put);
      }

      // fix the problem.
      HBaseFsck fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setFixAssignments(true);
      fsck.setFixMeta(true);
      fsck.setFixHdfsHoles(true);
      fsck.setFixHdfsOverlaps(true);
      fsck.setFixHdfsOrphans(true);
      fsck.setFixVersionFile(true);
      fsck.setSidelineBigOverlaps(true);
      fsck.setMaxMerge(2);
      fsck.onlineHbck();
      fsck.close();

      // verify that overlaps are fixed, and there are less rows
      // since one region is sidelined.
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(tableName).size());
      assertTrue(ROWKEYS.length > countRows());
    } finally {
      cleanupTable(tableName);
    }
  }

  @Test(timeout=180000)
  public void testHBaseFsck() throws Exception {
    assertNoErrors(doFsck(conf, false));
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
    desc.addFamily(hcd); // If a tableName has no CF's it doesn't get checked
    createTable(TEST_UTIL, desc, null);

    // We created 1 table, should be fine
    assertNoErrors(doFsck(conf, false));

    // Now let's mess it up and change the assignment in hbase:meta to
    // point to a different region server
    Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(tableName+",,"));
    ResultScanner scanner = meta.getScanner(scan);
    RegionInfo hri = null;

    Result res = scanner.next();
    ServerName currServer =
        ProtobufUtil.parseServerNameFrom(res.getValue(HConstants.CATALOG_FAMILY,
            HConstants.SERVER_QUALIFIER));
    long startCode = Bytes.toLong(res.getValue(HConstants.CATALOG_FAMILY,
        HConstants.STARTCODE_QUALIFIER));

    for (JVMClusterUtil.RegionServerThread rs :
        TEST_UTIL.getHBaseCluster().getRegionServerThreads()) {

      ServerName sn = rs.getRegionServer().getServerName();

      // When we find a diff RS, change the assignment and break
      if (!currServer.getHostAndPort().equals(sn.getHostAndPort()) ||
          startCode != sn.getStartcode()) {
        Put put = new Put(res.getRow());
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
            Bytes.toBytes(sn.getHostAndPort()));
        put.addColumn(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
            Bytes.toBytes(sn.getStartcode()));
        meta.put(put);
        hri = MetaTableAccessor.getRegionInfo(res);
        break;
      }
    }

    // Try to fix the data
    assertErrors(doFsck(conf, true), new HBaseFsck.ErrorReporter.ERROR_CODE[]{
        HBaseFsck.ErrorReporter.ERROR_CODE.SERVER_DOES_NOT_MATCH_META});

    TEST_UTIL.getHBaseCluster().getMaster()
        .getAssignmentManager().waitForAssignment(hri);

    // Should be fixed now
    assertNoErrors(doFsck(conf, false));

    // comment needed - what is the purpose of this line
    Table t = connection.getTable(tableName, tableExecutorService);
    ResultScanner s = t.getScanner(new Scan());
    s.close();
    t.close();

    scanner.close();
    meta.close();
  }

  /**
   * This creates and fixes a bad table with a missing region -- hole in meta and data present but
   * .regioninfo missing (an orphan hdfs region)in the fs. At last we check every row was present
   * at the correct region.
   */
  @Test(timeout = 180000)
  public void testHDFSRegioninfoMissingAndCheckRegionBoundary() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTable(tableName);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(tableName);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), true,
        true, false, true, RegionInfo.DEFAULT_REPLICA_ID);
      admin.enableTable(tableName);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
        new HBaseFsck.ErrorReporter.ERROR_CODE[] {
            HBaseFsck.ErrorReporter.ERROR_CODE.ORPHAN_HDFS_REGION,
            HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
            HBaseFsck.ErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(tableName).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));

      // check data belong to the correct region,every scan should get one row.
      for (int i = 0; i < ROWKEYS.length; i++) {
        if (i != ROWKEYS.length - 1) {
          assertEquals(1, countRows(ROWKEYS[i], ROWKEYS[i + 1]));
        } else {
          assertEquals(1, countRows(ROWKEYS[i], null));
        }
      }

    } finally {
      cleanupTable(tableName);
    }
  }

  /**
   * Creates and fixes a bad table with a successful split that have a deployed
   * start and end keys
   */
  @Test (timeout=180000)
  public void testSplitAndDupeRegion() throws Exception {
    TableName table =
      TableName.valueOf("testSplitAndDupeRegion");
    Table meta = null;

    try {
      setupTable(table);

      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // No Catalog Janitor running
      admin.enableCatalogJanitor(false);
      meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
      HRegionLocation loc = this.connection.getRegionLocation(table, SPLITS[0], false);
      RegionInfo hriParent = loc.getRegionInfo();

      // Split Region A just before B
      this.connection.getAdmin().split(table, Bytes.toBytes("A@"));
      Thread.sleep(1000);

      // We need to make sure the parent region is not in a split state, so we put it in CLOSED state.
      regionStates.updateRegionState(hriParent, RegionState.State.CLOSED);
      TEST_UTIL.assignRegion(hriParent);
      MetaTableAccessor.addRegionToMeta(meta, hriParent);
      ServerName server = regionStates.getRegionServerOfRegion(hriParent);

      if (server != null)
        TEST_UTIL.assertRegionOnServer(hriParent, server, REGION_ONLINE_TIMEOUT);

      while (findDeployedHSI(getDeployedHRIs((HBaseAdmin) admin), hriParent) == null) {
        Thread.sleep(250);
      }

      LOG.debug("Finished assignment of parent region");

      // TODO why is dupe region different from dupe start keys?
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS,
        HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS, HBaseFsck.ErrorReporter.ERROR_CODE.OVERLAP_IN_REGION_CHAIN});
      assertEquals(3, hbck.getOverlapGroups(table).size());

      // fix the degenerate region.
      hbck = new HBaseFsck(conf, hbfsckExecutorService);
      hbck.setDisplayFullReport(); // i.e. -details
      hbck.setTimeLag(0);
      hbck.setFixHdfsOverlaps(true);
      hbck.setRemoveParents(true);
      hbck.setFixReferenceFiles(true);
      hbck.setFixHFileLinks(true);
      hbck.connect();
      hbck.onlineHbck();
      hbck.close();

      hbck = doFsck(conf, false);

      assertNoErrors(hbck);
      assertEquals(0, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }
}
