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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.io.hfile.TestHFile;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestEndToEndSplitTransaction;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.util.HBaseFsck.PrintingErrorReporter;
import org.apache.hadoop.hbase.util.HBaseFsck.TableInfo;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.common.collect.Multimap;

/**
 * This tests HBaseFsck's ability to detect reasons for inconsistent tables.
 */
@Category(LargeTests.class)
public class TestHBaseFsck {
  final static Log LOG = LogFactory.getLog(TestHBaseFsck.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static Configuration conf = TEST_UTIL.getConfiguration();
  private final static String FAM_STR = "fam";
  private final static byte[] FAM = Bytes.toBytes(FAM_STR);
  private final static int REGION_ONLINE_TIMEOUT = 800;

  // for the instance, reset every test run
  private HTable tbl;
  private final static byte[][] SPLITS = new byte[][] { Bytes.toBytes("A"),
    Bytes.toBytes("B"), Bytes.toBytes("C") };
  // one row per region.
  private final static byte[][] ROWKEYS= new byte[][] {
    Bytes.toBytes("00"), Bytes.toBytes("50"), Bytes.toBytes("A0"), Bytes.toBytes("A5"),
    Bytes.toBytes("B0"), Bytes.toBytes("B5"), Bytes.toBytes("C0"), Bytes.toBytes("C5") };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.distributed.log.splitting", false);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.setHDFSClientRetry(0);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHBaseFsck() throws Exception {
    assertNoErrors(doFsck(conf, false));
    String table = "tableBadMetaAssign"; 
    TEST_UTIL.createTable(Bytes.toBytes(table), FAM);

    // We created 1 table, should be fine
    assertNoErrors(doFsck(conf, false));

    // Now let's mess it up and change the assignment in .META. to
    // point to a different region server
    HTable meta = new HTable(conf, HTableDescriptor.META_TABLEDESC.getName());
    ResultScanner scanner = meta.getScanner(new Scan());

    resforloop:
    for (Result res : scanner) {
      long startCode = Bytes.toLong(res.getValue(HConstants.CATALOG_FAMILY,
          HConstants.STARTCODE_QUALIFIER));

      for (JVMClusterUtil.RegionServerThread rs :
          TEST_UTIL.getHBaseCluster().getRegionServerThreads()) {

        ServerName sn = rs.getRegionServer().getServerName();

        // When we find a diff RS, change the assignment and break
        if (startCode != sn.getStartcode()) {
          Put put = new Put(res.getRow());
          put.setWriteToWAL(false);
          put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
            Bytes.toBytes(sn.getHostAndPort()));
          put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
            Bytes.toBytes(sn.getStartcode()));
          meta.put(put);
          break resforloop;
        }
      }
    }

    // Try to fix the data
    assertErrors(doFsck(conf, true), new ERROR_CODE[]{
        ERROR_CODE.SERVER_DOES_NOT_MATCH_META});

    // fixing assignments require opening regions is not synchronous.  To make
    // the test pass consistently so for now we bake in some sleep to let it
    // finish.  1s seems sufficient.
    Thread.sleep(1000);

    // Should be fixed now
    assertNoErrors(doFsck(conf, false));

    // comment needed - what is the purpose of this line
    HTable t = new HTable(conf, Bytes.toBytes(table));
    ResultScanner s = t.getScanner(new Scan());
    s.close();
    t.close();

    scanner.close();
    meta.close();
  }

  /**
   * Create a new region in META.
   */
  private HRegionInfo createRegion(Configuration conf, final HTableDescriptor
      htd, byte[] startKey, byte[] endKey)
      throws IOException {
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    HRegionInfo hri = new HRegionInfo(htd.getName(), startKey, endKey);
    Put put = new Put(hri.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
    meta.put(put);
    return hri;
  }

  /**
   * Debugging method to dump the contents of meta.
   */
  private void dumpMeta(byte[] tableName) throws IOException {
    List<byte[]> metaRows = TEST_UTIL.getMetaTableRows(tableName);
    for (byte[] row : metaRows) {
      LOG.info(Bytes.toString(row));
    }
  }

  /**
   * This method is used to undeploy a region -- close it and attempt to
   * remove its state from the Master.
   */
  private void undeployRegion(HBaseAdmin admin, ServerName sn,
      HRegionInfo hri) throws IOException, InterruptedException {
    try {
      HBaseFsckRepair.closeRegionSilentlyAndWait(admin, sn, hri);
      admin.getMaster().offline(hri.getRegionName());
    } catch (IOException ioe) {
      LOG.warn("Got exception when attempting to offline region "
          + Bytes.toString(hri.getRegionName()), ioe);
    }
  }
  /**
   * Delete a region from assignments, meta, or completely from hdfs.
   * @param unassign if true unassign region if assigned
   * @param metaRow  if true remove region's row from META
   * @param hdfs if true remove region's dir in HDFS
   */
  private void deleteRegion(Configuration conf, final HTableDescriptor htd,
      byte[] startKey, byte[] endKey, boolean unassign, boolean metaRow,
      boolean hdfs) throws IOException, InterruptedException {
    deleteRegion(conf, htd, startKey, endKey, unassign, metaRow, hdfs, false);
  }

  /**
   * Delete a region from assignments, meta, or completely from hdfs.
   * @param unassign if true unassign region if assigned
   * @param metaRow  if true remove region's row from META
   * @param hdfs if true remove region's dir in HDFS
   * @param regionInfoOnly if true remove a region dir's .regioninfo file
   */
  private void deleteRegion(Configuration conf, final HTableDescriptor htd,
      byte[] startKey, byte[] endKey, boolean unassign, boolean metaRow,
      boolean hdfs, boolean regionInfoOnly) throws IOException, InterruptedException {
    LOG.info("** Before delete:");
    dumpMeta(htd.getName());

    Map<HRegionInfo, ServerName> hris = tbl.getRegionLocations();
    for (Entry<HRegionInfo, ServerName> e: hris.entrySet()) {
      HRegionInfo hri = e.getKey();
      ServerName hsa = e.getValue();
      if (Bytes.compareTo(hri.getStartKey(), startKey) == 0
          && Bytes.compareTo(hri.getEndKey(), endKey) == 0) {

        LOG.info("RegionName: " +hri.getRegionNameAsString());
        byte[] deleteRow = hri.getRegionName();

        if (unassign) {
          LOG.info("Undeploying region " + hri + " from server " + hsa);
          undeployRegion(new HBaseAdmin(conf), hsa, new HRegionInfo(hri));
        }

        if (regionInfoOnly) {
          LOG.info("deleting hdfs .regioninfo data: " + hri.toString() + hsa.toString());
          Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
          FileSystem fs = rootDir.getFileSystem(conf);
          Path p = new Path(rootDir + "/" + htd.getNameAsString(), hri.getEncodedName());
          Path hriPath = new Path(p, HRegion.REGIONINFO_FILE);
          fs.delete(hriPath, true);
        }

        if (hdfs) {
          LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
          Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
          FileSystem fs = rootDir.getFileSystem(conf);
          Path p = new Path(rootDir + "/" + htd.getNameAsString(), hri.getEncodedName());
          HBaseFsck.debugLsr(conf, p);
          boolean success = fs.delete(p, true);
          LOG.info("Deleted " + p + " sucessfully? " + success);
          HBaseFsck.debugLsr(conf, p);
        }

        if (metaRow) {
          HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
          Delete delete = new Delete(deleteRow);
          meta.delete(delete);
        }
      }
      LOG.info(hri.toString() + hsa.toString());
    }

    TEST_UTIL.getMetaTableRows(htd.getName());
    LOG.info("*** After delete:");
    dumpMeta(htd.getName());
  }

  /**
   * Setup a clean table before we start mucking with it.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  HTable setupTable(String tablename) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tablename);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
    desc.addFamily(hcd); // If a table has no CF's it doesn't get checked
    TEST_UTIL.getHBaseAdmin().createTable(desc, SPLITS);
    tbl = new HTable(TEST_UTIL.getConfiguration(), tablename);

    List<Put> puts = new ArrayList<Put>();
    for (byte[] row : ROWKEYS) {
      Put p = new Put(row);
      p.add(FAM, Bytes.toBytes("val"), row);
      puts.add(p);
    }
    tbl.put(puts);
    tbl.flushCommits();
    long endTime = System.currentTimeMillis() + 60000;
    while (!TEST_UTIL.getHBaseAdmin().isTableEnabled(tablename)) {
      try {
        if (System.currentTimeMillis() > endTime) {
          fail("Failed to enable table " + tablename + " after waiting for 60 sec");
        }
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted when waiting table " + tablename + " to be enabled");
      }
    }
    return tbl;
  }

  /**
   * Counts the number of row to verify data loss or non-dataloss.
   */
  int countRows() throws IOException {
     Scan s = new Scan();
     ResultScanner rs = tbl.getScanner(s);
     int i = 0;
     while(rs.next() !=null) {
       i++;
     }
     return i;
  }

  /**
   * delete table in preparation for next test
   * 
   * @param tablename
   * @throws IOException
   */
  void deleteTable(String tablename) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.getConnection().clearRegionCache();
    byte[] tbytes = Bytes.toBytes(tablename);
    if (admin.isTableEnabled(tbytes)) {
      admin.disableTableAsync(tbytes);
    }
    while (!admin.isTableDisabled(tbytes)) {
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted when trying to disable table " + tablename);
      }
    }
    admin.deleteTable(tbytes);
  }

  /**
   * This creates a clean table and confirms that the table is clean.
   */
  @Test
  public void testHBaseFsckClean() throws Exception {
    assertNoErrors(doFsck(conf, false));
    String table = "tableClean";
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
      deleteTable(table);
    }
  }

  /**
   * Test thread pooling in the case where there are more regions than threads
   */
  @Test
  public void testHbckThreadpooling() throws Exception {
    String table = "tableDupeStartKey";
    try {
      // Create table with 4 regions
      setupTable(table);

      // limit number of threads to 1.
      Configuration newconf = new Configuration(conf);
      newconf.setInt("hbasefsck.numthreads", 1);  
      assertNoErrors(doFsck(newconf, false));
      
      // We should pass without triggering a RejectedExecutionException
    } finally {
      deleteTable(table);
    }    
  }

  @Test
  public void testHbckFixOrphanTable() throws Exception {
    String table = "tableInfo";
    FileSystem fs = null;
    Path tableinfo = null;
    try {
      setupTable(table);
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();

      Path hbaseTableDir = new Path(conf.get(HConstants.HBASE_DIR) + "/" + table );
      fs = hbaseTableDir.getFileSystem(conf);
      FileStatus status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      tableinfo = status.getPath();
      fs.rename(tableinfo, new Path("/.tableinfo"));

      //to report error if .tableinfo is missing.
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NO_TABLEINFO_FILE });

      // fix OrphanTable with default .tableinfo (htd not yet cached on master)
      hbck = doFsck(conf, true);
      assertNoErrors(hbck);
      status = null;
      status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      assertNotNull(status);

      HTableDescriptor htd = admin.getTableDescriptor(table.getBytes());
      htd.setValue("NOT_DEFAULT", "true");
      admin.disableTable(table);
      admin.modifyTable(table.getBytes(), htd);
      admin.enableTable(table);
      fs.delete(status.getPath(), true);

      // fix OrphanTable with cache
      htd = admin.getTableDescriptor(table.getBytes()); // warms up cached htd on master
      hbck = doFsck(conf, true);
      assertNoErrors(hbck);
      status = null;
      status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      assertNotNull(status);
      htd = admin.getTableDescriptor(table.getBytes());
      assertEquals(htd.getValue("NOT_DEFAULT"), "true");
    } finally {
      fs.rename(new Path("/.tableinfo"), tableinfo);
      deleteTable(table);
    }
  }

  /**
   * This create and fixes a bad table with regions that have a duplicate
   * start key
   */
  @Test
  public void testDupeStartKey() throws Exception {
    String table = "tableDupeStartKey";
    try {
      setupTable(table);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      HRegionInfo hriDupe = createRegion(conf, tbl.getTableDescriptor(),
          Bytes.toBytes("A"), Bytes.toBytes("A2"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriDupe);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriDupe);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.DUPE_STARTKEYS,
            ERROR_CODE.DUPE_STARTKEYS});
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows()); // seems like the "bigger" region won.

      // fix the degenerate region.
      doFsck(conf,true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * Get region info from local cluster.
   */
  Map<ServerName, List<String>> getDeployedHRIs(HBaseAdmin admin)
    throws IOException {
    ClusterStatus status = admin.getMaster().getClusterStatus();
    Collection<ServerName> regionServers = status.getServers();
    Map<ServerName, List<String>> mm =
        new HashMap<ServerName, List<String>>();
    HConnection connection = admin.getConnection();
    for (ServerName hsi : regionServers) {
      HRegionInterface server =
        connection.getHRegionConnection(hsi.getHostname(), hsi.getPort());

      // list all online regions from this region server
      List<HRegionInfo> regions = server.getOnlineRegions();
      List<String> regionNames = new ArrayList<String>();
      for (HRegionInfo hri : regions) {
        regionNames.add(hri.getRegionNameAsString());
      }
      mm.put(hsi, regionNames);
    }
    return mm;
  }

  /**
   * Returns the HSI a region info is on.
   */
  ServerName findDeployedHSI(Map<ServerName, List<String>> mm, HRegionInfo hri) {
    for (Map.Entry<ServerName,List <String>> e : mm.entrySet()) {
      if (e.getValue().contains(hri.getRegionNameAsString())) {
        return e.getKey();
      }
    }
    return null;
  }

  /**
   * This test makes sure that parallel instances of Hbck is disabled.
   *
   * @throws Exception
   */
  @Test
  public void testParallelHbck() throws Exception {
    final ExecutorService service;
    final Future<HBaseFsck> hbck1,hbck2;

    class RunHbck implements Callable<HBaseFsck>{
      boolean fail = true;
      public HBaseFsck call(){
        try{
          return doFsck(conf, false);
        } catch(Exception e){
          if (e.getMessage().contains("Duplicate hbck")) {
            fail = false;
          } else {
            LOG.fatal("hbck failed.", e);
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
   * This create and fixes a bad table with regions that have a duplicate
   * start key
   */
  @Test
  public void testDupeRegion() throws Exception {
    String table = "tableDupeRegion";
    try {
      setupTable(table);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      HRegionInfo hriDupe = createRegion(conf, tbl.getTableDescriptor(),
          Bytes.toBytes("A"), Bytes.toBytes("B"));

      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriDupe);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriDupe);

      // Yikes! The assignment manager can't tell between diff between two
      // different regions with the same start/endkeys since it doesn't
      // differentiate on ts/regionId!  We actually need to recheck
      // deployments!
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      while (findDeployedHSI(getDeployedHRIs(admin), hriDupe) == null) {
        Thread.sleep(250);
      }

      LOG.debug("Finished assignment of dupe region");

      // TODO why is dupe region different from dupe start keys?
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.DUPE_STARTKEYS,
            ERROR_CODE.DUPE_STARTKEYS});
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows()); // seems like the "bigger" region won.

      // fix the degenerate region.
      doFsck(conf,true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with regions that has startkey == endkey
   */
  @Test
  public void testDegenerateRegions() throws Exception {
    String table = "tableDegenerateRegions";
    try {
      setupTable(table);
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      HRegionInfo hriDupe = createRegion(conf, tbl.getTableDescriptor(),
          Bytes.toBytes("B"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriDupe);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriDupe);

      HBaseFsck hbck = doFsck(conf,false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.DEGENERATE_REGION,
          ERROR_CODE.DUPE_STARTKEYS, ERROR_CODE.DUPE_STARTKEYS});
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the degenerate region.
      doFsck(conf,true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where a region is completely contained
   * by another region.
   */
  @Test
  public void testContainedRegionOverlap() throws Exception {
    String table = "tableContainedRegionOverlap";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      HRegionInfo hriOverlap = createRegion(conf, tbl.getTableDescriptor(),
          Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.OVERLAP_IN_REGION_CHAIN });
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
       deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where an overlap group of
   * 3 regions. Set HBaseFsck.maxMerge to 2 to trigger sideline overlapped
   * region. Mess around the meta data so that closeRegion/offlineRegion
   * throws exceptions.
   */
  @Test
  public void testSidelineOverlapRegion() throws Exception {
    String table = "testSidelineOverlapRegion";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      HMaster master = cluster.getMaster();
      HRegionInfo hriOverlap1 = createRegion(conf, tbl.getTableDescriptor(),
        Bytes.toBytes("A"), Bytes.toBytes("AB"));
      master.assignRegion(hriOverlap1);
      master.getAssignmentManager().waitForAssignment(hriOverlap1);
      HRegionInfo hriOverlap2 = createRegion(conf, tbl.getTableDescriptor(),
        Bytes.toBytes("AB"), Bytes.toBytes("B"));
      master.assignRegion(hriOverlap2);
      master.getAssignmentManager().waitForAssignment(hriOverlap2);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.DUPE_STARTKEYS,
        ERROR_CODE.DUPE_STARTKEYS, ERROR_CODE.OVERLAP_IN_REGION_CHAIN});
      assertEquals(3, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // mess around the overlapped regions, to trigger NotServingRegionException
      Multimap<byte[], HbckInfo> overlapGroups = hbck.getOverlapGroups(table);
      ServerName serverName = null;
      byte[] regionName = null;
      for (HbckInfo hbi: overlapGroups.values()) {
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

          HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
          HBaseFsckRepair.closeRegionSilentlyAndWait(admin,
            cluster.getRegionServer(k).getServerName(), hbi.getHdfsHRI());
          admin.unassign(regionName, true);
          break;
        }
      }

      assertNotNull(regionName);
      assertNotNull(serverName);
      HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
      Put put = new Put(regionName);
      put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
        Bytes.toBytes(serverName.getHostAndPort()));
      meta.put(put);

      // fix the problem.
      HBaseFsck fsck = new HBaseFsck(conf);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
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

      // verify that overlaps are fixed, and there are less rows
      // since one region is sidelined.
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertTrue(ROWKEYS.length > countRows());
    } finally {
       deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where a region is completely contained
   * by another region, and there is a hole (sort of like a bad split)
   */
  @Test
  public void testOverlapAndOrphan() throws Exception {
    String table = "tableOverlapAndOrphan";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
          Bytes.toBytes("B"), true, true, false, true);
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      HRegionInfo hriOverlap = createRegion(conf, tbl.getTableDescriptor(),
          Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.ORPHAN_HDFS_REGION, ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
       deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where a region overlaps two regions --
   * a start key contained in another region and its end key is contained in
   * yet another region.
   */
  @Test
  public void testCoveredStartKey() throws Exception {
    String table = "tableCoveredStartKey";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      HRegionInfo hriOverlap = createRegion(conf, tbl.getTableDescriptor(),
          Bytes.toBytes("A2"), Bytes.toBytes("B2"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.OVERLAP_IN_REGION_CHAIN,
          ERROR_CODE.OVERLAP_IN_REGION_CHAIN });
      assertEquals(3, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf, false);
      assertErrors(hbck2, new ERROR_CODE[0]);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region -- hole in meta
   * and data missing in the fs.
   */
  @Test
  public void testRegionHole() throws Exception {
    String table = "tableRegionHole";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), true, true, true);
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.HOLE_IN_REGION_CHAIN});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length - 2 , countRows()); // lost a region so lost a row
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region -- hole in meta
   * and data present but .regioinfino missing (an orphan hdfs region)in the fs.
   */
  @Test
  public void testHDFSRegioninfoMissing() throws Exception {
    String table = "tableHDFSRegioininfoMissing";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), true, true, false, true);
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.ORPHAN_HDFS_REGION,
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.HOLE_IN_REGION_CHAIN});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is missing meta and
   * not assigned to a region server.
   */
  @Test
  public void testNotInMetaOrDeployedHole() throws Exception {
    String table = "tableNotInMetaOrDeployedHole";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), true, true, false); // don't rm from fs
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(doFsck(conf, true) , new ERROR_CODE[] {
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates fixes a bad table with a hole in meta.
   */
  @Test
  public void testNotInMetaHole() throws Exception {
    String table = "tableNotInMetaHole";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), false, true, false); // don't rm from fs
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(doFsck(conf, true) , new ERROR_CODE[] {
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is in meta but has
   * no deployment or data hdfs
   */
  @Test
  public void testNotInHdfs() throws Exception {
    String table = "tableNotInHdfs";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in hlog only there is no data loss
      TEST_UTIL.getHBaseAdmin().flush(table);

      // Mess it up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), false, false, true); // don't rm meta

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.NOT_IN_HDFS});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length - 2, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates entries in META with no hdfs data.  This should cleanly
   * remove the table.
   */
  @Test
  public void testNoHdfsTable() throws Exception {
    String table = "NoHdfsTable";
    setupTable(table);
    assertEquals(ROWKEYS.length, countRows());

    // make sure data in regions, if in hlog only there is no data loss
    TEST_UTIL.getHBaseAdmin().flush(table);

    // Mess it up by leaving a giant hole in meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes(""),
        Bytes.toBytes("A"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("C"),
        Bytes.toBytes(""), false, false, true); // don't rm meta

    HBaseFsck hbck = doFsck(conf, false);
    assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.NOT_IN_HDFS,
        ERROR_CODE.NOT_IN_HDFS, ERROR_CODE.NOT_IN_HDFS,
        ERROR_CODE.NOT_IN_HDFS,});
    // holes are separate from overlap groups
    assertEquals(0, hbck.getOverlapGroups(table).size());

    // fix hole
    doFsck(conf, true); // in 0.92+, meta entries auto create regiondirs

    // check that hole fixed
    assertNoErrors(doFsck(conf,false));
    assertFalse("Table "+ table + " should have been deleted",
        TEST_UTIL.getHBaseAdmin().tableExists(table));
  }

  /**
   * when the hbase.version file missing, It is fix the fault.
   */
  @Test
  public void testNoVersionFile() throws Exception {
    // delete the hbase.version file
    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);
    Path versionFile = new Path(rootDir, HConstants.VERSION_FILE_NAME);
    fs.delete(versionFile, true);

    // test
    HBaseFsck hbck = doFsck(conf, false);
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NO_VERSION_FILE });
    // fix hbase.version missing
    doFsck(conf, true);

    // no version file fixed
    assertNoErrors(doFsck(conf, false));
  }

  /**
   * the region is not deployed when the table is disabled.
   */
  @Test
  public void testRegionShouldNotBeDeployed() throws Exception {
    String table = "tableRegionShouldNotBeDeployed";
    try {
      LOG.info("Starting testRegionShouldNotBeDeployed.");
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      assertTrue(cluster.waitForActiveAndReadyMaster());

      // Create a ZKW to use in the test
      ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);

      FileSystem filesystem = FileSystem.get(conf);
      Path rootdir = filesystem.makeQualified(new Path(conf
          .get(HConstants.HBASE_DIR)));

      byte[][] SPLIT_KEYS = new byte[][] { new byte[0], Bytes.toBytes("aaa"),
          Bytes.toBytes("bbb"), Bytes.toBytes("ccc"), Bytes.toBytes("ddd") };
      HTableDescriptor htdDisabled = new HTableDescriptor(Bytes.toBytes(table));
      htdDisabled.addFamily(new HColumnDescriptor(FAM));

      // Write the .tableinfo
      FSTableDescriptors
          .createTableDescriptor(filesystem, rootdir, htdDisabled);
      List<HRegionInfo> disabledRegions = TEST_UTIL.createMultiRegionsInMeta(
          TEST_UTIL.getConfiguration(), htdDisabled, SPLIT_KEYS);

      // Let's just assign everything to first RS
      HRegionServer hrs = cluster.getRegionServer(0);
      ServerName serverName = hrs.getServerName();

      // create region files.
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      // Region of disable table was opened on RS
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      HRegionInfo region = disabledRegions.remove(0);
      ZKAssign.createNodeOffline(zkw, region, serverName);
      hrs.openRegion(region);

      int iTimes = 0;
      while (true) {
        RegionTransitionData rtd = ZKAssign.getData(zkw,
            region.getEncodedName());
        if (rtd != null && rtd.getEventType() == EventType.RS_ZK_REGION_OPENED) {
          break;
        }
        Thread.sleep(100);
        iTimes++;
        if (iTimes >= REGION_ONLINE_TIMEOUT) {
          break;
        }
      }
      assertTrue(iTimes < REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.SHOULD_NOT_BE_DEPLOYED });

      // fix this fault
      doFsck(conf, true);

      // check result
      assertNoErrors(doFsck(conf, false));
    } finally {
      TEST_UTIL.getHBaseAdmin().enableTable(table);
      deleteTable(table);
    }
  }

  /**
   * This creates two tables and mess both of them and fix them one by one
   */
  @Test
  public void testFixByTable() throws Exception {
    String table1 = "testFixByTable1";
    String table2 = "testFixByTable2";
    try {
      setupTable(table1);
      // make sure data in regions, if in hlog only there is no data loss
      TEST_UTIL.getHBaseAdmin().flush(table1);
      // Mess them up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), false, false, true); // don't rm meta

      setupTable(table2);
      // make sure data in regions, if in hlog only there is no data loss
      TEST_UTIL.getHBaseAdmin().flush(table2);
      // Mess them up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), false, false, true); // don't rm meta

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.NOT_IN_HDFS, ERROR_CODE.NOT_IN_HDFS});

      // fix hole in table 1
      doFsck(conf, true, table1);
      // check that hole in table 1 fixed
      assertNoErrors(doFsck(conf, false, table1));
      // check that hole in table 2 still there
      assertErrors(doFsck(conf, false, table2),
        new ERROR_CODE[] {ERROR_CODE.NOT_IN_HDFS});

      // fix hole in table 2
      doFsck(conf, true, table2);
      // check that hole in both tables fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length - 2, countRows());
    } finally {
      deleteTable(table1);
      deleteTable(table2);
    }
  }
  /**
   * A split parent in meta, in hdfs, and not deployed
   */
  @Test
  public void testLingeringSplitParent() throws Exception {
    String table = "testLingeringSplitParent";
    HTable meta = null;
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in hlog only there is no data loss
      TEST_UTIL.getHBaseAdmin().flush(table);
      HRegionLocation location = tbl.getRegionLocation("B");

      // Delete one region from meta, but not hdfs, unassign it.
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), true, true, false);

      // Create a new meta entry to fake it as a split parent.
      meta = new HTable(conf, HTableDescriptor.META_TABLEDESC.getName());
      HRegionInfo hri = location.getRegionInfo();

      HRegionInfo a = new HRegionInfo(tbl.getTableName(),
        Bytes.toBytes("B"), Bytes.toBytes("BM"));
      HRegionInfo b = new HRegionInfo(tbl.getTableName(),
        Bytes.toBytes("BM"), Bytes.toBytes("C"));
      Put p = new Put(hri.getRegionName());
      hri.setOffline(true);
      hri.setSplit(true);
      p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
      p.add(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER,
        Writables.getBytes(a));
      p.add(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER,
        Writables.getBytes(b));
      meta.put(p);
      meta.flushCommits();
      TEST_UTIL.getHBaseAdmin().flush(HConstants.META_TABLE_NAME);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.LINGERING_SPLIT_PARENT, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // regular repair cannot fix lingering split parent
      hbck = doFsck(conf, true);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.LINGERING_SPLIT_PARENT, ERROR_CODE.HOLE_IN_REGION_CHAIN});
      assertFalse(hbck.shouldRerun());
      hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.LINGERING_SPLIT_PARENT, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // fix lingering split parent
      hbck = new HBaseFsck(conf);
      hbck.connect();
      hbck.setDisplayFullReport(); // i.e. -details
      hbck.setTimeLag(0);
      hbck.setFixSplitParents(true);
      hbck.onlineHbck();
      assertTrue(hbck.shouldRerun());

      Get get = new Get(hri.getRegionName());
      Result result = meta.get(get);
      assertTrue(result.getColumn(HConstants.CATALOG_FAMILY,
        HConstants.SPLITA_QUALIFIER).isEmpty());
      assertTrue(result.getColumn(HConstants.CATALOG_FAMILY,
        HConstants.SPLITB_QUALIFIER).isEmpty());
      TEST_UTIL.getHBaseAdmin().flush(HConstants.META_TABLE_NAME);

      // fix other issues
      doFsck(conf, true);

      // check that all are fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
      IOUtils.closeQuietly(meta);
    }
  }

  /**
   * Tests that LINGERING_SPLIT_PARENT is not erroneously reported for
   * valid cases where the daughters are there.
   */
  @Test
  public void testValidLingeringSplitParent() throws Exception {
    String table = "testLingeringSplitParent";
    HTable meta = null;
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in hlog only there is no data loss
      TEST_UTIL.getHBaseAdmin().flush(table);
      HRegionLocation location = tbl.getRegionLocation("B");

      meta = new HTable(conf, HTableDescriptor.META_TABLEDESC.getName());
      HRegionInfo hri = location.getRegionInfo();

      // do a regular split
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      byte[] regionName = location.getRegionInfo().getRegionName();
      admin.split(location.getRegionInfo().getRegionName(), Bytes.toBytes("BM"));
      TestEndToEndSplitTransaction.blockUntilRegionSplit(
          TEST_UTIL.getConfiguration(), 60000, regionName, true);

      // TODO: fixHdfsHoles does not work against splits, since the parent dir lingers on
      // for some time until children references are deleted. HBCK erroneously sees this as
      // overlapping regions
      HBaseFsck hbck = doFsck(conf, true, true, false, false, false, true, true, true, null);
      assertErrors(hbck, new ERROR_CODE[] {}); //no LINGERING_SPLIT_PARENT reported

      // assert that the split META entry is still there.
      Get get = new Get(hri.getRegionName());
      Result result = meta.get(get);
      assertNotNull(result);
      assertNotNull(MetaReader.parseCatalogResult(result).getFirst());

      assertEquals(ROWKEYS.length, countRows());

      // assert that we still have the split regions
      assertEquals(tbl.getStartKeys().length, SPLITS.length + 1 + 1); //SPLITS + 1 is # regions pre-split.
      assertNoErrors(doFsck(conf, false));
    } finally {
      deleteTable(table);
      IOUtils.closeQuietly(meta);
    }
  }

  /**
   * Split crashed after write to META finished for the parent region, but
   * failed to write daughters (pre HBASE-7721 codebase)
   */
  @Test(timeout=75000)
  public void testSplitDaughtersNotInMeta() throws Exception {
    String table = "testSplitdaughtersNotInMeta";
    HTable meta = null;
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in hlog only there is no data loss
      TEST_UTIL.getHBaseAdmin().flush(table);
      HRegionLocation location = tbl.getRegionLocation("B");

      meta = new HTable(conf, HTableDescriptor.META_TABLEDESC.getName());
      HRegionInfo hri = location.getRegionInfo();

      // do a regular split
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      byte[] regionName = location.getRegionInfo().getRegionName();
      admin.split(location.getRegionInfo().getRegionName(), Bytes.toBytes("BM"));
      TestEndToEndSplitTransaction.blockUntilRegionSplit(
          TEST_UTIL.getConfiguration(), 60000, regionName, true);

      PairOfSameType<HRegionInfo> daughters = MetaReader.getDaughterRegions(meta.get(new Get(regionName)));

      // Delete daughter regions from meta, but not hdfs, unassign it.
      Map<HRegionInfo, ServerName> hris = tbl.getRegionLocations();
      undeployRegion(admin, hris.get(daughters.getFirst()), daughters.getFirst());
      undeployRegion(admin, hris.get(daughters.getSecond()), daughters.getSecond());

      meta.delete(new Delete(daughters.getFirst().getRegionName()));
      meta.delete(new Delete(daughters.getSecond().getRegionName()));
      meta.flushCommits();

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN}); //no LINGERING_SPLIT_PARENT

      // now fix it. The fix should not revert the region split, but add daughters to META
      hbck = doFsck(conf, true, true, false, false, false, false, false, false, null);
      assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // assert that the split META entry is still there.
      Get get = new Get(hri.getRegionName());
      Result result = meta.get(get);
      assertNotNull(result);
      assertNotNull(MetaReader.parseCatalogResult(result).getFirst());

      assertEquals(ROWKEYS.length, countRows());

      // assert that we still have the split regions
      assertEquals(tbl.getStartKeys().length, SPLITS.length + 1 + 1); //SPLITS + 1 is # regions pre-split.
      assertNoErrors(doFsck(conf, false)); //should be fixed by now
    } finally {
      deleteTable(table);
      IOUtils.closeQuietly(meta);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region which is the 1st region -- hole in
   * meta and data missing in the fs.
   */
  @Test
  public void testMissingFirstRegion() throws Exception {
    String table = "testMissingFirstRegion";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes(""), Bytes.toBytes("A"), true,
          true, true);
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.FIRST_REGION_STARTKEY_NOT_EMPTY });
      // fix hole
      doFsck(conf, true);
      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      deleteTable(table);
    }
  }
  
  /**
   * This creates and fixes a bad table with missing last region -- hole in meta and data missing in
   * the fs.
   */
  @Test
  public void testMissingLastRegion() throws Exception {
    String table = "testMissingLastRegion";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("C"), Bytes.toBytes(""), true,
          true, true);
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.LAST_REGION_ENDKEY_NOT_EMPTY });
      // fix hole
      doFsck(conf, true);
      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      deleteTable(table);
    }
  }

  /**
   * Test -noHdfsChecking option can detect and fix assignments issue.
   */
  @Test
  public void testFixAssignmentsAndNoHdfsChecking() throws Exception {
    String table = "testFixAssignmentsAndNoHdfsChecking";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by closing a region
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), true, false, false, false);

      // verify there is no other errors
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.NOT_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that noHdfsChecking report the same errors
      HBaseFsck fsck = new HBaseFsck(conf);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck, new ERROR_CODE[] {
        ERROR_CODE.NOT_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that fixAssignments works fine with noHdfsChecking
      fsck = new HBaseFsck(conf);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixAssignments(true);
      fsck.onlineHbck();
      assertTrue(fsck.shouldRerun());
      fsck.onlineHbck();
      assertNoErrors(fsck);

      assertEquals(ROWKEYS.length, countRows());
    } finally {
      deleteTable(table);
    }
  }

  /**
   * Test -noHdfsChecking option can detect region is not in meta but deployed.
   * However, it can not fix it without checking Hdfs because we need to get
   * the region info from Hdfs in this case, then to patch the meta.
   */
  @Test
  public void testFixMetaNotWorkingWithNoHdfsChecking() throws Exception {
    String table = "testFixMetaNotWorkingWithNoHdfsChecking";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by deleting a region from the metadata
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), false, true, false, false);

      // verify there is no other errors
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.NOT_IN_META, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that noHdfsChecking report the same errors
      HBaseFsck fsck = new HBaseFsck(conf);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck, new ERROR_CODE[] {
        ERROR_CODE.NOT_IN_META, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that fixMeta doesn't work with noHdfsChecking
      fsck = new HBaseFsck(conf);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixAssignments(true);
      fsck.setFixMeta(true);
      fsck.onlineHbck();
      assertFalse(fsck.shouldRerun());
      assertErrors(fsck, new ERROR_CODE[] {
        ERROR_CODE.NOT_IN_META, ERROR_CODE.HOLE_IN_REGION_CHAIN});
    } finally {
      deleteTable(table);
    }
  }

  /**
   * Test -fixHdfsHoles doesn't work with -noHdfsChecking option,
   * and -noHdfsChecking can't detect orphan Hdfs region.
   */
  @Test
  public void testFixHdfsHolesNotWorkingWithNoHdfsChecking() throws Exception {
    String table = "testFixHdfsHolesNotWorkingWithNoHdfsChecking";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), true, true, false, true);
      TEST_UTIL.getHBaseAdmin().enableTable(table);

      HRegionInfo hriOverlap = createRegion(conf, tbl.getTableDescriptor(),
        Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
        .waitForAssignment(hriOverlap);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.ORPHAN_HDFS_REGION, ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
        ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that noHdfsChecking can't detect ORPHAN_HDFS_REGION
      HBaseFsck fsck = new HBaseFsck(conf);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck, new ERROR_CODE[] {
        ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that fixHdfsHoles doesn't work with noHdfsChecking
      fsck = new HBaseFsck(conf);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixHdfsHoles(true);
      fsck.setFixHdfsOverlaps(true);
      fsck.setFixHdfsOrphans(true);
      fsck.onlineHbck();
      assertFalse(fsck.shouldRerun());
      assertErrors(fsck, new ERROR_CODE[] {
        ERROR_CODE.HOLE_IN_REGION_CHAIN});
    } finally {
      if (TEST_UTIL.getHBaseAdmin().isTableDisabled(table)) {
        TEST_UTIL.getHBaseAdmin().enableTable(table);
      }
      deleteTable(table);
    }
  }

  /**
   * We don't have an easy way to verify that a flush completed, so we loop until we find a
   * legitimate hfile and return it.
   * @param fs
   * @param table
   * @return Path of a flushed hfile.
   * @throws IOException
   */
  Path getFlushedHFile(FileSystem fs, String table) throws IOException {
    Path tableDir= FSUtils.getTablePath(FSUtils.getRootDir(conf), table);
    Path regionDir = FSUtils.getRegionDirs(fs, tableDir).get(0);
    Path famDir = new Path(regionDir, FAM_STR);

    // keep doing this until we get a legit hfile
    while (true) {
      FileStatus[] hfFss = fs.listStatus(famDir);
      if (hfFss.length == 0) {
        continue;
      }
      for (FileStatus hfs : hfFss) {
        if (!hfs.isDir()) {
          return hfs.getPath();
        }
      }
    }
  }

  /**
   * This creates a table and then corrupts an hfile.  Hbck should quarantine the file.
   */
  @Test(timeout=180000)
  public void testQuarantineCorruptHFile() throws Exception {
    String table = name.getMethodName();
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());
      TEST_UTIL.getHBaseAdmin().flush(table); // flush is async.

      FileSystem fs = FileSystem.get(conf);
      Path hfile = getFlushedHFile(fs, table);

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      TEST_UTIL.getHBaseAdmin().disableTable(table);

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
      TEST_UTIL.getHBaseAdmin().enableTable(table);
    } finally {
      deleteTable(table);
    }
  }

  /**
  * Test that use this should have a timeout, because this method could potentially wait forever.
  */
  private void doQuarantineTest(String table, HBaseFsck hbck, int check, int corrupt, int fail,
      int quar, int missing) throws Exception {
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());
      TEST_UTIL.getHBaseAdmin().flush(table); // flush is async.

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      TEST_UTIL.getHBaseAdmin().disableTable(table);
      
      String[] args = {"-sidelineCorruptHFiles", "-repairHoles", "-ignorePreCheckPermission", table};
      ExecutorService exec = new ScheduledThreadPoolExecutor(10);
      HBaseFsck res = hbck.exec(exec, args);

      HFileCorruptionChecker hfcc = res.getHFilecorruptionChecker();
      assertEquals(hfcc.getHFilesChecked(), check);
      assertEquals(hfcc.getCorrupted().size(), corrupt);
      assertEquals(hfcc.getFailures().size(), fail);
      assertEquals(hfcc.getQuarantined().size(), quar);
      assertEquals(hfcc.getMissing().size(), missing);

      // its been fixed, verify that we can enable
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      admin.enableTableAsync(table);
      while (!admin.isTableEnabled(table)) {
        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          e.printStackTrace();
          fail("Interrupted when trying to enable table " + table);
        }
      }
    } finally {
      deleteTable(table);
    }
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed an hfile after the corruption checker learned about it.
   */
  @Test(timeout=180000)
  public void testQuarantineMissingHFile() throws Exception {
    String table = name.getMethodName();
    ExecutorService exec = new ScheduledThreadPoolExecutor(10);
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, exec) {
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles) throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          boolean attemptedFirstHFile = false;
          protected void checkHFile(Path p) throws IOException {
            if (!attemptedFirstHFile) {
              attemptedFirstHFile = true;
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkHFile(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 4, 0, 0, 0, 1); // 4 attempted, but 1 missing.
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed an colfam dir before the corruption checker got to it.
   */
  @Test(timeout=180000)
  public void testQuarantineMissingFamdir() throws Exception {
    String table = name.getMethodName();
    ExecutorService exec = new ScheduledThreadPoolExecutor(10);
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, exec) {
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles) throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          boolean attemptedFirstFamDir = false;
          protected void checkColFamDir(Path p) throws IOException {
            if (!attemptedFirstFamDir) {
              attemptedFirstFamDir = true;
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkColFamDir(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 3, 0, 0, 0, 1);
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed a region dir before the corruption checker got to it.
   */
  @Test(timeout=180000)
  public void testQuarantineMissingRegionDir() throws Exception {
    String table = name.getMethodName();
    ExecutorService exec = new ScheduledThreadPoolExecutor(10);
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, exec) {
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles) throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          boolean attemptedFirstRegionDir = false;
          protected void checkRegionDir(Path p) throws IOException {
            if (!attemptedFirstRegionDir) {
              attemptedFirstRegionDir = true;
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkRegionDir(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 3, 0, 0, 0, 1);
  }

  /**
   * Test fixing lingering reference file.
   */
  @Test
  public void testLingeringReferenceFile() throws Exception {
    String table = "testLingeringReferenceFile";
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating a fake reference file
      FileSystem fs = FileSystem.get(conf);
      Path tableDir= FSUtils.getTablePath(FSUtils.getRootDir(conf), table);
      Path regionDir = FSUtils.getRegionDirs(fs, tableDir).get(0);
      Path famDir = new Path(regionDir, FAM_STR);
      Path fakeReferenceFile = new Path(famDir, "fbce357483ceea.12144538");
      fs.create(fakeReferenceFile);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.LINGERING_REFERENCE_HFILE });
      // fix reference file
      doFsck(conf, true);
      // check that reference file fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      deleteTable(table);
    }
  }

  /**
   * Test pluggable error reporter. It can be plugged in
   * from system property or configuration.
   */
  @Test
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
        PrintingErrorReporter.class.getName());
      MockErrorReporter.calledCount = 0;
    }
  }

  static class MockErrorReporter implements ErrorReporter {
    static int calledCount = 0;

    public void clear() {
      calledCount++;
    }

    public void report(String message) {
      calledCount++;
    }

    public void reportError(String message) {
      calledCount++;
    }

    public void reportError(ERROR_CODE errorCode, String message) {
      calledCount++;
    }

    public void reportError(ERROR_CODE errorCode, String message, TableInfo table) {
      calledCount++;
    }

    public void reportError(ERROR_CODE errorCode,
        String message, TableInfo table, HbckInfo info) {
      calledCount++;
    }

    public void reportError(ERROR_CODE errorCode, String message,
        TableInfo table, HbckInfo info1, HbckInfo info2) {
      calledCount++;
    }

    public int summarize() {
      return ++calledCount;
    }

    public void detail(String details) {
      calledCount++;
    }

    public ArrayList<ERROR_CODE> getErrorList() {
      calledCount++;
      return new ArrayList<ERROR_CODE>();
    }

    public void progress() {
      calledCount++;
    }

    public void print(String message) {
      calledCount++;
    }

    public void resetErrors() {
      calledCount++;
    }

    public boolean tableHasErrors(TableInfo table) {
      calledCount++;
      return false;
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();

  @org.junit.Rule
  public TestName name = new TestName();
}
