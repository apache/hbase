/**
 * Copyright 2010 The Apache Software Foundation
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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This tests HBaseFsck's ability to detect reasons for inconsistent tables.
 */
public class TestHBaseFsck {
  final static Log LOG = LogFactory.getLog(TestHBaseFsck.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static Configuration conf = TEST_UTIL.getConfiguration();
  private final static byte[] FAM = Bytes.toBytes("fam");

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
    new HTable(conf, Bytes.toBytes(table)).getScanner(new Scan());
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
          undeployRegion(new HBaseAdmin(conf), hsa, hri);
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
    admin.disableTableAsync(tbytes);
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
      ServerName hsi;
      while ( (hsi = findDeployedHSI(getDeployedHRIs(admin), hriDupe)) == null) {
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

    try {
      assertEquals(0, countRows());
    } catch (IOException ioe) {
      // we've actually deleted the table already. :)
      return;
    }
    fail("Should have failed with IOException");
  }
}
