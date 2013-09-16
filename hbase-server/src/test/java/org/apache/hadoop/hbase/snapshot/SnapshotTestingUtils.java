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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Assert;

import com.google.protobuf.ServiceException;

/**
 * Utilities class for snapshots
 */
public class SnapshotTestingUtils {

  private static final Log LOG = LogFactory.getLog(SnapshotTestingUtils.class);

  /**
   * Assert that we don't have any snapshots lists
   *
   * @throws IOException
   *           if the admin operation fails
   */
  public static void assertNoSnapshots(HBaseAdmin admin) throws IOException {
    assertEquals("Have some previous snapshots", 0, admin.listSnapshots()
        .size());
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its
   * name and table match the passed in parameters.
   */
  public static List<SnapshotDescription> assertExistsMatchingSnapshot(
      HBaseAdmin admin, String snapshotName, TableName tableName)
      throws IOException {
    // list the snapshot
    List<SnapshotDescription> snapshots = admin.listSnapshots();

    List<SnapshotDescription> returnedSnapshots = new ArrayList<SnapshotDescription>();
    for (SnapshotDescription sd : snapshots) {
      if (snapshotName.equals(sd.getName()) &&
          tableName.equals(TableName.valueOf(sd.getTable()))) {
        returnedSnapshots.add(sd);
      }
    }

    Assert.assertTrue("No matching snapshots found.", returnedSnapshots.size()>0);
    return returnedSnapshots;
  }

  /**
   * Make sure that there is only one snapshot returned from the master
   */
  public static void assertOneSnapshotThatMatches(HBaseAdmin admin,
      SnapshotDescription snapshot) throws IOException {
    assertOneSnapshotThatMatches(admin, snapshot.getName(),
        TableName.valueOf(snapshot.getTable()));
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its
   * name and table match the passed in parameters.
   */
  public static List<SnapshotDescription> assertOneSnapshotThatMatches(
      HBaseAdmin admin, String snapshotName, TableName tableName)
      throws IOException {
    // list the snapshot
    List<SnapshotDescription> snapshots = admin.listSnapshots();

    assertEquals("Should only have 1 snapshot", 1, snapshots.size());
    assertEquals(snapshotName, snapshots.get(0).getName());
    assertEquals(tableName, TableName.valueOf(snapshots.get(0).getTable()));

    return snapshots;
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its
   * name and table match the passed in parameters.
   */
  public static List<SnapshotDescription> assertOneSnapshotThatMatches(
      HBaseAdmin admin, byte[] snapshot, TableName tableName) throws IOException {
    return assertOneSnapshotThatMatches(admin, Bytes.toString(snapshot),
        tableName);
  }

  /**
   * Multi-family version of the confirmSnapshotValid function
   */
  public static void confirmSnapshotValid(
      SnapshotDescription snapshotDescriptor, TableName tableName,
      List<byte[]> nonEmptyTestFamilies, List<byte[]> emptyTestFamilies,
      Path rootDir, HBaseAdmin admin, FileSystem fs, boolean requireLogs,
      Path logsDir, Set<String> snapshotServers) throws IOException {
    if (nonEmptyTestFamilies != null) {
      for (byte[] testFamily : nonEmptyTestFamilies) {
        confirmSnapshotValid(snapshotDescriptor, tableName, testFamily,
            rootDir, admin, fs, requireLogs, logsDir, false, null);
      }
    }

    if (emptyTestFamilies != null) {
      for (byte[] testFamily : emptyTestFamilies) {
        confirmSnapshotValid(snapshotDescriptor, tableName, testFamily,
            rootDir, admin, fs, requireLogs, logsDir, true, null);
      }
    }
  }

  /**
   * Confirm that the snapshot contains references to all the files that should
   * be in the snapshot.
   */
  public static void confirmSnapshotValid(
      SnapshotDescription snapshotDescriptor, TableName tableName,
      byte[] testFamily, Path rootDir, HBaseAdmin admin, FileSystem fs,
      boolean requireLogs, Path logsDir, Set<String> snapshotServers)
      throws IOException {
    confirmSnapshotValid(snapshotDescriptor, tableName, testFamily, rootDir,
        admin, fs, requireLogs, logsDir, false, snapshotServers);
  }

  /**
   * Confirm that the snapshot contains references to all the files that should
   * be in the snapshot.
   */
  public static void confirmSnapshotValid(
      SnapshotDescription snapshotDescriptor, TableName tableName,
      byte[] testFamily, Path rootDir, HBaseAdmin admin, FileSystem fs,
      boolean requireLogs, Path logsDir, boolean familyEmpty,
      Set<String> snapshotServers) throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
        snapshotDescriptor, rootDir);
    assertTrue(fs.exists(snapshotDir));
    Path snapshotinfo = new Path(snapshotDir,
        SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
    assertTrue(fs.exists(snapshotinfo));
    // check the logs dir
    if (requireLogs) {
      TakeSnapshotUtils.verifyAllLogsGotReferenced(fs, logsDir,
          snapshotServers, snapshotDescriptor, new Path(snapshotDir,
              HConstants.HREGION_LOGDIR_NAME));
    }
    // check the table info
    HTableDescriptor desc = FSTableDescriptors.getTableDescriptorFromFs(fs, rootDir,
        tableName);
    HTableDescriptor snapshotDesc = FSTableDescriptors.getTableDescriptorFromFs(fs,
        snapshotDir);
    assertEquals(desc, snapshotDesc);

    // check the region snapshot for all the regions
    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    for (HRegionInfo info : regions) {
      String regionName = info.getEncodedName();
      Path regionDir = new Path(snapshotDir, regionName);
      HRegionInfo snapshotRegionInfo = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
      assertEquals(info, snapshotRegionInfo);

      // check to make sure we have the family
      if (!familyEmpty) {
        Path familyDir = new Path(regionDir, Bytes.toString(testFamily));
        assertTrue("Expected to find: " + familyDir + ", but it doesn't exist",
            fs.exists(familyDir));
        // make sure we have some files references
        assertTrue(fs.listStatus(familyDir).length > 0);
      }
    }
  }

  /**
   * Helper method for testing async snapshot operations. Just waits for the
   * given snapshot to complete on the server by repeatedly checking the master.
   *
   * @param master: the master running the snapshot
   * @param snapshot: the snapshot to check
   * @param sleep: amount to sleep between checks to see if the snapshot is done
   * @throws ServiceException if the snapshot fails
   */
  public static void waitForSnapshotToComplete(HMaster master,
      SnapshotDescription snapshot, long sleep) throws ServiceException {
    final IsSnapshotDoneRequest request = IsSnapshotDoneRequest.newBuilder()
        .setSnapshot(snapshot).build();
    IsSnapshotDoneResponse done = IsSnapshotDoneResponse.newBuilder()
        .buildPartial();
    while (!done.getDone()) {
      done = master.isSnapshotDone(null, request);
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw new ServiceException(e);
      }
    }
  }

  public static void cleanupSnapshot(HBaseAdmin admin, byte[] tableName)
      throws IOException {
    SnapshotTestingUtils.cleanupSnapshot(admin, Bytes.toString(tableName));
  }

  public static void cleanupSnapshot(HBaseAdmin admin, String snapshotName)
      throws IOException {
    // delete the taken snapshot
    admin.deleteSnapshot(snapshotName);
    assertNoSnapshots(admin);
  }

  /**
   * Expect the snapshot to throw an error when checking if the snapshot is
   * complete
   *
   * @param master master to check
   * @param snapshot the {@link SnapshotDescription} request to pass to the master
   * @param clazz expected exception from the master
   */
  public static void expectSnapshotDoneException(HMaster master,
      IsSnapshotDoneRequest snapshot,
      Class<? extends HBaseSnapshotException> clazz) {
    try {
      master.isSnapshotDone(null, snapshot);
      Assert.fail("didn't fail to lookup a snapshot");
    } catch (ServiceException se) {
      try {
        throw ProtobufUtil.getRemoteException(se);
      } catch (HBaseSnapshotException e) {
        assertEquals("Threw wrong snapshot exception!", clazz, e.getClass());
      } catch (Throwable t) {
        Assert.fail("Threw an unexpected exception:" + t);
      }
    }
  }

  /**
   * List all the HFiles in the given table
   *
   * @param fs: FileSystem where the table lives
   * @param tableDir directory of the table
   * @return array of the current HFiles in the table (could be a zero-length array)
   * @throws IOException on unexecpted error reading the FS
   */
  public static FileStatus[] listHFiles(final FileSystem fs, Path tableDir)
      throws IOException {
    // setup the filters we will need based on the filesystem
    PathFilter regionFilter = new FSUtils.RegionDirFilter(fs);
    PathFilter familyFilter = new FSUtils.FamilyDirFilter(fs);
    final PathFilter fileFilter = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        try {
          return fs.isFile(file);
        } catch (IOException e) {
          return false;
        }
      }
    };

    FileStatus[] regionDirs = FSUtils.listStatus(fs, tableDir, regionFilter);
    // if no regions, then we are done
    if (regionDirs == null || regionDirs.length == 0)
      return new FileStatus[0];

    // go through each of the regions, and add al the hfiles under each family
    List<FileStatus> regionFiles = new ArrayList<FileStatus>(regionDirs.length);
    for (FileStatus regionDir : regionDirs) {
      FileStatus[] fams = FSUtils.listStatus(fs, regionDir.getPath(),
          familyFilter);
      // if no families, then we are done again
      if (fams == null || fams.length == 0)
        continue;
      // add all the hfiles under the family
      regionFiles.addAll(SnapshotTestingUtils.getHFilesInRegion(fams, fs,
          fileFilter));
    }
    FileStatus[] files = new FileStatus[regionFiles.size()];
    regionFiles.toArray(files);
    return files;
  }

  /**
   * Get all the hfiles in the region, under the passed set of families
   *
   * @param families: all the family directories under the region
   * @param fs: filesystem where the families live
   * @param fileFilter: filter to only include files
   * @return collection of all the hfiles under all the passed in families (non-null)
   * @throws IOException on unexecpted error reading the FS
   */
  public static Collection<FileStatus> getHFilesInRegion(FileStatus[] families,
      FileSystem fs, PathFilter fileFilter) throws IOException {
    Set<FileStatus> files = new TreeSet<FileStatus>();
    for (FileStatus family : families) {
      // get all the hfiles in the family
      FileStatus[] hfiles = FSUtils
          .listStatus(fs, family.getPath(), fileFilter);
      // if no hfiles, then we are done with this family
      if (hfiles == null || hfiles.length == 0)
        continue;
      files.addAll(Arrays.asList(hfiles));
    }
    return files;
  }

  /**
   * Take an offline snapshot of the specified table and verify if the given
   * family is empty. Note that this will leave the table disabled
   * in the case of an offline snapshot.
   */
  public static void createOfflineSnapshotAndValidate(HBaseAdmin admin,
      TableName tableName, String familyName, String snapshotNameString,
      Path rootDir, FileSystem fs, boolean familyEmpty) throws Exception {

    createSnapshotAndValidate(admin, tableName, familyName,
        snapshotNameString, rootDir, fs, familyEmpty, false);
  }

  /**
   * Take a snapshot of the specified table and verify if the given family is
   * empty. Note that this will leave the table disabled
   * in the case of an offline snapshot.
   */
  public static void createSnapshotAndValidate(HBaseAdmin admin,
      TableName tableName, String familyName, String snapshotNameString,
      Path rootDir, FileSystem fs, boolean familyEmpty, boolean onlineSnapshot)
      throws Exception {

    if (!onlineSnapshot) {
      try {
        admin.disableTable(tableName);
      } catch (TableNotEnabledException tne) {
        LOG.info("In attempting to disable " + tableName
            + " it turns out that this table is already disabled.");
      }
    }

    admin.snapshot(snapshotNameString, tableName);

    List<SnapshotDescription> snapshots = SnapshotTestingUtils
        .assertExistsMatchingSnapshot(admin, snapshotNameString,
            tableName);

    if (snapshots == null || snapshots.size() != 1) {
      Assert.fail("Incorrect number of snapshots for table "
          + tableName);
    }

    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), tableName,
        Bytes.toBytes(familyName), rootDir, admin, fs, false, new Path(rootDir,
            HConstants.HREGION_LOGDIR_NAME), familyEmpty, null);
  }
  public static void createSnapshotAndValidate(HBaseAdmin admin,
      TableName tableName, String familyName, String snapshotNameString,
      Path rootDir, FileSystem fs) throws Exception {
    createSnapshotAndValidate(admin, tableName, familyName,
        snapshotNameString, rootDir, fs, false, false);
  }

  /**
   * This will create a snapshot. Note that this has the side effect
   * of leaving the input table disabled if the offline snapshot
   * option is chosen.
   *
   */
  public static void createSnapshotAndValidate(HBaseAdmin admin,
      TableName tableName, String familyName, String snapshotNameString,
      Path rootDir, FileSystem fs, boolean online) throws Exception {
    createSnapshotAndValidate(admin, tableName, familyName,
        snapshotNameString, rootDir, fs, false, online);
  }

  public static void createSnapshotAndValidate(HBaseAdmin admin,
      TableName tableName, List<byte[]> nonEmptyFamilyNames, List<byte[]> emptyFamilyNames,
      String snapshotNameString, Path rootDir, FileSystem fs) throws Exception {

    try {
      admin.disableTable(tableName);
    } catch (TableNotEnabledException tne) {
      LOG.info("In attempting to disable " + tableName + " it turns out that the this table is " +
          "already disabled.");
    }
    admin.snapshot(snapshotNameString, tableName);

    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertExistsMatchingSnapshot(admin,
      snapshotNameString, tableName);

    // Create test-timestamp-clone
    if (snapshots == null || snapshots.size() != 1) {
      Assert.fail("Incorrect number of snapshots for table " + tableName);
    }

    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), tableName, nonEmptyFamilyNames, emptyFamilyNames,
      rootDir, admin, fs, false, new Path(rootDir, HConstants.HREGION_LOGDIR_NAME), null);
  }

  // ==========================================================================
  //  Table Helpers
  // ==========================================================================
  public static void waitForTableToBeOnline(final HBaseTestingUtility util,
                                            final TableName tableName)
      throws IOException, InterruptedException {
    HRegionServer rs = util.getRSForFirstRegionInTable(tableName);
    List<HRegion> onlineRegions = rs.getOnlineRegions(tableName);
    for (HRegion region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }
    util.getHBaseAdmin().isTableAvailable(tableName);
  }

  public static void createTable(final HBaseTestingUtility util, final TableName tableName,
      final byte[]... families) throws IOException, InterruptedException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family: families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    byte[][] splitKeys = new byte[14][];
    byte[] hex = Bytes.toBytes("123456789abcde");
    for (int i = 0; i < splitKeys.length; ++i) {
      splitKeys[i] = new byte[] { hex[i] };
    }
    util.getHBaseAdmin().createTable(htd, splitKeys);
    waitForTableToBeOnline(util, tableName);
    assertEquals(15, util.getHBaseAdmin().getTableRegions(tableName).size());
  }

  public static void loadData(final HBaseTestingUtility util, final TableName tableName, int rows,
      byte[]... families) throws IOException, InterruptedException {
    loadData(util, new HTable(util.getConfiguration(), tableName), rows, families);
  }

  public static void loadData(final HBaseTestingUtility util, final HTable table, int rows,
      byte[]... families) throws IOException, InterruptedException {
    table.setAutoFlush(false, true);

    // Ensure one row per region
    assertTrue(rows >= 16);
    for (byte k0: Bytes.toBytes("0123456789abcdef")) {
      byte[] k = new byte[] { k0 };
      byte[] value = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), k);
      byte[] key = Bytes.add(k, Bytes.toBytes(MD5Hash.getMD5AsHex(value)));
      putData(table, families, key, value);
      rows--;
    }

    // Add other extra rows. more rows, more files
    while (rows-- > 0) {
      byte[] value = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), Bytes.toBytes(rows));
      byte[] key = Bytes.toBytes(MD5Hash.getMD5AsHex(value));
      putData(table, families, key, value);
    }
    table.flushCommits();

    waitForTableToBeOnline(util, table.getName());
  }

  private static void putData(final HTable table, final byte[][] families,
      final byte[] key, final byte[] value) throws IOException {
    byte[] q = Bytes.toBytes("q");
    Put put = new Put(key);
    put.setDurability(Durability.SKIP_WAL);
    for (byte[] family: families) {
      put.add(family, q, value);
    }
    table.put(put);
  }

  public static void deleteAllSnapshots(final HBaseAdmin admin)
      throws IOException {
    // Delete all the snapshots
    for (SnapshotDescription snapshot: admin.listSnapshots()) {
      admin.deleteSnapshot(snapshot.getName());
    }
    SnapshotTestingUtils.assertNoSnapshots(admin);
  }

  public static void deleteArchiveDirectory(final HBaseTestingUtility util)
      throws IOException {
    // Ensure the archiver to be empty
    MasterFileSystem mfs = util.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path archiveDir = new Path(mfs.getRootDir(), HConstants.HFILE_ARCHIVE_DIRECTORY);
    mfs.getFileSystem().delete(archiveDir, true);
  }

  public static void verifyRowCount(final HBaseTestingUtility util, final TableName tableName,
      long expectedRows) throws IOException {
    HTable table = new HTable(util.getConfiguration(), tableName);
    try {
      assertEquals(expectedRows, util.countRows(table));
    } finally {
      table.close();
    }
  }
}
