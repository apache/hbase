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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSVisitor;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Assert;

import com.google.protobuf.ServiceException;

/**
 * Utilities class for snapshots
 */
public class SnapshotTestingUtils {

  private static final Log LOG = LogFactory.getLog(SnapshotTestingUtils.class);
  private static byte[] KEYS = Bytes.toBytes("0123456789");

  /**
   * Assert that we don't have any snapshots lists
   *
   * @throws IOException
   *           if the admin operation fails
   */
  public static void assertNoSnapshots(Admin admin) throws IOException {
    assertEquals("Have some previous snapshots", 0, admin.listSnapshots()
        .size());
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its
   * name and table match the passed in parameters.
   */
  public static List<SnapshotDescription> assertExistsMatchingSnapshot(
      Admin admin, String snapshotName, TableName tableName)
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
  public static void assertOneSnapshotThatMatches(Admin admin,
      SnapshotDescription snapshot) throws IOException {
    assertOneSnapshotThatMatches(admin, snapshot.getName(),
        TableName.valueOf(snapshot.getTable()));
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its
   * name and table match the passed in parameters.
   */
  public static List<SnapshotDescription> assertOneSnapshotThatMatches(
      Admin admin, String snapshotName, TableName tableName)
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
      Admin admin, byte[] snapshot, TableName tableName) throws IOException {
    return assertOneSnapshotThatMatches(admin, Bytes.toString(snapshot),
        tableName);
  }

  /**
   * Confirm that the snapshot contains references to all the files that should
   * be in the snapshot.
   */
  public static void confirmSnapshotValid(
      SnapshotDescription snapshotDescriptor, TableName tableName,
      byte[] testFamily, Path rootDir, Admin admin, FileSystem fs)
      throws IOException {
    ArrayList nonEmptyTestFamilies = new ArrayList(1);
    nonEmptyTestFamilies.add(testFamily);
    confirmSnapshotValid(snapshotDescriptor, tableName,
      nonEmptyTestFamilies, null, rootDir, admin, fs);
  }

  /**
   * Confirm that the snapshot has no references files but only metadata.
   */
  public static void confirmEmptySnapshotValid(
      SnapshotDescription snapshotDescriptor, TableName tableName,
      byte[] testFamily, Path rootDir, Admin admin, FileSystem fs)
      throws IOException {
    ArrayList emptyTestFamilies = new ArrayList(1);
    emptyTestFamilies.add(testFamily);
    confirmSnapshotValid(snapshotDescriptor, tableName,
      null, emptyTestFamilies, rootDir, admin, fs);
  }

  /**
   * Confirm that the snapshot contains references to all the files that should
   * be in the snapshot. This method also perform some redundant check like
   * the existence of the snapshotinfo or the regioninfo which are done always
   * by the MasterSnapshotVerifier, at the end of the snapshot operation.
   */
  public static void confirmSnapshotValid(
      SnapshotDescription snapshotDescriptor, TableName tableName,
      List<byte[]> nonEmptyTestFamilies, List<byte[]> emptyTestFamilies,
      Path rootDir, Admin admin, FileSystem fs) throws IOException {
    final Configuration conf = admin.getConfiguration();

    // check snapshot dir
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
        snapshotDescriptor, rootDir);
    assertTrue(fs.exists(snapshotDir));

    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

    // Extract regions and families with store files
    final Set<byte[]> snapshotFamilies = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, desc);
    Map<String, SnapshotRegionManifest> regionManifests = manifest.getRegionManifestsMap();
    for (SnapshotRegionManifest regionManifest: regionManifests.values()) {
      SnapshotReferenceUtil.visitRegionStoreFiles(regionManifest,
          new SnapshotReferenceUtil.StoreFileVisitor() {
        @Override
        public void storeFile(final HRegionInfo regionInfo, final String family,
              final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          snapshotFamilies.add(Bytes.toBytes(family));
        }
      });
    }

    // Verify that there are store files in the specified families
    if (nonEmptyTestFamilies != null) {
      for (final byte[] familyName: nonEmptyTestFamilies) {
        assertTrue(snapshotFamilies.contains(familyName));
      }
    }

    // Verify that there are no store files in the specified families
    if (emptyTestFamilies != null) {
      for (final byte[] familyName: emptyTestFamilies) {
        assertFalse(snapshotFamilies.contains(familyName));
      }
    }

    // check the region snapshot for all the regions
    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    // remove the non-default regions
    RegionReplicaUtil.removeNonDefaultRegions(regions);
    boolean hasMob = regionManifests.containsKey(MobUtils.getMobRegionInfo(tableName)
        .getEncodedName());
    if (hasMob) {
      assertEquals(regions.size(), regionManifests.size() - 1);
    } else {
      assertEquals(regions.size(), regionManifests.size());
    }

    // Verify Regions (redundant check, see MasterSnapshotVerifier)
    for (HRegionInfo info : regions) {
      String regionName = info.getEncodedName();
      assertTrue(regionManifests.containsKey(regionName));
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
      done = master.getMasterRpcServices().isSnapshotDone(null, request);
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw new ServiceException(e);
      }
    }
  }

  /*
   * Take snapshot with maximum of numTries attempts, ignoring CorruptedSnapshotException
   * except for the last CorruptedSnapshotException
   */
  public static void snapshot(Admin admin,
      final String snapshotName, final String tableName,
      SnapshotDescription.Type type, int numTries) throws IOException {
    int tries = 0;
    CorruptedSnapshotException lastEx = null;
    while (tries++ < numTries) {
      try {
        admin.snapshot(snapshotName, TableName.valueOf(tableName), type);
        return;
      } catch (CorruptedSnapshotException cse) {
        LOG.warn("Got CorruptedSnapshotException", cse);
        lastEx = cse;
      }
    }
    throw lastEx;
  }

  public static void cleanupSnapshot(Admin admin, byte[] tableName)
      throws IOException {
    SnapshotTestingUtils.cleanupSnapshot(admin, Bytes.toString(tableName));
  }

  public static void cleanupSnapshot(Admin admin, String snapshotName)
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
      master.getMasterRpcServices().isSnapshotDone(null, snapshot);
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
  public static Path[] listHFiles(final FileSystem fs, final Path tableDir)
      throws IOException {
    final ArrayList<Path> hfiles = new ArrayList<Path>();
    FSVisitor.visitTableStoreFiles(fs, tableDir, new FSVisitor.StoreFileVisitor() {
      @Override
      public void storeFile(final String region, final String family, final String hfileName)
          throws IOException {
        hfiles.add(new Path(tableDir, new Path(region, new Path(family, hfileName))));
      }
    });
    return hfiles.toArray(new Path[hfiles.size()]);
  }

  public static String[] listHFileNames(final FileSystem fs, final Path tableDir)
      throws IOException {
    Path[] files = listHFiles(fs, tableDir);
    String[] names = new String[files.length];
    for (int i = 0; i < files.length; ++i) {
      names[i] = files[i].getName();
    }
    Arrays.sort(names);
    return names;
  }

  /**
   * Take a snapshot of the specified table and verify that the given family is
   * not empty. Note that this will leave the table disabled
   * in the case of an offline snapshot.
   */
  public static void createSnapshotAndValidate(Admin admin,
      TableName tableName, String familyName, String snapshotNameString,
      Path rootDir, FileSystem fs, boolean onlineSnapshot)
      throws Exception {
    ArrayList<byte[]> nonEmptyFamilyNames = new ArrayList<byte[]>(1);
    nonEmptyFamilyNames.add(Bytes.toBytes(familyName));
    createSnapshotAndValidate(admin, tableName, nonEmptyFamilyNames, /* emptyFamilyNames= */ null,
                              snapshotNameString, rootDir, fs, onlineSnapshot);
  }

  /**
   * Take a snapshot of the specified table and verify the given families.
   * Note that this will leave the table disabled in the case of an offline snapshot.
   */
  public static void createSnapshotAndValidate(Admin admin,
      TableName tableName, List<byte[]> nonEmptyFamilyNames, List<byte[]> emptyFamilyNames,
      String snapshotNameString, Path rootDir, FileSystem fs, boolean onlineSnapshot)
        throws Exception {
    if (!onlineSnapshot) {
      try {
        admin.disableTable(tableName);
      } catch (TableNotEnabledException tne) {
        LOG.info("In attempting to disable " + tableName + " it turns out that the this table is " +
            "already disabled.");
      }
    }
    admin.snapshot(snapshotNameString, tableName);

    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertExistsMatchingSnapshot(admin,
      snapshotNameString, tableName);
    if (snapshots == null || snapshots.size() != 1) {
      Assert.fail("Incorrect number of snapshots for table " + tableName);
    }

    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), tableName, nonEmptyFamilyNames,
      emptyFamilyNames, rootDir, admin, fs);
  }

  /**
   * Corrupt the specified snapshot by deleting some files.
   *
   * @param util {@link HBaseTestingUtility}
   * @param snapshotName name of the snapshot to corrupt
   * @return array of the corrupted HFiles
   * @throws IOException on unexecpted error reading the FS
   */
  public static ArrayList corruptSnapshot(final HBaseTestingUtility util, final String snapshotName)
      throws IOException {
    final MasterFileSystem mfs = util.getHBaseCluster().getMaster().getMasterFileSystem();
    final FileSystem fs = mfs.getFileSystem();

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName,
                                                                        mfs.getRootDir());
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    final TableName table = TableName.valueOf(snapshotDesc.getTable());

    final ArrayList corruptedFiles = new ArrayList();
    final Configuration conf = util.getConfiguration();
    SnapshotReferenceUtil.visitTableStoreFiles(conf, fs, snapshotDir, snapshotDesc,
        new SnapshotReferenceUtil.StoreFileVisitor() {
      @Override
      public void storeFile(final HRegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
        String region = regionInfo.getEncodedName();
        String hfile = storeFile.getName();
        HFileLink link = HFileLink.build(conf, table, region, family, hfile);
        if (corruptedFiles.size() % 2 == 0) {
          fs.delete(link.getAvailablePath(fs), true);
          corruptedFiles.add(hfile);
        }
      }
    });

    assertTrue(corruptedFiles.size() > 0);
    return corruptedFiles;
  }

  // ==========================================================================
  //  Snapshot Mock
  // ==========================================================================
  public static class SnapshotMock {
    protected final static String TEST_FAMILY = "cf";
    public final static int TEST_NUM_REGIONS = 4;

    private final Configuration conf;
    private final FileSystem fs;
    private final Path rootDir;

    static class RegionData {
      public HRegionInfo hri;
      public Path tableDir;
      public Path[] files;

      public RegionData(final Path tableDir, final HRegionInfo hri, final int nfiles) {
        this.tableDir = tableDir;
        this.hri = hri;
        this.files = new Path[nfiles];
      }
    }

    public static class SnapshotBuilder {
      private final RegionData[] tableRegions;
      private final SnapshotDescription desc;
      private final HTableDescriptor htd;
      private final Configuration conf;
      private final FileSystem fs;
      private final Path rootDir;
      private Path snapshotDir;
      private int snapshotted = 0;

      public SnapshotBuilder(final Configuration conf, final FileSystem fs,
          final Path rootDir, final HTableDescriptor htd,
          final SnapshotDescription desc, final RegionData[] tableRegions)
          throws IOException {
        this.fs = fs;
        this.conf = conf;
        this.rootDir = rootDir;
        this.htd = htd;
        this.desc = desc;
        this.tableRegions = tableRegions;
        this.snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir);
        new FSTableDescriptors(conf)
          .createTableDescriptorForTableDirectory(snapshotDir,
              new TableDescriptor(htd), false);
      }

      public HTableDescriptor getTableDescriptor() {
        return this.htd;
      }

      public SnapshotDescription getSnapshotDescription() {
        return this.desc;
      }

      public Path getSnapshotsDir() {
        return this.snapshotDir;
      }

      public Path[] addRegion() throws IOException {
        return addRegion(desc);
      }

      public Path[] addRegionV1() throws IOException {
        return addRegion(desc.toBuilder()
                          .setVersion(SnapshotManifestV1.DESCRIPTOR_VERSION)
                          .build());
      }

      public Path[] addRegionV2() throws IOException {
        return addRegion(desc.toBuilder()
                          .setVersion(SnapshotManifestV2.DESCRIPTOR_VERSION)
                          .build());
      }

      private Path[] addRegion(final SnapshotDescription desc) throws IOException {
        if (this.snapshotted == tableRegions.length) {
          throw new UnsupportedOperationException("No more regions in the table");
        }

        RegionData regionData = tableRegions[this.snapshotted++];
        ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(desc.getName());
        SnapshotManifest manifest = SnapshotManifest.create(conf, fs, snapshotDir, desc, monitor);
        manifest.addRegion(regionData.tableDir, regionData.hri);
        return regionData.files;
      }

      public Path commit() throws IOException {
        ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(desc.getName());
        SnapshotManifest manifest = SnapshotManifest.create(conf, fs, snapshotDir, desc, monitor);
        manifest.addTableDescriptor(htd);
        manifest.consolidate();
        SnapshotDescriptionUtils.completeSnapshot(desc, rootDir, snapshotDir, fs);
        snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(desc, rootDir);
        return snapshotDir;
      }
    }

    public SnapshotMock(final Configuration conf, final FileSystem fs, final Path rootDir) {
      this.fs = fs;
      this.conf = conf;
      this.rootDir = rootDir;
    }

    public SnapshotBuilder createSnapshotV1(final String snapshotName, final String tableName)
        throws IOException {
      return createSnapshot(snapshotName, tableName, SnapshotManifestV1.DESCRIPTOR_VERSION);
    }

    public SnapshotBuilder createSnapshotV2(final String snapshotName, final String tableName)
        throws IOException {
      return createSnapshot(snapshotName, tableName, SnapshotManifestV2.DESCRIPTOR_VERSION);
    }

    private SnapshotBuilder createSnapshot(final String snapshotName, final String tableName,
        final int version) throws IOException {
      HTableDescriptor htd = createHtd(tableName);
      RegionData[] regions = createTable(htd, TEST_NUM_REGIONS);

      SnapshotDescription desc = SnapshotDescription.newBuilder()
        .setTable(htd.getNameAsString())
        .setName(snapshotName)
        .setVersion(version)
        .build();

      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir);
      SnapshotDescriptionUtils.writeSnapshotInfo(desc, workingDir, fs);
      return new SnapshotBuilder(conf, fs, rootDir, htd, desc, regions);
    }

    public HTableDescriptor createHtd(final String tableName) {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
      return htd;
    }

    private RegionData[] createTable(final HTableDescriptor htd, final int nregions)
        throws IOException {
      Path tableDir = FSUtils.getTableDir(rootDir, htd.getTableName());
      new FSTableDescriptors(conf).createTableDescriptorForTableDirectory(tableDir,
          new TableDescriptor(htd), false);

      assertTrue(nregions % 2 == 0);
      RegionData[] regions = new RegionData[nregions];
      for (int i = 0; i < regions.length; i += 2) {
        byte[] startKey = Bytes.toBytes(0 + i * 2);
        byte[] endKey = Bytes.toBytes(1 + i * 2);

        // First region, simple with one plain hfile.
        HRegionInfo hri = new HRegionInfo(htd.getTableName(), startKey, endKey);
        HRegionFileSystem rfs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, hri);
        regions[i] = new RegionData(tableDir, hri, 3);
        for (int j = 0; j < regions[i].files.length; ++j) {
          Path storeFile = createStoreFile(rfs.createTempName());
          regions[i].files[j] = rfs.commitStoreFile(TEST_FAMILY, storeFile);
        }

        // Second region, used to test the split case.
        // This region contains a reference to the hfile in the first region.
        startKey = Bytes.toBytes(2 + i * 2);
        endKey = Bytes.toBytes(3 + i * 2);
        hri = new HRegionInfo(htd.getTableName());
        rfs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, hri);
        regions[i+1] = new RegionData(tableDir, hri, regions[i].files.length);
        for (int j = 0; j < regions[i].files.length; ++j) {
          String refName = regions[i].files[j].getName() + '.' + regions[i].hri.getEncodedName();
          Path refFile = createStoreFile(new Path(rootDir, refName));
          regions[i+1].files[j] = rfs.commitStoreFile(TEST_FAMILY, refFile);
        }
      }
      return regions;
    }

    private Path createStoreFile(final Path storeFile)
        throws IOException {
      FSDataOutputStream out = fs.create(storeFile);
      try {
        out.write(Bytes.toBytes(storeFile.toString()));
      } finally {
        out.close();
      }
      return storeFile;
    }
  }

  // ==========================================================================
  //  Table Helpers
  // ==========================================================================
  public static void waitForTableToBeOnline(final HBaseTestingUtility util,
                                            final TableName tableName)
      throws IOException, InterruptedException {
    HRegionServer rs = util.getRSForFirstRegionInTable(tableName);
    List<Region> onlineRegions = rs.getOnlineRegions(tableName);
    for (Region region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }
    // Wait up to 60 seconds for a table to be available.
    util.waitFor(60000, util.predicateTableAvailable(tableName));
  }

  public static void createTable(final HBaseTestingUtility util, final TableName tableName,
      int regionReplication, final byte[]... families) throws IOException, InterruptedException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(regionReplication);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    byte[][] splitKeys = getSplitKeys();
    util.createTable(htd, splitKeys);
    assertEquals((splitKeys.length + 1) * regionReplication,
        util.getHBaseAdmin().getTableRegions(tableName).size());
  }

  public static byte[][] getSplitKeys() {
    byte[][] splitKeys = new byte[KEYS.length-2][];
    for (int i = 0; i < splitKeys.length; ++i) {
      splitKeys[i] = new byte[] { KEYS[i+1] };
    }
    return splitKeys;
  }

  public static void createTable(final HBaseTestingUtility util, final TableName tableName,
      final byte[]... families) throws IOException, InterruptedException {
    createTable(util, tableName, 1, families);
  }

  public static void loadData(final HBaseTestingUtility util, final TableName tableName, int rows,
      byte[]... families) throws IOException, InterruptedException {
    BufferedMutator mutator = util.getConnection().getBufferedMutator(tableName);
    loadData(util, mutator, rows, families);
  }

  public static void loadData(final HBaseTestingUtility util, final BufferedMutator mutator, int rows,
      byte[]... families) throws IOException, InterruptedException {
    // Ensure one row per region
    assertTrue(rows >= KEYS.length);
    for (byte k0: KEYS) {
      byte[] k = new byte[] { k0 };
      byte[] value = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), k);
      byte[] key = Bytes.add(k, Bytes.toBytes(MD5Hash.getMD5AsHex(value)));
      final byte[][] families1 = families;
      final byte[] key1 = key;
      final byte[] value1 = value;
      mutator.mutate(createPut(families1, key1, value1));
      rows--;
    }

    // Add other extra rows. more rows, more files
    while (rows-- > 0) {
      byte[] value = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), Bytes.toBytes(rows));
      byte[] key = Bytes.toBytes(MD5Hash.getMD5AsHex(value));
      final byte[][] families1 = families;
      final byte[] key1 = key;
      final byte[] value1 = value;
      mutator.mutate(createPut(families1, key1, value1));
    }
    mutator.flush();

    waitForTableToBeOnline(util, mutator.getName());
  }

  private static Put createPut(final byte[][] families, final byte[] key, final byte[] value) {
    byte[] q = Bytes.toBytes("q");
    Put put = new Put(key);
    put.setDurability(Durability.SKIP_WAL);
    for (byte[] family: families) {
      put.add(family, q, value);
    }
    return put;
  }

  public static void deleteAllSnapshots(final Admin admin)
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
    Table table = util.getConnection().getTable(tableName);
    try {
      assertEquals(expectedRows, util.countRows(table));
    } finally {
      table.close();
    }
  }

  public static void verifyReplicasCameOnline(TableName tableName, Admin admin,
      int regionReplication) throws IOException {
    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    HashSet<HRegionInfo> set = new HashSet<HRegionInfo>();
    for (HRegionInfo hri : regions) {
      set.add(RegionReplicaUtil.getRegionInfoForDefaultReplica(hri));
      for (int i = 0; i < regionReplication; i++) {
        HRegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(hri, i);
        if (!regions.contains(replica)) {
          Assert.fail(replica + " is not contained in the list of online regions");
        }
      }
    }
    assert(set.size() == getSplitKeys().length + 1);
  }
}
