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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.HSnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * Test the master-related aspects of a snapshot
 */
@Category(MediumTests.class)
public class TestSnapshotFromMaster {

  private static final Log LOG = LogFactory.getLog(TestSnapshotFromMaster.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static Path rootDir;
  private static Path snapshots;
  private static FileSystem fs;
  private static HMaster master;

  // for hfile archiving test.
  private static Path archiveDir;
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  // refresh the cache every 1/2 second
  private static final long cacheRefreshPeriod = 500;

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
    fs = UTIL.getDFSCluster().getFileSystem();
    master = UTIL.getMiniHBaseCluster().getMaster();
    rootDir = master.getMasterFileSystem().getRootDir();
    snapshots = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
    archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 3);
    conf.setInt("hbase.hstore.compactionThreshold", 5);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // drop the number of attempts for the hbase admin
    conf.setInt("hbase.client.retries.number", 1);
    // Ensure no extra cleaners on by default (e.g. TimeToLiveHFileCleaner)
    conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, "");
    conf.set(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS, "");
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setLong(SnapshotHFileCleaner.HFILE_CACHE_REFRESH_PERIOD_CONF_KEY, cacheRefreshPeriod);

    // prevent aggressive region split
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
    master.getSnapshotManagerForTesting().setSnapshotHandlerForTesting(null);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);

    // delete the archive directory, if its exists
    if (fs.exists(archiveDir)) {
      if (!fs.delete(archiveDir, true)) {
        throw new IOException("Couldn't delete archive directory (" + archiveDir
            + " for an unknown reason");
      }
    }

    // delete the snapshot directory, if its exists
    if (fs.exists(snapshots)) {
      if (!fs.delete(snapshots, true)) {
        throw new IOException("Couldn't delete snapshots directory (" + snapshots
            + " for an unknown reason");
      }
    }
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      // NOOP;
    }
  }

  /**
   * Test that the contract from the master for checking on a snapshot are valid.
   * <p>
   * <ol>
   * <li>If a snapshot fails with an error, we expect to get the source error.</li>
   * <li>If there is no snapshot name supplied, we should get an error.</li>
   * <li>If asking about a snapshot has hasn't occurred, you should get an error.</li>
   * </ol>
   */
  @Test(timeout = 15000)
  public void testIsDoneContract() throws Exception {

    String snapshotName = "asyncExpectedFailureTest";

    // check that we get an exception when looking up snapshot where one hasn't happened
    SnapshotTestingUtils.expectSnapshotDoneException(master, new HSnapshotDescription(),
      UnknownSnapshotException.class);

    // and that we get the same issue, even if we specify a name
    SnapshotDescription desc = SnapshotDescription.newBuilder()
      .setName(snapshotName).build();
    SnapshotTestingUtils.expectSnapshotDoneException(master, new HSnapshotDescription(desc),
      UnknownSnapshotException.class);

    // set a mock handler to simulate a snapshot
    DisabledTableSnapshotHandler mockHandler = Mockito.mock(DisabledTableSnapshotHandler.class);
    Mockito.when(mockHandler.getException()).thenReturn(null);
    Mockito.when(mockHandler.getSnapshot()).thenReturn(desc);
    Mockito.when(mockHandler.isFinished()).thenReturn(new Boolean(true));

    master.getSnapshotManagerForTesting().setSnapshotHandlerForTesting(mockHandler);

    // if we do a lookup without a snapshot name, we should fail - you should always know your name
    SnapshotTestingUtils.expectSnapshotDoneException(master, new HSnapshotDescription(),
      UnknownSnapshotException.class);

    // then do the lookup for the snapshot that it is done
    boolean isDone = master.isSnapshotDone(new HSnapshotDescription(desc));
    assertTrue("Snapshot didn't complete when it should have.", isDone);

    // now try the case where we are looking for a snapshot we didn't take
    desc = SnapshotDescription.newBuilder().setName("Not A Snapshot").build();
    SnapshotTestingUtils.expectSnapshotDoneException(master, new HSnapshotDescription(desc),
      UnknownSnapshotException.class);

    // then create a snapshot to the fs and make sure that we can find it when checking done
    snapshotName = "completed";
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    desc = desc.toBuilder().setName(snapshotName).build();
    SnapshotDescriptionUtils.writeSnapshotInfo(desc, snapshotDir, fs);

    isDone = master.isSnapshotDone(new HSnapshotDescription(desc));
    assertTrue("Completed, on-disk snapshot not found", isDone);
  }

  @Test
  public void testGetCompletedSnapshots() throws Exception {
    // first check when there are no snapshots
    List<HSnapshotDescription> snapshots = master.getCompletedSnapshots();
    assertEquals("Found unexpected number of snapshots", 0, snapshots.size());

    // write one snapshot to the fs
    String snapshotName = "completed";
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName(snapshotName).build();
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, snapshotDir, fs);

    // check that we get one snapshot
    snapshots = master.getCompletedSnapshots();
    assertEquals("Found unexpected number of snapshots", 1, snapshots.size());
    List<HSnapshotDescription> expected = Lists.newArrayList(new HSnapshotDescription(snapshot));
    assertEquals("Returned snapshots don't match created snapshots", expected, snapshots);

    // write a second snapshot
    snapshotName = "completed_two";
    snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    snapshot = SnapshotDescription.newBuilder().setName(snapshotName).build();
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, snapshotDir, fs);
    expected.add(new HSnapshotDescription(snapshot));

    // check that we get one snapshot
    snapshots = master.getCompletedSnapshots();
    assertEquals("Found unexpected number of snapshots", 2, snapshots.size());
    assertEquals("Returned snapshots don't match created snapshots", expected, snapshots);
  }

  @Test
  public void testDeleteSnapshot() throws Exception {

    String snapshotName = "completed";
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName(snapshotName).build();

    try {
      master.deleteSnapshot(new HSnapshotDescription(snapshot));
      fail("Master didn't throw exception when attempting to delete snapshot that doesn't exist");
    } catch (IOException e) {
      LOG.debug("Correctly failed delete of non-existant snapshot:" + e.getMessage());
    }

    // write one snapshot to the fs
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, snapshotDir, fs);

    // then delete the existing snapshot,which shouldn't cause an exception to be thrown
    master.deleteSnapshot(new HSnapshotDescription(snapshot));
  }

  /**
   * Test that the snapshot hfile archive cleaner works correctly. HFiles that are in snapshots
   * should be retained, while those that are not in a snapshot should be deleted.
   * @throws Exception on failure
   */
  @Test
  public void testSnapshotHFileArchiving() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // load the table
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);

    // disable the table so we can take a snapshot
    admin.disableTable(TABLE_NAME);

    // take a snapshot of the table
    String snapshotName = "snapshot";
    byte[] snapshotNameBytes = Bytes.toBytes(snapshotName);
    admin.snapshot(snapshotNameBytes, TABLE_NAME);

    Configuration conf = master.getConfiguration();
    LOG.info("After snapshot File-System state");
    FSUtils.logFileSystemState(fs, rootDir, LOG);

    // ensure we only have one snapshot
    SnapshotTestingUtils.assertOneSnapshotThatMatches(admin, snapshotNameBytes, TABLE_NAME);

    // renable the table so we can compact the regions
    admin.enableTable(TABLE_NAME);

    // compact the files so we get some archived files for the table we just snapshotted
    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    for (HRegion region : regions) {
      region.waitForFlushesAndCompactions(); // enable can trigger a compaction, wait for it.
      region.compactStores();
    }
    LOG.info("After compaction File-System state");
    FSUtils.logFileSystemState(fs, rootDir, LOG);

    // make sure the cleaner has run
    LOG.debug("Running hfile cleaners");
    ensureHFileCleanersRun();
    LOG.info("After cleaners File-System state: " + rootDir);
    FSUtils.logFileSystemState(fs, rootDir, LOG);

    // get the snapshot files for the table
    Path snapshotTable = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    FileStatus[] snapshotHFiles = SnapshotTestingUtils.listHFiles(fs, snapshotTable);
    // check that the files in the archive contain the ones that we need for the snapshot
    LOG.debug("Have snapshot hfiles:");
    for (FileStatus file : snapshotHFiles) {
      LOG.debug(file.getPath());
    }
    // get the archived files for the table
    Collection<String> files = getArchivedHFiles(archiveDir, rootDir, fs, STRING_TABLE_NAME);

    // and make sure that there is a proper subset
    for (FileStatus file : snapshotHFiles) {
      assertTrue("Archived hfiles " + files + " is missing snapshot file:" + file.getPath(),
        files.contains(file.getPath().getName()));
    }

    // delete the existing snapshot
    admin.deleteSnapshot(snapshotNameBytes);
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // make sure that we don't keep around the hfiles that aren't in a snapshot
    // make sure we wait long enough to refresh the snapshot hfile
    List<BaseHFileCleanerDelegate> delegates = UTIL.getMiniHBaseCluster().getMaster()
        .getHFileCleaner().cleanersChain;
    for (BaseHFileCleanerDelegate delegate: delegates) {
      if (delegate instanceof SnapshotHFileCleaner) {
        ((SnapshotHFileCleaner)delegate).getFileCacheForTesting().triggerCacheRefreshForTesting();
      }
    }
    // run the cleaner again
    LOG.debug("Running hfile cleaners");
    ensureHFileCleanersRun();
    LOG.info("After delete snapshot cleaners run File-System state");
    FSUtils.logFileSystemState(fs, rootDir, LOG);

    files = getArchivedHFiles(archiveDir, rootDir, fs, STRING_TABLE_NAME);
    assertEquals("Still have some hfiles in the archive, when their snapshot has been deleted.", 0,
      files.size());
  }

  /**
   * @return all the HFiles for a given table that have been archived
   * @throws IOException on expected failure
   */
  private final Collection<String> getArchivedHFiles(Path archiveDir, Path rootDir,
      FileSystem fs, String tableName) throws IOException {
    Path tableArchive = new Path(archiveDir, tableName);
    FileStatus[] archivedHFiles = SnapshotTestingUtils.listHFiles(fs, tableArchive);
    List<String> files = new ArrayList<String>(archivedHFiles.length);
    LOG.debug("Have archived hfiles: " + tableArchive);
    for (FileStatus file : archivedHFiles) {
      LOG.debug(file.getPath());
      files.add(file.getPath().getName());
    }
    // sort the archived files

    Collections.sort(files);
    return files;
  }

  /**
   * Make sure the {@link HFileCleaner HFileCleaners} run at least once
   */
  private static void ensureHFileCleanersRun() {
    UTIL.getHBaseCluster().getMaster().getHFileCleaner().chore();
  }
}
