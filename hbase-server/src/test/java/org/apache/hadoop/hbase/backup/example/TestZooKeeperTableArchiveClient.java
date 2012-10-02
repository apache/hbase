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
package org.apache.hadoop.hbase.backup.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.CheckedArchivingHFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Spin up a small cluster and check that the hfiles of region are properly long-term archived as
 * specified via the {@link ZKTableArchiveClient}.
 */
@Category(LargeTests.class)
public class TestZooKeeperTableArchiveClient {

  private static final Log LOG = LogFactory.getLog(TestZooKeeperTableArchiveClient.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  private static final int numRS = 2;
  private static final int maxTries = 5;
  private static final long ttl = 1000;
  private static ZKTableArchiveClient archivingClient;

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(numRS);
    archivingClient = new ZKTableArchiveClient(UTIL.getConfiguration(), UTIL.getHBaseAdmin()
        .getConnection());
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
    // set the ttl on the hfiles
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, ttl);
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
      CheckedArchivingHFileCleaner.class.getCanonicalName(),
      LongTermArchivingHFileCleaner.class.getCanonicalName());
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
    // make sure that backups are off for all tables
    archivingClient.disableHFileBackup();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("problem shutting down cluster", e);
    }
  }

  /**
   * Test turning on/off archiving
   */
  @Test
  public void testArchivingEnableDisable() throws Exception {
    // 1. turn on hfile backups
    LOG.debug("----Starting archiving");
    archivingClient.enableHFileBackupAsync(TABLE_NAME);
    assertTrue("Archving didn't get turned on", archivingClient
        .getArchivingEnabled(TABLE_NAME));

    // 2. Turn off archiving and make sure its off
    archivingClient.disableHFileBackup();
    assertFalse("Archving didn't get turned off.", archivingClient.getArchivingEnabled(TABLE_NAME));

    // 3. Check enable/disable on a single table
    archivingClient.enableHFileBackupAsync(TABLE_NAME);
    assertTrue("Archving didn't get turned on", archivingClient
        .getArchivingEnabled(TABLE_NAME));

    // 4. Turn off archiving and make sure its off
    archivingClient.disableHFileBackup(TABLE_NAME);
    assertFalse("Archving didn't get turned off for " + STRING_TABLE_NAME,
      archivingClient.getArchivingEnabled(TABLE_NAME));
  }

  @Test
  public void testArchivingOnSingleTable() throws Exception {
    // turn on hfile retention
    LOG.debug("----Starting archiving");
    archivingClient.enableHFileBackupAsync(TABLE_NAME);
    assertTrue("Archving didn't get turned on", archivingClient
        .getArchivingEnabled(TABLE_NAME));

    // get the RS and region serving our table
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // get the parent RS and monitor
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    FileSystem fs = hrs.getFileSystem();

    // put some data on the region
    LOG.debug("-------Loading table");
    UTIL.loadRegion(region, TEST_FAM);
    loadAndCompact(region);

    // check that we actually have some store files that were archived
    Store store = region.getStore(TEST_FAM);
    Path storeArchiveDir = HFileArchiveTestingUtil.getStoreArchivePath(UTIL.getConfiguration(),
      region, store);

    // check to make sure we archived some files
    assertTrue("Didn't create a store archive directory", fs.exists(storeArchiveDir));
    assertTrue("No files in the store archive",
      FSUtils.listStatus(fs, storeArchiveDir, null).length > 0);

    // and then put some non-tables files in the archive
    Configuration conf = UTIL.getConfiguration();
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    // write a tmp file to the archive dir
    Path tmpFile = new Path(archiveDir, "toDelete");
    FSDataOutputStream out = fs.create(tmpFile);
    out.write(1);
    out.close();

    assertTrue(fs.exists(tmpFile));
    // make sure we wait long enough for the files to expire
    Thread.sleep(ttl);

    // print currrent state for comparison
    FSUtils.logFileSystemState(fs, archiveDir, LOG);

    // ensure there are no archived files after waiting for a timeout
    ensureHFileCleanersRun();

    // check to make sure the right things get deleted
    assertTrue("Store archive got deleted", fs.exists(storeArchiveDir));
    assertTrue("Archived HFiles got deleted",
      FSUtils.listStatus(fs, storeArchiveDir, null).length > 0);

    assertFalse(
      "Tmp file (non-table archive file) didn't " + "get deleted, archive dir: "
          + fs.listStatus(archiveDir), fs.exists(tmpFile));
    LOG.debug("Turning off hfile backup.");
    // stop archiving the table
    archivingClient.disableHFileBackup();
    LOG.debug("Deleting table from archive.");
    // now remove the archived table
    Path primaryTable = new Path(HFileArchiveUtil.getArchivePath(UTIL.getConfiguration()),
        STRING_TABLE_NAME);
    fs.delete(primaryTable, true);
    LOG.debug("Deleted primary table, waiting for file cleaners to run");
    // and make sure the archive directory is retained after a cleanup
    // have to do this manually since delegates aren't run if there isn't any files in the archive
    // dir to cleanup
    Thread.sleep(ttl);
    UTIL.getHBaseCluster().getMaster().getHFileCleaner().triggerNow();
    Thread.sleep(ttl);
    LOG.debug("File cleaners done, checking results.");
    // but we still have the archive directory
    assertTrue(fs.exists(HFileArchiveUtil.getArchivePath(UTIL.getConfiguration())));
  }

  /**
   * Make sure all the {@link HFileCleaner} run.
   * <p>
   * Blocking operation up to 3x ttl
   * @throws InterruptedException
   */
  private void ensureHFileCleanersRun() throws InterruptedException {
    LOG.debug("Waiting on archive cleaners to run...");
    CheckedArchivingHFileCleaner.resetCheck();
    do {
      UTIL.getHBaseCluster().getMaster().getHFileCleaner().triggerNow();
      LOG.debug("Triggered, sleeping an amount until we can pass the check.");
      Thread.sleep(ttl);
    } while (!CheckedArchivingHFileCleaner.getChecked());
  }

  /**
   * Test archiving/cleaning across multiple tables, where some are retained, and others aren't
   * @throws Exception
   */
  @Test
  public void testMultipleTables() throws Exception {
    archivingClient.enableHFileBackupAsync(TABLE_NAME);
    assertTrue("Archving didn't get turned on", archivingClient
        .getArchivingEnabled(TABLE_NAME));

    // create the another table that we don't archive
    String otherTable = "otherTable";
    UTIL.createTable(Bytes.toBytes(otherTable), TEST_FAM);

    // get the parent RS and monitor
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());

    // put data in the filesystem of the first table
    LOG.debug("Loading data into:" + STRING_TABLE_NAME);
    loadAndCompact(STRING_TABLE_NAME);

    // and some data in the other table
    LOG.debug("Loading data into:" + otherTable);
    loadAndCompact(otherTable);

    // make sure we wait long enough for the other table's files to expire
    ensureHFileCleanersRun();

    // check to make sure the right things get deleted
    Path primaryStoreArchive = HFileArchiveTestingUtil.getStoreArchivePath(UTIL, STRING_TABLE_NAME,
      TEST_FAM);
    Path otherStoreArchive = HFileArchiveTestingUtil
        .getStoreArchivePath(UTIL, otherTable, TEST_FAM);
    // make sure the primary store doesn't have any files
    assertTrue("Store archive got deleted", fs.exists(primaryStoreArchive));
    assertTrue("Archived HFiles got deleted",
      FSUtils.listStatus(fs, primaryStoreArchive, null).length > 0);
    FileStatus[] otherArchiveFiles = FSUtils.listStatus(fs, otherStoreArchive, null);
    assertNull("Archived HFiles (" + otherStoreArchive
        + ") should have gotten deleted, but didn't, remaining files:"
        + getPaths(otherArchiveFiles), otherArchiveFiles);
    // sleep again to make sure we the other table gets cleaned up
    ensureHFileCleanersRun();
    // first pass removes the store archive
    assertFalse(fs.exists(otherStoreArchive));
    // second pass removes the region
    ensureHFileCleanersRun();
    Path parent = otherStoreArchive.getParent();
    assertFalse(fs.exists(parent));
    // third pass remove the table
    ensureHFileCleanersRun();
    parent = otherStoreArchive.getParent();
    assertFalse(fs.exists(parent));
    // but we still have the archive directory
    assertTrue(fs.exists(HFileArchiveUtil.getArchivePath(UTIL.getConfiguration())));

    FSUtils.logFileSystemState(fs, HFileArchiveUtil.getArchivePath(UTIL.getConfiguration()), LOG);
    UTIL.deleteTable(Bytes.toBytes(otherTable));
  }

  private List<Path> getPaths(FileStatus[] files) {
    if (files == null || files.length == 0) return null;

    List<Path> paths = new ArrayList<Path>(files.length);
    for (FileStatus file : files) {
      paths.add(file.getPath());
    }
    return paths;
  }

  private void loadAndCompact(String tableName) throws Exception {
    byte[] table = Bytes.toBytes(tableName);
    // get the RS and region serving our table
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(table);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // get the parent RS and monitor
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(table);
    FileSystem fs = hrs.getFileSystem();

    // put some data on the region
    LOG.debug("-------Loading table");
    UTIL.loadRegion(region, TEST_FAM);
    loadAndCompact(region);

    // check that we actually have some store files that were archived
    Store store = region.getStore(TEST_FAM);
    Path storeArchiveDir = HFileArchiveTestingUtil.getStoreArchivePath(UTIL.getConfiguration(),
      region, store);

    // check to make sure we archived some files
    assertTrue("Didn't create a store archive directory", fs.exists(storeArchiveDir));
    assertTrue("No files in the store archive",
      FSUtils.listStatus(fs, storeArchiveDir, null).length > 0);

    // wait for the compactions to finish
    region.waitForFlushesAndCompactions();
  }

  /**
   * Load the given region and then ensure that it compacts some files
   */
  private void loadAndCompact(HRegion region) throws Exception {
    int tries = 0;
    Exception last = null;
    while (tries++ <= maxTries) {
      try {
        // load the region with data
        UTIL.loadRegion(region, TEST_FAM);
        // and then trigger a compaction to be sure we try to archive
        compactRegion(region, TEST_FAM);
        return;
      } catch (Exception e) {
        // keep this around for if we fail later
        last = e;
      }
    }
    throw last;
  }

  /**
   * Compact all the store files in a given region.
   */
  private void compactRegion(HRegion region, byte[] family) throws IOException {
    Store store = region.getStores().get(TEST_FAM);
    store.compactRecentForTesting(store.getStorefiles().size());
  }
}
