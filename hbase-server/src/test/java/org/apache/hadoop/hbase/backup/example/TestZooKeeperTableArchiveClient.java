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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
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
  /** time in ms to let hfiles live in the archive */
  private static final long ttl = 1000;
  /** ms to wait for the archiver to realize it needs to change archiving state */
  private static final long WAIT_FOR_ZK_ARCHIVE_STATE_CHANGE = 100;
  private static ZKTableArchiveClient archivingClient;

  /**
   * Setup the config for the cluster
   * @throws Exception on failure
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
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test turning on/off archiving
   * @throws Exception on failure
   */
  @Test
  public void testArchivingEnableDisable() throws Exception {
    // 1. turn on hfile backups
    LOG.debug("----Starting archiving");
    archivingClient.enableHFileBackupAsync(TABLE_NAME);
    assertTrue("Archving didn't get turned on", archivingClient.getArchivingEnabled(TABLE_NAME));

    // 2. Turn off archiving and make sure its off
    archivingClient.disableHFileBackup();
    assertFalse("Archving didn't get turned off.", archivingClient.getArchivingEnabled(TABLE_NAME));

    // 3. Check enable/disable on a single table
    archivingClient.enableHFileBackupAsync(TABLE_NAME);
    assertTrue("Archving didn't get turned on", archivingClient.getArchivingEnabled(TABLE_NAME));

    // 4. Turn off archiving and make sure its off
    archivingClient.disableHFileBackup(TABLE_NAME);
    assertFalse("Archving didn't get turned off for " + STRING_TABLE_NAME,
      archivingClient.getArchivingEnabled(TABLE_NAME));
  }

  @Test
  public void testArchivingOnSingleTable() throws Exception {
    // turn on archiving for our table
    enableArchiving(STRING_TABLE_NAME);

    FileSystem fs = FSUtils.getCurrentFileSystem(UTIL.getConfiguration());

    // make sure there are files to archive
    int archived = loadAndEnsureArchivedStoreFiles(STRING_TABLE_NAME, TEST_FAM);

    waitForFilesToExpire();

    ensureAllTableFilesinArchive(STRING_TABLE_NAME, TEST_FAM, fs, archived);

    // turn off archiving
    disableArchiving(STRING_TABLE_NAME);

    // then ensure that those files are deleted after the timeout
    waitForFilesToExpire();
    ensureAllTableFilesinArchive(STRING_TABLE_NAME, TEST_FAM, fs, 0);
    // but that we still have the archive directory around
    assertTrue(fs.exists(HFileArchiveUtil.getArchivePath(UTIL.getConfiguration())));
  }

  /**
   * Test archiving/cleaning across multiple tables, where some are retained, and others aren't
   * @throws Exception
   */
  @Test
  public void testMultipleTables() throws Exception {
    // create the another table that we don't archive
    String otherTable = "otherTable";
    UTIL.createTable(Bytes.toBytes(otherTable), TEST_FAM);

    // archive the primary table
    enableArchiving(STRING_TABLE_NAME);

    FileSystem fs = FSUtils.getCurrentFileSystem(UTIL.getConfiguration());

    // make sure there are files to archive for both tables
    int primaryArchived = loadAndEnsureArchivedStoreFiles(STRING_TABLE_NAME, TEST_FAM);
    loadAndEnsureArchivedStoreFiles(otherTable, TEST_FAM);

    Path otherStoreArchive = HFileArchiveTestingUtil
        .getStoreArchivePath(UTIL, otherTable, TEST_FAM);
    // make sure we archive the primary table
    ensureAllTableFilesinArchive(STRING_TABLE_NAME, TEST_FAM, fs, primaryArchived);

    waitForFilesToExpire();

    // make sure that we didn't long-term archive the non-archive table
    assertThat(0, new HasExactFiles(fs, otherStoreArchive));

    // make sure we still archive the primary table
    ensureAllTableFilesinArchive(STRING_TABLE_NAME, TEST_FAM, fs, primaryArchived);
  }

  /**
   * Turn on hfile archiving and ensure its enabled
   * @param tableName name of the table to enable
   */
  private void enableArchiving(String tableName) throws Exception {
    LOG.debug("----Starting archiving on table:" + tableName);
    archivingClient.enableHFileBackupAsync(Bytes.toBytes(tableName));
    assertTrue("Archving didn't get turned on", archivingClient.getArchivingEnabled(tableName));
    waitForCleanerToChangeState(false);
  }

  /**
   * Turn off hfile archiving and ensure its enabled
   * @param tableName name of the table to enable
   */
  private void disableArchiving(String tableName) throws Exception {
    LOG.debug("----Disable archiving on table:" + tableName);
    archivingClient.disableHFileBackup(Bytes.toBytes(tableName));
    assertFalse("Archving didn't get turned of", archivingClient.getArchivingEnabled(tableName));
    waitForCleanerToChangeState(true);
  }

  private void waitForCleanerToChangeState(boolean wasArchiving) throws InterruptedException {
    // get the cleaner from the master
    LongTermArchivingHFileCleaner cleaner = (LongTermArchivingHFileCleaner) UTIL
        .getMiniHBaseCluster().getMaster().getHFileCleaner().getCleanerChain().get(1);
    // wait for it to switch state
    while (cleaner.isArchiving(STRING_TABLE_NAME) == wasArchiving) {
      Thread.sleep(WAIT_FOR_ZK_ARCHIVE_STATE_CHANGE);
    }
  }

  /**
   * Wait for files in the archive to expire
   */
  private void waitForFilesToExpire() throws Exception {
    // sleep long enough for archived files to expire
    Thread.sleep(ttl + 10);

    // make sure we clean the archive
    ensureHFileCleanersRun();
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
   * Load a table with a single region and compact the files (ensuring that there are some files in
   * the archived directory).
   * @param tableName name of the table to load
   * @return the number of archived store files
   * @throws Exception on failure
   */
  private int loadAndEnsureArchivedStoreFiles(String tableName, byte[] family) throws Exception {
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
    // load the region with data
    UTIL.loadRegion(region, family);
    // wait for the flushes/compactions to finish
    region.waitForFlushesAndCompactions();
    // then trigger a compaction to be sure we have files in the archive
    Store store = region.getStore(family);
    try {
      store.compactRecentForTesting(store.getStorefilesCount());
    } catch (RuntimeException e) {
      // we can ignore this, as long as we have some archived files, which is checked below
      LOG.debug("Failed to compact store:" + e.getMessage() + ", current store files:"
          + store.getStorefilesCount());
    }

    // check that we actually have some store files that were archived
    Path storeArchiveDir = HFileArchiveTestingUtil.getStoreArchivePath(UTIL.getConfiguration(),
      region, store);

    // check to make sure we archived some files
    int storeFiles = FSUtils.listStatus(fs, storeArchiveDir, null).length;
    assertTrue("Didn't create a store archive directory", fs.exists(storeArchiveDir));
    assertThat(null, new HasAnyFiles(fs, storeArchiveDir));
    return storeFiles;
  }

  /**
   * Compact all the store files in a given region. If there is only a single store file, ensures
   * that there are some archived store files (from a previous compaction) on which to test
   * archiving.
   */
  private void compactRegion(HRegion region, byte[] family) throws IOException {

  }

  private void ensureAllTableFilesinArchive(String tablename, byte[] family, FileSystem fs,
      int expectedArchiveFiles) throws Exception {
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(Bytes.toBytes(tablename));
    // make sure we only have 1 region serving this table
    assertEquals("Test doesn't support more than 1 region serving table: " + tablename + "", 1,
      servingRegions.size());
    HRegion region = servingRegions.get(0);
    Store store = region.getStore(TEST_FAM);
    Path storeArchiveDir = HFileArchiveTestingUtil.getStoreArchivePath(UTIL.getConfiguration(),
      region, store);

    // log the current fs state for the archive dir
    Path regionArchiveDir = HFileArchiveUtil.getArchivePath(UTIL.getConfiguration());
    FSUtils.logFileSystemState(fs, regionArchiveDir, LOG);

    // if we don't have store files, we shouldn't have the directory, but otherwise should have the
    // store archive directory to store the archived files
    assertEquals("Didn't create a store archive directory:" + storeArchiveDir,
      expectedArchiveFiles != 0, fs.exists(storeArchiveDir));
    assertThat(expectedArchiveFiles, new HasExactFiles(fs, storeArchiveDir));
  }

  private static class HasAnyFiles extends BaseMatcher<Object> {
    protected FileSystem fs;
    protected Path storeArchiveDir;

    public HasAnyFiles(FileSystem fs, Path storeArchiveDir) {
      this.fs = fs;
      this.storeArchiveDir = storeArchiveDir;
    }

    @Override
    public boolean matches(Object arg0) {
      try {
        return FSUtils.listStatus(fs, storeArchiveDir, null).length > 0;
      } catch (IOException e) {
        LOG.error("Failed to read the FS!", e);
        return false;
      }
    }

    @Override
    public void describeTo(Description desc) {
      desc.appendText("No store files in archive");
    }
  }

  private static class HasExactFiles extends BaseMatcher<Integer> {
    protected FileSystem fs;
    protected Path storeArchiveDir;

    public HasExactFiles(FileSystem fs, Path storeArchiveDir) {
      this.fs = fs;
      this.storeArchiveDir = storeArchiveDir;
    }

    @Override
    public boolean matches(Object arg0) {
      try {
        int expected = ((Integer) arg0).intValue();
        FileStatus[] files = FSUtils.listStatus(fs, storeArchiveDir, null);
        if (expected == 0 && files == null) {
          LOG.debug("Directory '" + storeArchiveDir + "' doesn't exist, therefore 0 files!");
          return true;
        }
        return expected == files.length;
      } catch (IOException e) {
        LOG.error("Failed to read the FS!", e);
        return false;
      }
    }

    @Override
    public void describeTo(Description desc) {
      desc.appendText("Store files in archive doesn't match expected");
    }
  }
}