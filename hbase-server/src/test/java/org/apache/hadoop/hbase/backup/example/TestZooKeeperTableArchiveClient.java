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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Spin up a small cluster and check that the hfiles of region are properly long-term archived as
 * specified via the {@link ZKTableArchiveClient}.
 */
@Category(MediumTests.class)
public class TestZooKeeperTableArchiveClient {

  private static final Log LOG = LogFactory.getLog(TestZooKeeperTableArchiveClient.class);
  private static final HBaseTestingUtility UTIL = HBaseTestingUtility.createLocalHTU();
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  private static ZKTableArchiveClient archivingClient;
  private final List<Path> toCleanup = new ArrayList<Path>();

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniZKCluster();
    archivingClient = new ZKTableArchiveClient(UTIL.getConfiguration(), UTIL.getHBaseAdmin()
        .getConnection());
    // make hfile archiving node so we can archive files
    ZooKeeperWatcher watcher = UTIL.getZooKeeperWatcher();
    String archivingZNode = ZKTableArchiveClient.getArchiveZNode(UTIL.getConfiguration(), watcher);
    ZKUtil.createWithParents(watcher, archivingZNode);
  }

  private static void setupConf(Configuration conf) {
    // only compact with 3 files
    conf.setInt("hbase.hstore.compaction.min", 3);
  }

  @After
  public void tearDown() throws Exception {
    try {
      FileSystem fs = UTIL.getTestFileSystem();
      // cleanup each of the files/directories registered
      for (Path file : toCleanup) {
      // remove the table and archive directories
        FSUtils.delete(fs, file, true);
      }
    } catch (IOException e) {
      LOG.warn("Failure to delete archive directory", e);
    } finally {
      toCleanup.clear();
    }
    // make sure that backups are off for all tables
    archivingClient.disableHFileBackup();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniZKCluster();
    } catch (Exception e) {
      LOG.warn("problem shutting down cluster", e);
    }
  }

  /**
   * Test turning on/off archiving
   */
  @Test (timeout=300000)
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

  @Test (timeout=300000)
  public void testArchivingOnSingleTable() throws Exception {
    createArchiveDirectory();
    FileSystem fs = UTIL.getTestFileSystem();
    Path archiveDir = getArchiveDir();
    Path tableDir = getTableDir(STRING_TABLE_NAME);
    toCleanup.add(archiveDir);
    toCleanup.add(tableDir);

    Configuration conf = UTIL.getConfiguration();
    // setup the delegate
    Stoppable stop = new StoppableImplementation();
    HFileCleaner cleaner = setupAndCreateCleaner(conf, fs, archiveDir, stop);
    List<BaseHFileCleanerDelegate> cleaners = turnOnArchiving(STRING_TABLE_NAME, cleaner);
    final LongTermArchivingHFileCleaner delegate = (LongTermArchivingHFileCleaner) cleaners.get(0);

    // create the region
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAM);
    HRegion region = UTIL.createTestRegion(STRING_TABLE_NAME, hcd);

    loadFlushAndCompact(region, TEST_FAM);

    // get the current hfiles in the archive directory
    List<Path> files = getAllFiles(fs, archiveDir);
    if (files == null) {
      FSUtils.logFileSystemState(fs, UTIL.getDataTestDir(), LOG);
      throw new RuntimeException("Didn't archive any files!");
    }
    CountDownLatch finished = setupCleanerWatching(delegate, cleaners, files.size());

    runCleaner(cleaner, finished, stop);

    // know the cleaner ran, so now check all the files again to make sure they are still there
    List<Path> archivedFiles = getAllFiles(fs, archiveDir);
    assertEquals("Archived files changed after running archive cleaner.", files, archivedFiles);

    // but we still have the archive directory
    assertTrue(fs.exists(HFileArchiveUtil.getArchivePath(UTIL.getConfiguration())));
  }

  /**
   * Test archiving/cleaning across multiple tables, where some are retained, and others aren't
   * @throws Exception on failure
   */
  @Test (timeout=300000)
  public void testMultipleTables() throws Exception {
    createArchiveDirectory();
    String otherTable = "otherTable";

    FileSystem fs = UTIL.getTestFileSystem();
    Path archiveDir = getArchiveDir();
    Path tableDir = getTableDir(STRING_TABLE_NAME);
    Path otherTableDir = getTableDir(otherTable);

    // register cleanup for the created directories
    toCleanup.add(archiveDir);
    toCleanup.add(tableDir);
    toCleanup.add(otherTableDir);
    Configuration conf = UTIL.getConfiguration();
    // setup the delegate
    Stoppable stop = new StoppableImplementation();
    HFileCleaner cleaner = setupAndCreateCleaner(conf, fs, archiveDir, stop);
    List<BaseHFileCleanerDelegate> cleaners = turnOnArchiving(STRING_TABLE_NAME, cleaner);
    final LongTermArchivingHFileCleaner delegate = (LongTermArchivingHFileCleaner) cleaners.get(0);

    // create the region
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAM);
    HRegion region = UTIL.createTestRegion(STRING_TABLE_NAME, hcd);
    loadFlushAndCompact(region, TEST_FAM);

    // create the another table that we don't archive
    hcd = new HColumnDescriptor(TEST_FAM);
    HRegion otherRegion = UTIL.createTestRegion(otherTable, hcd);
    loadFlushAndCompact(otherRegion, TEST_FAM);

    // get the current hfiles in the archive directory
    List<Path> files = getAllFiles(fs, archiveDir);
    if (files == null) {
      FSUtils.logFileSystemState(fs, archiveDir, LOG);
      throw new RuntimeException("Didn't load archive any files!");
    }

    // make sure we have files from both tables
    int initialCountForPrimary = 0;
    int initialCountForOtherTable = 0;
    for (Path file : files) {
      String tableName = file.getParent().getParent().getParent().getName();
      // check to which table this file belongs
      if (tableName.equals(otherTable)) initialCountForOtherTable++;
      else if (tableName.equals(STRING_TABLE_NAME)) initialCountForPrimary++;
    }

    assertTrue("Didn't archive files for:" + STRING_TABLE_NAME, initialCountForPrimary > 0);
    assertTrue("Didn't archive files for:" + otherTable, initialCountForOtherTable > 0);

    // run the cleaners, checking for each of the directories + files (both should be deleted and
    // need to be checked) in 'otherTable' and the files (which should be retained) in the 'table'
    CountDownLatch finished = setupCleanerWatching(delegate, cleaners, files.size() + 3);
    // run the cleaner
    cleaner.start();
    // wait for the cleaner to check all the files
    finished.await();
    // stop the cleaner
    stop.stop("");

    // know the cleaner ran, so now check all the files again to make sure they are still there
    List<Path> archivedFiles = getAllFiles(fs, archiveDir);
    int archivedForPrimary = 0;
    for(Path file: archivedFiles) {
      String tableName = file.getParent().getParent().getParent().getName();
      // ensure we don't have files from the non-archived table
      assertFalse("Have a file from the non-archived table: " + file, tableName.equals(otherTable));
      if (tableName.equals(STRING_TABLE_NAME)) archivedForPrimary++;
    }

    assertEquals("Not all archived files for the primary table were retained.", initialCountForPrimary,
      archivedForPrimary);

    // but we still have the archive directory
    assertTrue("Archive directory was deleted via archiver", fs.exists(archiveDir));
  }


  private void createArchiveDirectory() throws IOException {
    //create the archive and test directory
    FileSystem fs = UTIL.getTestFileSystem();
    Path archiveDir = getArchiveDir();
    fs.mkdirs(archiveDir);
  }

  private Path getArchiveDir() throws IOException {
    return new Path(UTIL.getDataTestDir(), HConstants.HFILE_ARCHIVE_DIRECTORY);
  }

  private Path getTableDir(String tableName) throws IOException {
    Path testDataDir = UTIL.getDataTestDir();
    FSUtils.setRootDir(UTIL.getConfiguration(), testDataDir);
    return new Path(testDataDir, tableName);
  }

  private HFileCleaner setupAndCreateCleaner(Configuration conf, FileSystem fs, Path archiveDir,
      Stoppable stop) {
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
      LongTermArchivingHFileCleaner.class.getCanonicalName());
    return new HFileCleaner(1000, stop, conf, fs, archiveDir);
  }

  /**
   * Start archiving table for given hfile cleaner
   * @param tableName table to archive
   * @param cleaner cleaner to check to make sure change propagated
   * @return underlying {@link LongTermArchivingHFileCleaner} that is managing archiving
   * @throws IOException on failure
   * @throws KeeperException on failure
   */
  private List<BaseHFileCleanerDelegate> turnOnArchiving(String tableName, HFileCleaner cleaner)
      throws IOException, KeeperException {
    // turn on hfile retention
    LOG.debug("----Starting archiving for table:" + tableName);
    archivingClient.enableHFileBackupAsync(Bytes.toBytes(tableName));
    assertTrue("Archving didn't get turned on", archivingClient.getArchivingEnabled(tableName));

    // wait for the archiver to get the notification
    List<BaseHFileCleanerDelegate> cleaners = cleaner.getDelegatesForTesting();
    LongTermArchivingHFileCleaner delegate = (LongTermArchivingHFileCleaner) cleaners.get(0);
    while (!delegate.archiveTracker.keepHFiles(STRING_TABLE_NAME)) {
      // spin until propagation - should be fast
    }
    return cleaners;
  }

  /**
   * Spy on the {@link LongTermArchivingHFileCleaner} to ensure we can catch when the cleaner has
   * seen all the files
   * @return a {@link CountDownLatch} to wait on that releases when the cleaner has been called at
   *         least the expected number of times.
   */
  private CountDownLatch setupCleanerWatching(LongTermArchivingHFileCleaner cleaner,
      List<BaseHFileCleanerDelegate> cleaners, final int expected) {
    // replace the cleaner with one that we can can check
    BaseHFileCleanerDelegate delegateSpy = Mockito.spy(cleaner);
    final int[] counter = new int[] { 0 };
    final CountDownLatch finished = new CountDownLatch(1);
    Mockito.doAnswer(new Answer<Iterable<FileStatus>>() {

      @Override
      public Iterable<FileStatus> answer(InvocationOnMock invocation) throws Throwable {
        counter[0]++;
        LOG.debug(counter[0] + "/ " + expected + ") Wrapping call to getDeletableFiles for files: "
            + invocation.getArguments()[0]);

        @SuppressWarnings("unchecked")
        Iterable<FileStatus> ret = (Iterable<FileStatus>) invocation.callRealMethod();
        if (counter[0] >= expected) finished.countDown();
        return ret;
      }
    }).when(delegateSpy).getDeletableFiles(Mockito.anyListOf(FileStatus.class));
    cleaners.set(0, delegateSpy);

    return finished;
  }

  /**
   * Get all the files (non-directory entries) in the file system under the passed directory
   * @param dir directory to investigate
   * @return all files under the directory
   */
  private List<Path> getAllFiles(FileSystem fs, Path dir) throws IOException {
    FileStatus[] files = FSUtils.listStatus(fs, dir, null);
    if (files == null) {
      LOG.warn("No files under:" + dir);
      return null;
    }

    List<Path> allFiles = new ArrayList<Path>();
    for (FileStatus file : files) {
      if (file.isDir()) {
        List<Path> subFiles = getAllFiles(fs, file.getPath());
        if (subFiles != null) allFiles.addAll(subFiles);
        continue;
      }
      allFiles.add(file.getPath());
    }
    return allFiles;
  }

  private void loadFlushAndCompact(HRegion region, byte[] family) throws IOException {
    // create two hfiles in the region
    createHFileInRegion(region, family);
    createHFileInRegion(region, family);

    Store s = region.getStore(family);
    int count = s.getStorefilesCount();
    assertTrue("Don't have the expected store files, wanted >= 2 store files, but was:" + count,
      count >= 2);

    // compact the two files into one file to get files in the archive
    LOG.debug("Compacting stores");
    region.compactStores(true);
  }

  /**
   * Create a new hfile in the passed region
   * @param region region to operate on
   * @param columnFamily family for which to add data
   * @throws IOException
   */
  private void createHFileInRegion(HRegion region, byte[] columnFamily) throws IOException {
    // put one row in the region
    Put p = new Put(Bytes.toBytes("row"));
    p.add(columnFamily, Bytes.toBytes("Qual"), Bytes.toBytes("v1"));
    region.put(p);
    // flush the region to make a store file
    region.flushcache();
  }

  /**
   * @param cleaner
   */
  private void runCleaner(HFileCleaner cleaner, CountDownLatch finished, Stoppable stop)
      throws InterruptedException {
    // run the cleaner
    cleaner.start();
    // wait for the cleaner to check all the files
    finished.await();
    // stop the cleaner
    stop.stop("");
  }
}