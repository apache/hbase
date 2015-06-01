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
package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that the {@link HFileArchiver} correctly removes all the parts of a region when cleaning up
 * a region
 */
@Category(MediumTests.class)
public class TestHFileArchiving {

  private static final Log LOG = LogFactory.getLog(TestHFileArchiving.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster();

    // We don't want the cleaner to remove files. The tests do that.
    UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().interrupt();
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // drop the memstore size so we get flushes
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // disable major compactions
    conf.setInt(HConstants.MAJOR_COMPACTION_PERIOD, 0);

    // prevent aggressive region split
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
  }

  @After
  public void tearDown() throws Exception {
    // cleanup the archive directory
    try {
      clearArchiveDirectory();
    } catch (IOException e) {
      Assert.fail("Failure to delete archive directory:" + e.getMessage());
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

  @Test
  public void testRemovesRegionDirOnArchive() throws Exception {
    TableName TABLE_NAME =
        TableName.valueOf("testRemovesRegionDirOnArchive");
    UTIL.createTable(TABLE_NAME, TEST_FAM);

    final Admin admin = UTIL.getHBaseAdmin();

    // get the current store files for the region
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // and load the table
    UTIL.loadRegion(region, TEST_FAM);

    // shutdown the table so we can manipulate the files
    admin.disableTable(TABLE_NAME);

    FileSystem fs = UTIL.getTestFileSystem();

    // now attempt to depose the region
    Path rootDir = region.getRegionFileSystem().getTableDir().getParent();
    Path regionDir = HRegion.getRegionDir(rootDir, region.getRegionInfo());

    HFileArchiver.archiveRegion(UTIL.getConfiguration(), fs, region.getRegionInfo());

    // check for the existence of the archive directory and some files in it
    Path archiveDir = HFileArchiveTestingUtil.getRegionArchiveDir(UTIL.getConfiguration(), region);
    assertTrue(fs.exists(archiveDir));

    // check to make sure the store directory was copied
    // check to make sure the store directory was copied
    FileStatus[] stores = fs.listStatus(archiveDir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        if (p.getName().contains(HConstants.RECOVERED_EDITS_DIR)) {
          return false;
        }
        return true;
      }
    });
    assertTrue(stores.length == 1);

    // make sure we archived the store files
    FileStatus[] storeFiles = fs.listStatus(stores[0].getPath());
    assertTrue(storeFiles.length > 0);

    // then ensure the region's directory isn't present
    assertFalse(fs.exists(regionDir));

    UTIL.deleteTable(TABLE_NAME);
  }

  /**
   * Test that the region directory is removed when we archive a region without store files, but
   * still has hidden files.
   * @throws Exception
   */
  @Test
  public void testDeleteRegionWithNoStoreFiles() throws Exception {
    TableName TABLE_NAME =
        TableName.valueOf("testDeleteRegionWithNoStoreFiles");
    UTIL.createTable(TABLE_NAME, TEST_FAM);

    // get the current store files for the region
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    FileSystem fs = region.getRegionFileSystem().getFileSystem();

    // make sure there are some files in the regiondir
    Path rootDir = FSUtils.getRootDir(fs.getConf());
    Path regionDir = HRegion.getRegionDir(rootDir, region.getRegionInfo());
    FileStatus[] regionFiles = FSUtils.listStatus(fs, regionDir, null);
    Assert.assertNotNull("No files in the region directory", regionFiles);
    if (LOG.isDebugEnabled()) {
      List<Path> files = new ArrayList<Path>();
      for (FileStatus file : regionFiles) {
        files.add(file.getPath());
      }
      LOG.debug("Current files:" + files);
    }
    // delete the visible folders so we just have hidden files/folders
    final PathFilter dirFilter = new FSUtils.DirFilter(fs);
    PathFilter nonHidden = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        return dirFilter.accept(file) && !file.getName().toString().startsWith(".");
      }
    };
    FileStatus[] storeDirs = FSUtils.listStatus(fs, regionDir, nonHidden);
    for (FileStatus store : storeDirs) {
      LOG.debug("Deleting store for test");
      fs.delete(store.getPath(), true);
    }

    // then archive the region
    HFileArchiver.archiveRegion(UTIL.getConfiguration(), fs, region.getRegionInfo());

    // and check to make sure the region directoy got deleted
    assertFalse("Region directory (" + regionDir + "), still exists.", fs.exists(regionDir));

    UTIL.deleteTable(TABLE_NAME);
  }

  @Test
  public void testArchiveOnTableDelete() throws Exception {
    TableName TABLE_NAME =
        TableName.valueOf("testArchiveOnTableDelete");
    UTIL.createTable(TABLE_NAME, TEST_FAM);

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

    // get the hfiles in the region
    List<HRegion> regions = hrs.getOnlineRegions(TABLE_NAME);
    assertEquals("More that 1 region for test table.", 1, regions.size());

    region = regions.get(0);
    // wait for all the compactions to complete
    region.waitForFlushesAndCompactions();

    // disable table to prevent new updates
    UTIL.getHBaseAdmin().disableTable(TABLE_NAME);
    LOG.debug("Disabled table");

    // remove all the files from the archive to get a fair comparison
    clearArchiveDirectory();

    // then get the current store files
    List<String> storeFiles = getRegionStoreFiles(region);

    // then delete the table so the hfiles get archived
    UTIL.deleteTable(TABLE_NAME);
    LOG.debug("Deleted table");

    assertArchiveFiles(fs, storeFiles, 30000);
  }

  private void assertArchiveFiles(FileSystem fs, List<String> storeFiles, long timeout) throws IOException {
    long end = System.currentTimeMillis() + timeout;
    Path archiveDir = HFileArchiveUtil.getArchivePath(UTIL.getConfiguration());
    List<String> archivedFiles = new ArrayList<String>();

    // We have to ensure that the DeleteTableHandler is finished. HBaseAdmin.deleteXXX() can return before all files
    // are archived. We should fix HBASE-5487 and fix synchronous operations from admin.
    while (System.currentTimeMillis() < end) {
      archivedFiles = getAllFileNames(fs, archiveDir);
      if (archivedFiles.size() >= storeFiles.size()) {
        break;
      }
    }

    Collections.sort(storeFiles);
    Collections.sort(archivedFiles);

    LOG.debug("Store files:");
    for (int i = 0; i < storeFiles.size(); i++) {
      LOG.debug(i + " - " + storeFiles.get(i));
    }
    LOG.debug("Archive files:");
    for (int i = 0; i < archivedFiles.size(); i++) {
      LOG.debug(i + " - " + archivedFiles.get(i));
    }

    assertTrue("Archived files are missing some of the store files!",
      archivedFiles.containsAll(storeFiles));
  }


  /**
   * Test that the store files are archived when a column family is removed.
   * @throws Exception
   */
  @Test
  public void testArchiveOnTableFamilyDelete() throws Exception {
    TableName TABLE_NAME =
        TableName.valueOf("testArchiveOnTableFamilyDelete");
    UTIL.createTable(TABLE_NAME, new byte[][] {TEST_FAM, Bytes.toBytes("fam2")});

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

    // get the hfiles in the region
    List<HRegion> regions = hrs.getOnlineRegions(TABLE_NAME);
    assertEquals("More that 1 region for test table.", 1, regions.size());

    region = regions.get(0);
    // wait for all the compactions to complete
    region.waitForFlushesAndCompactions();

    // disable table to prevent new updates
    UTIL.getHBaseAdmin().disableTable(TABLE_NAME);
    LOG.debug("Disabled table");

    // remove all the files from the archive to get a fair comparison
    clearArchiveDirectory();

    // then get the current store files
    List<String> storeFiles = getRegionStoreFiles(region);

    // then delete the table so the hfiles get archived
    UTIL.getHBaseAdmin().deleteColumn(TABLE_NAME, TEST_FAM);

    assertArchiveFiles(fs, storeFiles, 30000);

    UTIL.deleteTable(TABLE_NAME);
  }

  /**
   * Test HFileArchiver.resolveAndArchive() race condition HBASE-7643
   */
  @Test
  public void testCleaningRace() throws Exception {
    final long TEST_TIME = 20 * 1000;

    Configuration conf = UTIL.getMiniHBaseCluster().getMaster().getConfiguration();
    Path rootDir = UTIL.getDataTestDirOnTestFS("testCleaningRace");
    FileSystem fs = UTIL.getTestFileSystem();

    Path archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    Path regionDir = new Path(FSUtils.getTableDir(new Path("./"),
        TableName.valueOf("table")), "abcdef");
    Path familyDir = new Path(regionDir, "cf");

    Path sourceRegionDir = new Path(rootDir, regionDir);
    fs.mkdirs(sourceRegionDir);

    Stoppable stoppable = new StoppableImplementation();

    // The cleaner should be looping without long pauses to reproduce the race condition.
    HFileCleaner cleaner = new HFileCleaner(1, stoppable, conf, fs, archiveDir);
    try {
      cleaner.start();

      // Keep creating/archiving new files while the cleaner is running in the other thread
      long startTime = System.currentTimeMillis();
      for (long fid = 0; (System.currentTimeMillis() - startTime) < TEST_TIME; ++fid) {
        Path file = new Path(familyDir,  String.valueOf(fid));
        Path sourceFile = new Path(rootDir, file);
        Path archiveFile = new Path(archiveDir, file);

        fs.createNewFile(sourceFile);

        try {
          // Try to archive the file
          HFileArchiver.archiveRegion(fs, rootDir,
              sourceRegionDir.getParent(), sourceRegionDir);

          // The archiver succeded, the file is no longer in the original location
          // but it's in the archive location.
          LOG.debug("hfile=" + fid + " should be in the archive");
          assertTrue(fs.exists(archiveFile));
          assertFalse(fs.exists(sourceFile));
        } catch (IOException e) {
          // The archiver is unable to archive the file. Probably HBASE-7643 race condition.
          // in this case, the file should not be archived, and we should have the file
          // in the original location.
          LOG.debug("hfile=" + fid + " should be in the source location");
          assertFalse(fs.exists(archiveFile));
          assertTrue(fs.exists(sourceFile));

          // Avoid to have this file in the next run
          fs.delete(sourceFile, false);
        }
      }
    } finally {
      stoppable.stop("test end");
      cleaner.join();
      fs.delete(rootDir, true);
    }
  }

  private void clearArchiveDirectory() throws IOException {
    UTIL.getTestFileSystem().delete(
      new Path(UTIL.getDefaultRootDirPath(), HConstants.HFILE_ARCHIVE_DIRECTORY), true);
  }

  /**
   * Get the names of all the files below the given directory
   * @param fs
   * @param archiveDir
   * @return
   * @throws IOException
   */
  private List<String> getAllFileNames(final FileSystem fs, Path archiveDir) throws IOException {
    FileStatus[] files = FSUtils.listStatus(fs, archiveDir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        if (p.getName().contains(HConstants.RECOVERED_EDITS_DIR)) {
          return false;
        }
        return true;
      }
    });
    return recurseOnFiles(fs, files, new ArrayList<String>());
  }

  /** Recursively lookup all the file names under the file[] array **/
  private List<String> recurseOnFiles(FileSystem fs, FileStatus[] files, List<String> fileNames)
      throws IOException {
    if (files == null || files.length == 0) return fileNames;

    for (FileStatus file : files) {
      if (file.isDirectory()) {
        recurseOnFiles(fs, FSUtils.listStatus(fs, file.getPath(), null), fileNames);
      } else fileNames.add(file.getPath().getName());
    }
    return fileNames;
  }

  private List<String> getRegionStoreFiles(final HRegion region) throws IOException {
    Path regionDir = region.getRegionFileSystem().getRegionDir();
    FileSystem fs = region.getRegionFileSystem().getFileSystem();
    List<String> storeFiles = getAllFileNames(fs, regionDir);
    // remove all the non-storefile named files for the region
    for (int i = 0; i < storeFiles.size(); i++) {
      String file = storeFiles.get(i);
      if (file.contains(HRegionFileSystem.REGION_INFO_FILE) || file.contains("wal")) {
        storeFiles.remove(i--);
      }
    }
    storeFiles.remove(HRegionFileSystem.REGION_INFO_FILE);
    return storeFiles;
  }
}
