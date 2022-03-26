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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that the {@link HFileArchiver} correctly removes all the parts of a region when cleaning up
 * a region
 */
@Category({LargeTests.class, MiscTests.class})
public class TestHFileArchiving {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileArchiving.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileArchiving.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");

  private static DirScanPool POOL;
  @Rule
  public TestName name = new TestName();

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster();

    // We don't want the cleaner to remove files. The tests do that.
    UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().cancel(true);

    POOL = DirScanPool.getHFileCleanerScanPool(UTIL.getConfiguration());
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
    clearArchiveDirectory();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    UTIL.shutdownMiniCluster();
    POOL.shutdownNow();
  }

  @Test
  public void testArchiveStoreFilesDifferentFileSystemsWallWithSchemaPlainRoot() throws Exception {
    String walDir = "mockFS://mockFSAuthority:9876/mockDir/wals/";
    String baseDir = CommonFSUtils.getRootDir(UTIL.getConfiguration()).toString() + "/";
    testArchiveStoreFilesDifferentFileSystems(walDir, baseDir,
      HFileArchiver::archiveStoreFiles);
  }

  @Test
  public void testArchiveStoreFilesDifferentFileSystemsWallNullPlainRoot() throws Exception {
    String baseDir = CommonFSUtils.getRootDir(UTIL.getConfiguration()).toString() + "/";
    testArchiveStoreFilesDifferentFileSystems(null, baseDir,
      HFileArchiver::archiveStoreFiles);
  }

  @Test
  public void testArchiveStoreFilesDifferentFileSystemsWallAndRootSame() throws Exception {
    String baseDir = CommonFSUtils.getRootDir(UTIL.getConfiguration()).toString() + "/";
    testArchiveStoreFilesDifferentFileSystems("/hbase/wals/", baseDir,
      HFileArchiver::archiveStoreFiles);
  }

  @Test
  public void testArchiveStoreFilesDifferentFileSystemsFileAlreadyArchived() throws Exception {
    String baseDir = CommonFSUtils.getRootDir(UTIL.getConfiguration()).toString() + "/";
    testArchiveStoreFilesDifferentFileSystems("/hbase/wals/", baseDir, true, false, false,
      HFileArchiver::archiveStoreFiles);
  }

  @Test
  public void testArchiveStoreFilesDifferentFileSystemsArchiveFileMatchCurrent() throws Exception {
    String baseDir = CommonFSUtils.getRootDir(UTIL.getConfiguration()).toString() + "/";
    testArchiveStoreFilesDifferentFileSystems("/hbase/wals/", baseDir, true, true, false,
      HFileArchiver::archiveStoreFiles);
  }

  @Test(expected = IOException.class)
  public void testArchiveStoreFilesDifferentFileSystemsArchiveFileMismatch() throws Exception {
    String baseDir = CommonFSUtils.getRootDir(UTIL.getConfiguration()).toString() + "/";
    testArchiveStoreFilesDifferentFileSystems("/hbase/wals/", baseDir, true, true, true,
      HFileArchiver::archiveStoreFiles);
  }

  private void testArchiveStoreFilesDifferentFileSystems(String walDir, String expectedBase,
    ArchivingFunction<Configuration, FileSystem, RegionInfo, Path, byte[],
      Collection<HStoreFile>> archivingFunction) throws IOException {
    testArchiveStoreFilesDifferentFileSystems(walDir, expectedBase, false, true, false,
      archivingFunction);
  }

  private void testArchiveStoreFilesDifferentFileSystems(String walDir, String expectedBase,
    boolean archiveFileExists, boolean sourceFileExists, boolean archiveFileDifferentLength,
    ArchivingFunction<Configuration, FileSystem, RegionInfo, Path, byte[],
      Collection<HStoreFile>> archivingFunction) throws IOException {
    FileSystem mockedFileSystem = mock(FileSystem.class);
    Configuration conf = new Configuration(UTIL.getConfiguration());
    if(walDir != null) {
      conf.set(CommonFSUtils.HBASE_WAL_DIR, walDir);
    }
    when(mockedFileSystem.getScheme()).thenReturn("mockFS");
    when(mockedFileSystem.mkdirs(any())).thenReturn(true);
    HashMap<Path,Boolean> existsTracker = new HashMap<>();
    Path filePath = new Path("/mockDir/wals/mockFile");
    String expectedDir = expectedBase +
      "archive/data/default/mockTable/mocked-region-encoded-name/testfamily/mockFile";
    existsTracker.put(new Path(expectedDir), archiveFileExists);
    existsTracker.put(filePath, sourceFileExists);
    when(mockedFileSystem.exists(any())).thenAnswer(invocation ->
      existsTracker.getOrDefault((Path)invocation.getArgument(0), true));
    FileStatus mockedStatus = mock(FileStatus.class);
    when(mockedStatus.getLen()).thenReturn(12L).thenReturn(archiveFileDifferentLength ? 34L : 12L);
    when(mockedFileSystem.getFileStatus(any())).thenReturn(mockedStatus);
    RegionInfo mockedRegion = mock(RegionInfo.class);
    TableName tableName = TableName.valueOf("mockTable");
    when(mockedRegion.getTable()).thenReturn(tableName);
    when(mockedRegion.getEncodedName()).thenReturn("mocked-region-encoded-name");
    Path tableDir = new Path("mockFS://mockDir/tabledir");
    byte[] family = Bytes.toBytes("testfamily");
    HStoreFile mockedFile = mock(HStoreFile.class);
    List<HStoreFile> list = new ArrayList<>();
    list.add(mockedFile);
    when(mockedFile.getPath()).thenReturn(filePath);
    when(mockedFileSystem.rename(any(),any())).thenReturn(true);
    archivingFunction.apply(conf, mockedFileSystem, mockedRegion, tableDir, family, list);

    if (sourceFileExists) {
      ArgumentCaptor<Path> srcPath = ArgumentCaptor.forClass(Path.class);
      ArgumentCaptor<Path> destPath = ArgumentCaptor.forClass(Path.class);
      if (archiveFileExists) {
        // Verify we renamed the archived file to sideline, and then renamed the source file.
        verify(mockedFileSystem, times(2)).rename(srcPath.capture(), destPath.capture());
        assertEquals(expectedDir, srcPath.getAllValues().get(0).toString());
        assertEquals(filePath, srcPath.getAllValues().get(1));
        assertEquals(expectedDir, destPath.getAllValues().get(1).toString());
      } else {
        // Verify we renamed the source file to the archived file.
        verify(mockedFileSystem, times(1)).rename(srcPath.capture(), destPath.capture());
        assertEquals(filePath, srcPath.getAllValues().get(0));
        assertEquals(expectedDir, destPath.getAllValues().get(0).toString());
      }
    } else {
      if (archiveFileExists) {
        // Verify we did not rename. No source file with a present archive file should be a no-op.
        verify(mockedFileSystem, never()).rename(any(), any());
      } else {
        fail("Unsupported test conditions: sourceFileExists and archiveFileExists both false.");
      }
    }
  }

  @FunctionalInterface
  private interface ArchivingFunction<Configuration, FS, Region, Dir, Family, Files> {
    void apply(Configuration config, FS fs, Region region, Dir dir, Family family, Files files)
      throws IOException;
  }

  @Test
  public void testArchiveRecoveredEditsWalDirNull() throws Exception {
    testArchiveRecoveredEditsWalDirNullOrSame(null);
  }

  @Test
  public void testArchiveRecoveredEditsWalDirSameFsStoreFiles() throws Exception {
    testArchiveRecoveredEditsWalDirNullOrSame("/wal-dir");
  }

  private void testArchiveRecoveredEditsWalDirNullOrSame(String walDir) throws Exception {
    String originalRootDir = UTIL.getConfiguration().get(HConstants.HBASE_DIR);
    try {
      String baseDir = "mockFS://mockFSAuthority:9876/hbase/";
      UTIL.getConfiguration().set(HConstants.HBASE_DIR, baseDir);
      testArchiveStoreFilesDifferentFileSystems(walDir, baseDir,
        (conf, fs, region, dir, family, list) -> HFileArchiver
          .archiveRecoveredEdits(conf, fs, region, family, list));
    } finally {
      UTIL.getConfiguration().set(HConstants.HBASE_DIR, originalRootDir);
    }
  }

  @Test(expected = IOException.class)
  public void testArchiveRecoveredEditsWrongFS() throws Exception {
    String baseDir = CommonFSUtils.getRootDir(UTIL.getConfiguration()).toString() + "/";
    //Internally, testArchiveStoreFilesDifferentFileSystems will pass a "mockedFS"
    // to HFileArchiver.archiveRecoveredEdits, but since wal-dir is supposedly on same FS
    // as root dir it would lead to conflicting FSes and an IOException is expected.
    testArchiveStoreFilesDifferentFileSystems("/wal-dir", baseDir,
      (conf, fs, region, dir, family, list) -> HFileArchiver
        .archiveRecoveredEdits(conf, fs, region, family, list));
  }

  @Test
  public void testArchiveRecoveredEditsWalDirDifferentFS() throws Exception {
    String walDir = "mockFS://mockFSAuthority:9876/mockDir/wals/";
    testArchiveStoreFilesDifferentFileSystems(walDir, walDir,
      (conf, fs, region, dir, family, list) ->
        HFileArchiver.archiveRecoveredEdits(conf, fs, region, family, list));
  }

  @Test
  public void testRemoveRegionDirOnArchive() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, TEST_FAM);

    final Admin admin = UTIL.getAdmin();

    // get the current store files for the region
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(tableName);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // and load the table
    UTIL.loadRegion(region, TEST_FAM);

    // shutdown the table so we can manipulate the files
    admin.disableTable(tableName);

    FileSystem fs = UTIL.getTestFileSystem();

    // now attempt to depose the region
    Path rootDir = region.getRegionFileSystem().getTableDir().getParent();
    Path regionDir = FSUtils.getRegionDirFromRootDir(rootDir, region.getRegionInfo());

    HFileArchiver.archiveRegion(UTIL.getConfiguration(), fs, region.getRegionInfo());

    // check for the existence of the archive directory and some files in it
    Path archiveDir = HFileArchiveTestingUtil.getRegionArchiveDir(UTIL.getConfiguration(), region);
    assertTrue(fs.exists(archiveDir));

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

    UTIL.deleteTable(tableName);
  }

  /**
   * Test that the region directory is removed when we archive a region without store files, but
   * still has hidden files.
   * @throws IOException throws an IOException if there's problem creating a table
   *   or if there's an issue with accessing FileSystem.
   */
  @Test
  public void testDeleteRegionWithNoStoreFiles() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, TEST_FAM);

    // get the current store files for the region
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(tableName);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    FileSystem fs = region.getRegionFileSystem().getFileSystem();

    // make sure there are some files in the regiondir
    Path rootDir = CommonFSUtils.getRootDir(fs.getConf());
    Path regionDir = FSUtils.getRegionDirFromRootDir(rootDir, region.getRegionInfo());
    FileStatus[] regionFiles = CommonFSUtils.listStatus(fs, regionDir, null);
    Assert.assertNotNull("No files in the region directory", regionFiles);
    if (LOG.isDebugEnabled()) {
      List<Path> files = new ArrayList<>();
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
        return dirFilter.accept(file) && !file.getName().startsWith(".");
      }
    };
    FileStatus[] storeDirs = CommonFSUtils.listStatus(fs, regionDir, nonHidden);
    for (FileStatus store : storeDirs) {
      LOG.debug("Deleting store for test");
      fs.delete(store.getPath(), true);
    }

    // then archive the region
    HFileArchiver.archiveRegion(UTIL.getConfiguration(), fs, region.getRegionInfo());

    // and check to make sure the region directoy got deleted
    assertFalse("Region directory (" + regionDir + "), still exists.", fs.exists(regionDir));

    UTIL.deleteTable(tableName);
  }

  private List<HRegion> initTableForArchivingRegions(TableName tableName) throws IOException {
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("b"), Bytes.toBytes("c"), Bytes.toBytes("d")
    };

    UTIL.createTable(tableName, TEST_FAM, splitKeys);

    // get the current store files for the regions
    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
    // make sure we have 4 regions serving this table
    assertEquals(4, regions.size());

    // and load the table
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      UTIL.loadTable(table, TEST_FAM);
    }

    // disable the table so that we can manipulate the files
    UTIL.getAdmin().disableTable(tableName);

    return regions;
  }

  @Test
  public void testArchiveRegions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<HRegion> regions = initTableForArchivingRegions(tableName);

    FileSystem fs = UTIL.getTestFileSystem();

    // now attempt to depose the regions
    Path rootDir = CommonFSUtils.getRootDir(UTIL.getConfiguration());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, regions.get(0).getRegionInfo().getTable());
    List<Path> regionDirList = regions.stream()
      .map(region -> FSUtils.getRegionDirFromTableDir(tableDir, region.getRegionInfo()))
      .collect(Collectors.toList());

    HFileArchiver.archiveRegions(UTIL.getConfiguration(), fs, rootDir, tableDir, regionDirList);

    // check for the existence of the archive directory and some files in it
    for (HRegion region : regions) {
      Path archiveDir = HFileArchiveTestingUtil.getRegionArchiveDir(UTIL.getConfiguration(),
        region);
      assertTrue(fs.exists(archiveDir));

      // check to make sure the store directory was copied
      FileStatus[] stores = fs.listStatus(archiveDir,
        p -> !p.getName().contains(HConstants.RECOVERED_EDITS_DIR));
      assertTrue(stores.length == 1);

      // make sure we archived the store files
      FileStatus[] storeFiles = fs.listStatus(stores[0].getPath());
      assertTrue(storeFiles.length > 0);
    }

    // then ensure the region's directories aren't present
    for (Path regionDir: regionDirList) {
      assertFalse(fs.exists(regionDir));
    }

    UTIL.deleteTable(tableName);
  }

  @Test(expected=IOException.class)
  public void testArchiveRegionsWhenPermissionDenied() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<HRegion> regions = initTableForArchivingRegions(tableName);

    // now attempt to depose the regions
    Path rootDir = CommonFSUtils.getRootDir(UTIL.getConfiguration());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, regions.get(0).getRegionInfo().getTable());
    List<Path> regionDirList = regions.stream()
      .map(region -> FSUtils.getRegionDirFromTableDir(tableDir, region.getRegionInfo()))
      .collect(Collectors.toList());

    // To create a permission denied error, we do archive regions as a non-current user
    UserGroupInformation
      ugi = UserGroupInformation.createUserForTesting("foo1234", new String[]{"group1"});

    try {
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        FileSystem fs = UTIL.getTestFileSystem();
        HFileArchiver.archiveRegions(UTIL.getConfiguration(), fs, rootDir, tableDir,
          regionDirList);
        return null;
      });
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("Permission denied"));
      throw e;
    } finally {
      UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testArchiveOnTableDelete() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, TEST_FAM);

    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(tableName);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // get the parent RS and monitor
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(tableName);
    FileSystem fs = hrs.getFileSystem();

    // put some data on the region
    LOG.debug("-------Loading table");
    UTIL.loadRegion(region, TEST_FAM);

    // get the hfiles in the region
    List<HRegion> regions = hrs.getRegions(tableName);
    assertEquals("More that 1 region for test table.", 1, regions.size());

    region = regions.get(0);
    // wait for all the compactions to complete
    region.waitForFlushesAndCompactions();

    // disable table to prevent new updates
    UTIL.getAdmin().disableTable(tableName);
    LOG.debug("Disabled table");

    // remove all the files from the archive to get a fair comparison
    clearArchiveDirectory();

    // then get the current store files
    byte[][]columns = region.getTableDescriptor().getColumnFamilyNames().toArray(new byte[0][]);
    List<String> storeFiles = region.getStoreFileList(columns);

    // then delete the table so the hfiles get archived
    UTIL.deleteTable(tableName);
    LOG.debug("Deleted table");

    assertArchiveFiles(fs, storeFiles, 30000);
  }

  private void assertArchiveFiles(FileSystem fs, List<String> storeFiles, long timeout)
          throws IOException {
    long end = EnvironmentEdgeManager.currentTime() + timeout;
    Path archiveDir = HFileArchiveUtil.getArchivePath(UTIL.getConfiguration());
    List<String> archivedFiles = new ArrayList<>();

    // We have to ensure that the DeleteTableHandler is finished. HBaseAdmin.deleteXXX()
    // can return before all files
    // are archived. We should fix HBASE-5487 and fix synchronous operations from admin.
    while (EnvironmentEdgeManager.currentTime() < end) {
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
   * @throws java.io.IOException if there's a problem creating a table.
   * @throws java.lang.InterruptedException problem getting a RegionServer.
   */
  @Test
  public void testArchiveOnTableFamilyDelete() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, new byte[][] {TEST_FAM, Bytes.toBytes("fam2")});

    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(tableName);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // get the parent RS and monitor
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(tableName);
    FileSystem fs = hrs.getFileSystem();

    // put some data on the region
    LOG.debug("-------Loading table");
    UTIL.loadRegion(region, TEST_FAM);

    // get the hfiles in the region
    List<HRegion> regions = hrs.getRegions(tableName);
    assertEquals("More that 1 region for test table.", 1, regions.size());

    region = regions.get(0);
    // wait for all the compactions to complete
    region.waitForFlushesAndCompactions();

    // disable table to prevent new updates
    UTIL.getAdmin().disableTable(tableName);
    LOG.debug("Disabled table");

    // remove all the files from the archive to get a fair comparison
    clearArchiveDirectory();

    // then get the current store files
    byte[][]columns = region.getTableDescriptor().getColumnFamilyNames().toArray(new byte[0][]);
    List<String> storeFiles = region.getStoreFileList(columns);

    // then delete the table so the hfiles get archived
    UTIL.getAdmin().deleteColumnFamily(tableName, TEST_FAM);

    assertArchiveFiles(fs, storeFiles, 30000);

    UTIL.deleteTable(tableName);
  }

  /**
   * Test HFileArchiver.resolveAndArchive() race condition HBASE-7643
   */
  @Test
  public void testCleaningRace() throws Exception {
    final long TEST_TIME = 20 * 1000;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");

    Configuration conf = UTIL.getMiniHBaseCluster().getMaster().getConfiguration();
    Path rootDir = UTIL.getDataTestDirOnTestFS("testCleaningRace");
    FileSystem fs = UTIL.getTestFileSystem();

    Path archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    Path regionDir = new Path(CommonFSUtils.getTableDir(new Path("./"),
        TableName.valueOf(name.getMethodName())), "abcdef");
    Path familyDir = new Path(regionDir, "cf");

    Path sourceRegionDir = new Path(rootDir, regionDir);
    fs.mkdirs(sourceRegionDir);

    Stoppable stoppable = new StoppableImplementation();

    // The cleaner should be looping without long pauses to reproduce the race condition.
    HFileCleaner cleaner = getHFileCleaner(stoppable, conf, fs, archiveDir);
    assertNotNull("cleaner should not be null", cleaner);
    try {
      choreService.scheduleChore(cleaner);
      // Keep creating/archiving new files while the cleaner is running in the other thread
      long startTime = EnvironmentEdgeManager.currentTime();
      for (long fid = 0; (EnvironmentEdgeManager.currentTime() - startTime) < TEST_TIME; ++fid) {
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
      cleaner.cancel(true);
      choreService.shutdown();
      fs.delete(rootDir, true);
    }
  }

  @Test
  public void testArchiveRegionTableAndRegionDirsNull() throws IOException {
    Path rootDir = UTIL.getDataTestDirOnTestFS("testCleaningRace");
    FileSystem fileSystem = UTIL.getTestFileSystem();
    // Try to archive the file but with null regionDir, can't delete sourceFile
    assertFalse(HFileArchiver.archiveRegion(fileSystem, rootDir, null, null));
  }

  @Test
  public void testArchiveRegionWithTableDirNull() throws IOException {
    Path regionDir = new Path(CommonFSUtils.getTableDir(new Path("./"),
            TableName.valueOf(name.getMethodName())), "xyzabc");
    Path familyDir = new Path(regionDir, "rd");
    Path rootDir = UTIL.getDataTestDirOnTestFS("testCleaningRace");
    Path file = new Path(familyDir, "1");
    Path sourceFile = new Path(rootDir, file);
    FileSystem fileSystem = UTIL.getTestFileSystem();
    fileSystem.createNewFile(sourceFile);
    Path sourceRegionDir = new Path(rootDir, regionDir);
    fileSystem.mkdirs(sourceRegionDir);
    // Try to archive the file
    assertFalse(HFileArchiver.archiveRegion(fileSystem, rootDir, null, sourceRegionDir));
    assertFalse(fileSystem.exists(sourceRegionDir));
  }

  @Test
  public void testArchiveRegionWithRegionDirNull() throws IOException {
    Path regionDir = new Path(CommonFSUtils.getTableDir(new Path("./"),
            TableName.valueOf(name.getMethodName())), "elgn4nf");
    Path familyDir = new Path(regionDir, "rdar");
    Path rootDir = UTIL.getDataTestDirOnTestFS("testCleaningRace");
    Path file = new Path(familyDir, "2");
    Path sourceFile = new Path(rootDir, file);
    FileSystem fileSystem = UTIL.getTestFileSystem();
    fileSystem.createNewFile(sourceFile);
    Path sourceRegionDir = new Path(rootDir, regionDir);
    fileSystem.mkdirs(sourceRegionDir);
    // Try to archive the file but with null regionDir, can't delete sourceFile
    assertFalse(HFileArchiver.archiveRegion(fileSystem, rootDir, sourceRegionDir.getParent(),
            null));
    assertTrue(fileSystem.exists(sourceRegionDir));
    fileSystem.delete(sourceRegionDir, true);
  }

  // Avoid passing a null master to CleanerChore, see HBASE-21175
  private HFileCleaner getHFileCleaner(Stoppable stoppable, Configuration conf, FileSystem fs,
    Path archiveDir) throws IOException {
    Map<String, Object> params = new HashMap<>();
    params.put(HMaster.MASTER, UTIL.getMiniHBaseCluster().getMaster());
    HFileCleaner cleaner = new HFileCleaner(1, stoppable, conf, fs, archiveDir, POOL);
    return Objects.requireNonNull(cleaner);
  }

  private void clearArchiveDirectory() throws IOException {
    UTIL.getTestFileSystem().delete(
      new Path(UTIL.getDefaultRootDirPath(), HConstants.HFILE_ARCHIVE_DIRECTORY), true);
  }

  /**
   * Get the names of all the files below the given directory
   * @param fs the file system to inspect
   * @param archiveDir the directory in which to look
   * @return a list of all files in the directory and sub-directories
   * @throws java.io.IOException throws IOException in case FS is unavailable
   */
  private List<String> getAllFileNames(final FileSystem fs, Path archiveDir) throws IOException  {
    FileStatus[] files = CommonFSUtils.listStatus(fs, archiveDir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        if (p.getName().contains(HConstants.RECOVERED_EDITS_DIR)) {
          return false;
        }
        return true;
      }
    });
    return recurseOnFiles(fs, files, new ArrayList<>());
  }

  /** Recursively lookup all the file names under the file[] array **/
  private List<String> recurseOnFiles(FileSystem fs, FileStatus[] files, List<String> fileNames)
      throws IOException {
    if (files == null || files.length == 0) {
      return fileNames;
    }

    for (FileStatus file : files) {
      if (file.isDirectory()) {
        recurseOnFiles(fs, CommonFSUtils.listStatus(fs, file.getPath(), null), fileNames);
      } else {
        fileNames.add(file.getPath().getName());
      }
    }
    return fileNames;
  }
}
