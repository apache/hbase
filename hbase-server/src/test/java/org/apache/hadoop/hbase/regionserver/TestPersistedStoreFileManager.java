/*
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.ListUtils;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestPersistedStoreFileManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPersistedStoreFileManager.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] DEFAULT_STORE_BYTE = TEST_UTIL.fam1;
  private static final String DEFAULT_STORE_NAME = Bytes.toString(DEFAULT_STORE_BYTE);
  private static final ArrayList<HStoreFile> EMPTY_LIST = new ArrayList<>();
  private static final CellComparator DEFAULT_CELL_COMPARATOR = new CellComparatorImpl();
  private static final Comparator<HStoreFile> COMPARATOR = StoreFileComparators.SEQ_ID;

  private Path baseDir;
  private FileSystem fs;
  private PersistedStoreFileManager storeFileManager;
  private StoreFilePathAccessor storeFilePathAccessor;
  private Configuration conf;
  private HRegion region;
  private HRegionFileSystem regionFS;
  private List<HStoreFile> initialStoreFiles;
  private List<HStoreFile> sortedInitialStoreFiles;
  private List<HStoreFile> additionalStoreFiles;
  private List<HStoreFile> sortedAdditionalStoreFiles;
  private List<HStoreFile> sortedCombinedStoreFiles;
  private List<Path> initialStorePaths;
  private List<Path> additionalStorePaths;
  private TableName tableName;
  private String regionName;
  private TableDescriptor htd;
  private RegionInfo regioninfo;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException, InterruptedException {
    conf = TEST_UTIL.getConfiguration();
    baseDir = TEST_UTIL.getDataTestDirOnTestFS();
    tableName = TableName.valueOf(name.getMethodName());
    htd = TEST_UTIL.createTableDescriptor(tableName, DEFAULT_STORE_BYTE);
    regioninfo = RegionInfoBuilder.newBuilder(tableName).build();
    region = TEST_UTIL.createRegionAndWAL(regioninfo, baseDir, conf, htd);
    regionFS = region.getRegionFileSystem();
    fs = TEST_UTIL.getTestFileSystem();
    initialStoreFiles = createStoreFilesList();
    sortedInitialStoreFiles = ImmutableList.sortedCopyOf(COMPARATOR, initialStoreFiles);
    additionalStoreFiles = createStoreFilesList();
    sortedAdditionalStoreFiles = ImmutableList.sortedCopyOf(COMPARATOR, additionalStoreFiles);
    sortedCombinedStoreFiles = ImmutableList.sortedCopyOf(COMPARATOR,
      ListUtils.union(initialStoreFiles, additionalStoreFiles));
    initialStorePaths = createPathList();
    additionalStorePaths = createPathList();
    regionName = region.getRegionInfo().getEncodedName();

    storeFilePathAccessor =
      new HTableStoreFilePathAccessor(conf, TEST_UTIL.getAdmin().getConnection());
    // the hbase:storefile should be created in master startup, but we initialize it here for
    // unit tests
    storeFilePathAccessor.initialize(TEST_UTIL.getHBaseCluster().getMaster());

    storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), regionFS, regioninfo,
        DEFAULT_STORE_NAME, storeFilePathAccessor);

    verifyStoreFileManagerWhenStarts();
  }

  @After
  public void after() throws IOException {
    storeFilePathAccessor
      .deleteStoreFilePaths(tableName.getNameAsString(), regionName, DEFAULT_STORE_NAME);
  }

  @Test
  public void testLoadFiles() throws IOException {
    storeFileManager.loadFiles(initialStoreFiles);
    compareIncludedInManagerVsTable(sortedInitialStoreFiles);
  }

  @Test
  public void testLoadFiles_WithReadOnly() throws IOException {
    storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), regionFS, regioninfo,
        DEFAULT_STORE_NAME, storeFilePathAccessor, true);
    storeFileManager.loadFiles(initialStoreFiles);
    compareIncludedInManagerVsTable(sortedInitialStoreFiles, EMPTY_LIST);
  }

  @Test
  public void testLoadFilesWithEmptyList() throws IOException {
    storeFileManager.loadFiles(initialStoreFiles);

    // writing empty list to loadFiles will not fail but it's not doing anything
    // and mostly this is expected when a fresh region is created.
    storeFileManager.loadFiles(EMPTY_LIST);
    compareIncludedInManagerVsTable(sortedInitialStoreFiles);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLoadFilesWithNull() throws IOException {
    storeFileManager.loadFiles(initialStoreFiles);
    storeFileManager.loadFiles(null);
  }

  @Test
  public void testInsertNewFiles() throws IOException {
    storeFileManager.insertNewFiles(initialStoreFiles);
    compareIncludedInManagerVsTable(sortedInitialStoreFiles);
    storeFileManager.insertNewFiles(additionalStoreFiles);
    compareIncludedInManagerVsTable(sortedCombinedStoreFiles);
  }

  @Test
  public void testLoadInitialFilesWithNoData() throws IOException {
    assertNull(storeFileManager.loadInitialFiles());
  }

  @Test
  public void testLoadInitialFiles() throws IOException {
    StoreFilePathAccessor mockStoreFilePathAccessor = Mockito.mock(StoreFilePathAccessor.class);
    HRegionFileSystem mockFs = Mockito.spy(regionFS);
    PersistedStoreFileManager storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), mockFs, regioninfo,
        DEFAULT_STORE_NAME, mockStoreFilePathAccessor);

    // make sure the tracking table is not empty and return the list of initialStoreFiles
    List<Path> storeFilePaths =
      StorefileTrackingUtils.convertStoreFilesToPaths(initialStoreFiles);
    when(mockStoreFilePathAccessor
      .getIncludedStoreFilePaths(tableName.getNameAsString(), regionName, DEFAULT_STORE_NAME))
      .thenReturn(storeFilePaths);

    Collection<StoreFileInfo> expectedStoreFileInfos =
      convertToStoreFileInfos(mockFs.getFileSystem(), initialStoreFiles);
    Collection<StoreFileInfo> actualStoreFileInfos = storeFileManager.loadInitialFiles();
    verify(mockFs, times(0)).getStoreFiles(DEFAULT_STORE_NAME);
    assertEquals(expectedStoreFileInfos, actualStoreFileInfos);
  }

  @Test
  public void testLoadInitialFilesWithRefreshFileSystem() throws IOException {
    StoreFilePathAccessor mockStoreFilePathAccessor = Mockito.mock(StoreFilePathAccessor.class);
    HRegionFileSystem mockFs = Mockito.mock(HRegionFileSystem.class);
    PersistedStoreFileManager storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), mockFs, regioninfo,
        DEFAULT_STORE_NAME, mockStoreFilePathAccessor, false);

    Collection<StoreFileInfo> expectedStoreFileInfos =
      convertToStoreFileInfos(fs, initialStoreFiles);

    when(mockFs.getStoreFiles(DEFAULT_STORE_NAME)).thenReturn(expectedStoreFileInfos);

    Collection<StoreFileInfo> actualStoreFileInfos = storeFileManager.loadInitialFiles();
    verify(mockFs, times(1)).getStoreFiles(DEFAULT_STORE_NAME);
    assertEquals(expectedStoreFileInfos, actualStoreFileInfos);
  }

  @Test
  public void testLoadInitialFilesWithNoFiles() throws IOException {
    HRegionFileSystem mockFs = Mockito.mock(HRegionFileSystem.class);
    when(mockFs.getStoreFiles(DEFAULT_STORE_NAME)).thenReturn(null);

    PersistedStoreFileManager storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), mockFs, regioninfo,
        DEFAULT_STORE_NAME, storeFilePathAccessor);

    Collection<StoreFileInfo> actualStoreFileInfos = storeFileManager.loadInitialFiles();
    verify(mockFs, times(1)).getStoreFiles(DEFAULT_STORE_NAME);
    assertNull(actualStoreFileInfos);
    assertEquals(EMPTY_LIST, storeFileManager.getStorefiles());
  }

  @Test
  public void testLoadInitialFilesWithFilesFromFileSystem() throws IOException {
    HRegionFileSystem mockFs = Mockito.mock(HRegionFileSystem.class);
    when(mockFs.getFileSystem()).thenReturn(fs);
    Collection<StoreFileInfo> expectedStoreFileInfos =
      convertToStoreFileInfos(fs, initialStoreFiles);
    when(mockFs.getStoreFiles(DEFAULT_STORE_NAME)).thenReturn(expectedStoreFileInfos);

    PersistedStoreFileManager storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), mockFs, regioninfo,
        DEFAULT_STORE_NAME, storeFilePathAccessor);

    // try to check if there is any store file to be loaded
    Collection<StoreFileInfo> actualStoreFileInfos = storeFileManager.loadInitialFiles();
    verify(mockFs, times(1)).getStoreFiles(DEFAULT_STORE_NAME);
    assertEquals(expectedStoreFileInfos, actualStoreFileInfos);
  }

  @Test
  public void testClearFiles() throws IOException {
    storeFileManager.clearFiles();
    compareIncludedInManagerVsTable(EMPTY_LIST);

    storeFileManager.loadFiles(initialStoreFiles);
    compareIncludedInManagerVsTable(sortedInitialStoreFiles);

    storeFileManager.clearFiles();
    compareIncludedInManagerVsTable(EMPTY_LIST, sortedInitialStoreFiles);
  }

  @Test
  public void testClearCompactedFiles() throws IOException {
    storeFileManager.clearCompactedFiles();
    verifyCompactedfiles(EMPTY_LIST);

    storeFileManager.addCompactionResults(initialStoreFiles, initialStoreFiles);
    verifyCompactedfiles(sortedInitialStoreFiles);
    storeFileManager.clearCompactedFiles();
    verifyCompactedfiles(EMPTY_LIST);
  }

  @Test
  public void testAddCompactionResults() throws IOException {
    storeFileManager.clearFiles();
    storeFileManager.clearCompactedFiles();

    List<HStoreFile> firstCompactionResult = createStoreFilesList();
    List<Path> firstCompactionResultPath = StorefileTrackingUtils.convertStoreFilesToPaths(firstCompactionResult);
    List<HStoreFile> secondCompactionResult = createStoreFilesList();
    List<Path> secondCompactionResultPath = StorefileTrackingUtils.convertStoreFilesToPaths(secondCompactionResult);
    List<HStoreFile> thirdCompactionResult = createStoreFilesList();
    List<Path> thirdCompactionResultPath = StorefileTrackingUtils.convertStoreFilesToPaths(thirdCompactionResult);
    // Composition of all compaction results, loaded into tmpFiles to track that tmpFiles are being
    // removed correctly
    List<Path> initialCompactionTmpPaths = ImmutableList.copyOf(
      Iterables.concat(firstCompactionResultPath, secondCompactionResultPath, thirdCompactionResultPath));

    // manager.storefiles    = EMPTY_LIST            -> firstCompactionResult
    // manager.compactedfiles= [empty]               -> EMPTY_LIST
    // manager.tmpFiles = EMPTY_LIST                 -> initialCompactionTmpPaths (all results)
    storeFileManager.addCompactionResults(EMPTY_LIST, firstCompactionResult);
    ImmutableList<HStoreFile> expectedFirstCompactionResult =
      ImmutableList.sortedCopyOf(COMPARATOR, firstCompactionResult);
    compareIncludedInManagerVsTable(expectedFirstCompactionResult);
    verifyCompactedfiles(EMPTY_LIST);

    // manager.storefiles    = firstCompactionResult -> secondCompactionResult
    // manager.compactedfiles= EMPTY_LIST  -> firstCompactionResult
    // manager.tmpFiles      = second + third -> third
    storeFileManager.addCompactionResults(firstCompactionResult, secondCompactionResult);
    ImmutableList<HStoreFile> expectedSecondCompactionResult =
      ImmutableList.sortedCopyOf(COMPARATOR, secondCompactionResult);
    compareIncludedInManagerVsTable(expectedSecondCompactionResult);
    verifyCompactedfiles(expectedFirstCompactionResult);

    // check manager.compactedfiles accumulates
    // manager.storefiles    = secondCompactionResult-> thirdCompactionResult
    // manager.compactedfiles= firstCompactionResult -> firstCompactionResult+secondCompactionResult
    // manager.tmpFiles      = third -> EMPTY
    storeFileManager.addCompactionResults(secondCompactionResult, thirdCompactionResult);
    ImmutableList<HStoreFile> expectedThirdCompactionResult =
      ImmutableList.sortedCopyOf(COMPARATOR, thirdCompactionResult);
    compareIncludedInManagerVsTable(expectedThirdCompactionResult);
    secondCompactionResult.addAll(firstCompactionResult);
    ImmutableList<HStoreFile> expectedCompactedfiles =
      ImmutableList.sortedCopyOf(COMPARATOR, secondCompactionResult);
    verifyCompactedfiles(expectedCompactedfiles);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddCompactionResultsWithEmptyResults() throws IOException {
    // PersistedStoreFileManager can only perform addCompactionResults
    // after storefiles are flushed and being tracking in-memory
    storeFileManager.addCompactionResults(initialStoreFiles, EMPTY_LIST);
  }

  @Test
  public void testRemoveCompactedFiles() throws IOException {
    // make sure the store file tracking is empty
    compareIncludedInManagerVsTable(EMPTY_LIST);
    verifyCompactedfiles(EMPTY_LIST);

    List<HStoreFile> storefilesSet1 = initialStoreFiles;
    List<HStoreFile> storefilesSet2 = additionalStoreFiles;
    List<HStoreFile> compactedFiles = Lists.newArrayList(storefilesSet1);
    // load some files into store file manager
    storeFileManager.loadFiles(storefilesSet1);

    storeFileManager.addCompactionResults(storefilesSet1, storefilesSet2);
    List<HStoreFile> expectedCompactedfiles = sortedInitialStoreFiles;
    List<HStoreFile> expectedIncluded = sortedAdditionalStoreFiles;
    verifyCompactedfiles(expectedCompactedfiles);
    compareIncludedInManagerVsTable(expectedIncluded);

    storeFileManager.removeCompactedFiles(compactedFiles);
    verifyCompactedfiles(EMPTY_LIST);
    compareIncludedInManagerVsTable(expectedIncluded);
  }

  @Test
  public void testRemoveCompactedFilesWhenEmpty() throws IOException {
    // simulate the store.close() remove again and it should not change the table both in memory
    // and the table.
    HRegionFileSystem mockFs = Mockito.mock(HRegionFileSystem.class);
    StoreFilePathAccessor mockStoreFilePathAccessor = Mockito.mock(StoreFilePathAccessor.class);
    PersistedStoreFileManager storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), mockFs, regioninfo,
        DEFAULT_STORE_NAME, mockStoreFilePathAccessor);
    assertTrue(storeFileManager.getCompactedfiles().isEmpty());
    storeFileManager.removeCompactedFiles(sortedInitialStoreFiles);
    assertTrue(storeFileManager.getCompactedfiles().isEmpty());
  }

  @Test
  public void testRemoveCompactedFilesNormalOperation() throws IOException {
    // simulate the store.close() remove again and it should not change the table both in memory
    // and the table.
    HRegionFileSystem mockFs = Mockito.mock(HRegionFileSystem.class);
    StoreFilePathAccessor mockStoreFilePathAccessor = Mockito.mock(StoreFilePathAccessor.class);
    PersistedStoreFileManager storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), mockFs, regioninfo, DEFAULT_STORE_NAME,
        mockStoreFilePathAccessor);
    assertTrue(storeFileManager.getCompactedfiles().isEmpty());

    storeFileManager.addCompactionResults(initialStoreFiles, initialStoreFiles);
    assertEquals(storeFileManager.getCompactedfiles(), sortedInitialStoreFiles);
    storeFileManager.removeCompactedFiles(sortedInitialStoreFiles);
    assertTrue(storeFileManager.getCompactedfiles().isEmpty());
  }

  @Test
  public void testUpdatePathListToTracker_ReadOnly() throws IOException {
    HRegionFileSystem mockFs = Mockito.mock(HRegionFileSystem.class);
    StoreFilePathAccessor mockStoreFilePathAccessor = Mockito.mock(StoreFilePathAccessor.class);
    PersistedStoreFileManager storeFileManager =
      new PersistedStoreFileManager(DEFAULT_CELL_COMPARATOR, COMPARATOR, conf,
        Mockito.mock(CompactionPolicy.class).getConf(), mockFs, regioninfo, DEFAULT_STORE_NAME,
        mockStoreFilePathAccessor, true);

    StoreFilePathUpdate storeFilePathUpdate = StoreFilePathUpdate.builder()
      .withStoreFiles(initialStoreFiles).build();
    storeFileManager.updatePathListToTracker(storeFilePathUpdate);
    verify(mockStoreFilePathAccessor, times(0))
      .writeStoreFilePaths(regioninfo.getTable().getNameAsString(), regioninfo.getEncodedName(),
        DEFAULT_STORE_NAME, storeFilePathUpdate);
  }

  private void compareIncludedInManagerVsTable(List<HStoreFile> expectedFiles) throws IOException {
    compareIncludedInManagerVsTable(expectedFiles, expectedFiles);
  }

  private void compareIncludedInManagerVsTable(List<HStoreFile> expectedStoreFilesOnHeap,
    List<HStoreFile> expectedStoreFilesInTable) throws IOException {
    Collection<HStoreFile> storeFilesOnHeap = storeFileManager.getStorefiles();
    assertEquals(expectedStoreFilesOnHeap, storeFilesOnHeap);

    Collection<Path> includedPathsFromAccessor = storeFilePathAccessor
      .getIncludedStoreFilePaths(tableName.getNameAsString(), regionName, DEFAULT_STORE_NAME);
    assertEquals(StorefileTrackingUtils.convertStoreFilesToPaths(expectedStoreFilesInTable),
      includedPathsFromAccessor);
  }

  private void verifyCompactedfiles(List<HStoreFile> expectedCompactedfilesOnHeap) {
    Collection<HStoreFile> compactedFilesOnHeap = storeFileManager.getCompactedfiles();
    assertEquals(expectedCompactedfilesOnHeap, compactedFilesOnHeap);
  }

  private List<HStoreFile> createStoreFilesList() throws IOException {
    HStoreFile sf1 = createFile();
    HStoreFile sf2 = createFile();
    HStoreFile sf3 = createFile();
    return Lists.newArrayList(sf1, sf2, sf3);
  }

  private List<Path> createPathList() throws IOException {
    Path path1 = createFilePath();
    Path path2 = createFilePath();
    Path path3 = createFilePath();
    return Lists.newArrayList(path1, path2, path3);
  }

  private MockHStoreFile createFile() throws IOException {
    return new MockHStoreFile(TEST_UTIL, createFilePath(), 0, 0, false, 1);
  }

  private Path createFilePath() throws IOException {
    Path testFilePath = StoreFileWriter.getUniqueFile(fs, baseDir);
    FSDataOutputStream out = fs.create(testFilePath);
    out.write(0);
    out.close();
    return testFilePath;
  }

  private void verifyStoreFileManagerWhenStarts() {
    assertTrue(storeFileManager.getStorefiles().isEmpty());
    assertTrue(storeFileManager.getCompactedfiles().isEmpty());
  }

  private Collection<StoreFileInfo> convertToStoreFileInfos(FileSystem fs,
    List<HStoreFile> storeFiles)
    throws IOException {
    ArrayList<StoreFileInfo> result = new ArrayList<>(storeFiles.size());
    for (HStoreFile storeFile: storeFiles) {
      StoreFileInfo info = new StoreFileInfo(conf, fs, storeFile.getFileInfo().getFileStatus());
      result.add(info);
    }
    return result;
  }
}
