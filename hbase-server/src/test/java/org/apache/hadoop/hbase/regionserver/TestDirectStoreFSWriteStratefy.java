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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.List;

/**
 * Test for DirectStoreFSWriteStratefy
 */
@Category({ RegionServerTests.class, LargeTests.class})
public class TestDirectStoreFSWriteStratefy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDirectStoreFSWriteStratefy.class);

  @Rule
  public TestName name = new TestName();

  private static final String CF_NAME = "cf";

  private final byte[] cf = Bytes.toBytes(CF_NAME);

  private HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private TableName table;

  @Before
  public void setup() throws Exception {
    UTIL.startMiniCluster();
    table = TableName.valueOf(name.getMethodName());
    UTIL.createTable(table, cf);

  }

  @After
  public void shutdown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetParentSplitsDir() throws Exception {
    HRegionFileSystem regionFS = UTIL.getHBaseCluster().getRegions(table).get(0).
      getStores().get(0).getRegionFileSystem();
    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(regionFS);
    Path parentDir = writeStrategy.getParentSplitsDir();
    assertEquals(regionFS.getTableDir(), parentDir);
  }

  @Test
  public void testGetParentMergesDir() throws Exception {
    HRegionFileSystem regionFS = UTIL.getHBaseCluster().getRegions(table).get(0).
      getStores().get(0).getRegionFileSystem();
    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(regionFS);
    Path parentDir = writeStrategy.getParentMergesDir();
    assertEquals(regionFS.getTableDir(), parentDir);
  }

  @Test
  public void testCreateSplitsDir() throws Exception {
    HRegion region = UTIL.getHBaseCluster().getRegions(table).get(0);
    HRegionFileSystem regionFS = region.getStores().get(0).getRegionFileSystem();
    RegionInfo daughterA = RegionInfoBuilder.newBuilder(table)
      .setStartKey(region.getRegionInfo().getStartKey())
      .setEndKey(Bytes.toBytes("bbbb"))
      .setSplit(false)
      .setRegionId(region.getRegionInfo().getRegionId())
      .build();
    RegionInfo daughterB = RegionInfoBuilder.newBuilder(table)
      .setStartKey(Bytes.toBytes("bbbb"))
      .setEndKey(region.getRegionInfo().getEndKey())
      .setSplit(false)
      .setRegionId(region.getRegionInfo().getRegionId())
      .build();
    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(regionFS);
    writeStrategy.createSplitsDir(daughterA, daughterB);

    FileSystem fileSystem = region.getFilesystem();
    Path daughterAPath = new Path(region.getRegionFileSystem().getTableDir(), daughterA.getEncodedName());
    assertTrue(fileSystem.exists(daughterAPath));
    Path daughterBPath = new Path(region.getRegionFileSystem().getTableDir(), daughterB.getEncodedName());
    assertTrue(fileSystem.exists(daughterBPath));
  }

  @Test
  public void testCreateMergesDir() throws Exception {
    HRegionFileSystem regionFS = UTIL.getHBaseCluster().getRegions(table).get(0).
      getStores().get(0).getRegionFileSystem();
    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(regionFS);
    long cTime = regionFS.getFileSystem().
      getFileLinkStatus(regionFS.getTableDir()).getModificationTime();
    writeStrategy.createMergesDir();
    //assert the table dir has not been re-written
    assertEquals(cTime, regionFS.getFileSystem().
      getFileLinkStatus(regionFS.getTableDir()).getModificationTime());
  }

  @Test(expected = IOException.class)
  public void testCreateMergesDirTableDirMissing() throws Exception {
    HRegionFileSystem regionFS = UTIL.getHBaseCluster().getRegions(table).get(0).
      getStores().get(0).getRegionFileSystem();
    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(regionFS);
    regionFS.getFileSystem().delete(regionFS.getTableDir(), true);
    writeStrategy.createMergesDir();
  }

  @Test
  public void testSplitStoreDir() throws Exception {
    //first put some data in order to have a store file created
    putThreeRowsAndFlush(table);
    HRegion region = UTIL.getHBaseCluster().getRegions(table).get(0);
    HRegionFileSystem regionFS = region.getStores().get(0).getRegionFileSystem();
    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(regionFS);
    RegionInfo daughterA = RegionInfoBuilder.newBuilder(table)
      .setStartKey(region.getRegionInfo().getStartKey())
      .setEndKey(Bytes.toBytes("002"))
      .setSplit(false)
      .setRegionId(region.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
      .build();
    HStoreFile file = (HStoreFile) region.getStore(cf).getStorefiles().toArray()[0];
    Path result = writeStrategy.splitStoreFile(region.getRegionInfo(), daughterA, CF_NAME,
      file, Bytes.toBytes("002"), false, region.getSplitPolicy(), regionFS.fs);
    //asserts the reference file naming is correct
    validateResultingFile(region.getRegionInfo().getEncodedName(), result);
    //Additionally check if split region dir was created directly under table dir, not on .tmp
    Path resultGreatGrandParent = result.getParent().getParent().getParent();
    assertEquals(regionFS.getTableDir().getName(), resultGreatGrandParent.getName());
  }

  @Test
  public void testMergeStoreFile() throws IOException {
    //splitting the table first
    UTIL.getAdmin().split(table, Bytes.toBytes("002"));
    //Add data and flush to create files in the two different regions
    putThreeRowsAndFlush(table);
    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(table);
    HRegion first = regions.get(0);
    HRegion second = regions.get(1);
    HRegionFileSystem regionFS = first.getRegionFileSystem();

    RegionInfo mergeResult = RegionInfoBuilder.newBuilder(table)
      .setStartKey(first.getRegionInfo().getStartKey())
      .setEndKey(second.getRegionInfo().getEndKey())
      .setSplit(false)
      .setRegionId(first.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
      .build();

    HRegionFileSystem mergeRegionFs = HRegionFileSystem.createRegionOnFileSystem(
      UTIL.getHBaseCluster().getMaster().getConfiguration(),
      regionFS.getFileSystem(), regionFS.getTableDir(), mergeResult);

    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(mergeRegionFs);

    //merge file from first region
    HStoreFile file = (HStoreFile) first.getStore(cf).getStorefiles().toArray()[0];
    mergeFileFromRegion(writeStrategy, first, mergeResult, file);
    //merge file from second region
    file = (HStoreFile) second.getStore(cf).getStorefiles().toArray()[0];
    mergeFileFromRegion(writeStrategy, second, mergeResult, file);
  }

  @Test
  public void testCommitDaughterRegionNoFiles() throws IOException {
    HRegion region = UTIL.getHBaseCluster().getRegions(table).get(0);
    HRegionFileSystem regionFS = region.getStores().get(0).getRegionFileSystem();
    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(regionFS);
    RegionInfo daughterA = RegionInfoBuilder.newBuilder(table)
      .setStartKey(region.getRegionInfo().getStartKey())
      .setEndKey(Bytes.toBytes("002"))
      .setSplit(false)
      .setRegionId(region.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
      .build();
    Path splitDir = writeStrategy.getSplitsDir(daughterA);
    StoreFileTrackingUtils.init(UTIL.getHBaseCluster().getMaster());
    Path result = writeStrategy.commitDaughterRegion(daughterA);
    assertEquals(splitDir, result);
    StoreFilePathAccessor accessor = StoreFileTrackingUtils.
      createStoreFilePathAccessor(UTIL.getConfiguration(), UTIL.getConnection());
    List<Path>  filePaths = accessor.getIncludedStoreFilePaths(table.getNameAsString(),
      daughterA.getRegionNameAsString(), CF_NAME);
    //should have not listed any file
    assertEquals(0, filePaths.size());
  }

  @Test
  public void testCommitDaughterRegionWithFiles() throws IOException {
    //first put some data in order to have a store file created
    putThreeRowsAndFlush(table);
    HRegion region = UTIL.getHBaseCluster().getRegions(table).get(0);
    HRegionFileSystem regionFS = region.getStores().get(0).getRegionFileSystem();
    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(regionFS);
    RegionInfo daughterA = RegionInfoBuilder.newBuilder(table)
      .setStartKey(region.getRegionInfo().getStartKey())
      .setEndKey(Bytes.toBytes("002"))
      .setSplit(false)
      .setRegionId(region.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
      .build();
    RegionInfo daughterB = RegionInfoBuilder.newBuilder(table)
      .setStartKey(Bytes.toBytes("002"))
      .setEndKey(region.getRegionInfo().getEndKey())
      .setSplit(false)
      .setRegionId(region.getRegionInfo().getRegionId())
      .build();
    Path splitDirA = writeStrategy.getSplitsDir(daughterA);
    Path splitDirB = writeStrategy.getSplitsDir(daughterB);
    StoreFileTrackingUtils.init(UTIL.getHBaseCluster().getMaster());
    HStoreFile file = (HStoreFile) region.getStore(cf).getStorefiles().toArray()[0];
    writeStrategy.splitStoreFile(region.getRegionInfo(), daughterA, CF_NAME,
      file, Bytes.toBytes("002"), false, region.getSplitPolicy(), regionFS.fs);
    writeStrategy.splitStoreFile(region.getRegionInfo(), daughterB, CF_NAME,
      file, Bytes.toBytes("002"), true, region.getSplitPolicy(), regionFS.fs);
    Path resultA = writeStrategy.commitDaughterRegion(daughterA);
    Path resultB = writeStrategy.commitDaughterRegion(daughterB);
    assertEquals(splitDirA, resultA);
    assertEquals(splitDirB, resultB);
    StoreFilePathAccessor accessor = StoreFileTrackingUtils.
      createStoreFilePathAccessor(UTIL.getConfiguration(), UTIL.getConnection());
    List<Path>  filePathsA = accessor.getIncludedStoreFilePaths(table.getNameAsString(),
      daughterA.getRegionNameAsString(), CF_NAME);
    assertEquals(1, filePathsA.size());
    List<Path>  filePathsB = accessor.getIncludedStoreFilePaths(table.getNameAsString(),
      daughterB.getRegionNameAsString(), CF_NAME);
    assertEquals(1, filePathsB.size());
  }

  @Test
  public void testCommitMergedRegion() throws IOException {
    //splitting the table first
    UTIL.getAdmin().split(table, Bytes.toBytes("002"));
    //Add data and flush to create files in the two different regions
    putThreeRowsAndFlush(table);
    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(table);
    HRegion first = regions.get(0);
    HRegion second = regions.get(1);
    HRegionFileSystem regionFS = first.getRegionFileSystem();

    RegionInfo mergeResult = RegionInfoBuilder.newBuilder(table)
      .setStartKey(first.getRegionInfo().getStartKey())
      .setEndKey(second.getRegionInfo().getEndKey())
      .setSplit(false)
      .setRegionId(first.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
      .build();

    HRegionFileSystem mergeRegionFs = HRegionFileSystem.createRegionOnFileSystem(
      UTIL.getHBaseCluster().getMaster().getConfiguration(),
      regionFS.getFileSystem(), regionFS.getTableDir(), mergeResult);

    DirectStoreFSWriteStrategy writeStrategy = new DirectStoreFSWriteStrategy(mergeRegionFs);

    //merge file from first region
    HStoreFile file = (HStoreFile) first.getStore(cf).getStorefiles().toArray()[0];
    StoreFileTrackingUtils.init(UTIL.getHBaseCluster().getMaster());
    mergeFileFromRegion(writeStrategy, first, mergeResult, file);
    //merge file from second region
    file = (HStoreFile) second.getStore(cf).getStorefiles().toArray()[0];
    mergeFileFromRegion(writeStrategy, second, mergeResult, file);
    writeStrategy.commitMergedRegion(mergeResult);
    StoreFilePathAccessor accessor = StoreFileTrackingUtils.
      createStoreFilePathAccessor(UTIL.getConfiguration(), UTIL.getConnection());
    List<Path>  filePaths = accessor.getIncludedStoreFilePaths(table.getNameAsString(),
      mergeResult.getRegionNameAsString(), CF_NAME);
    assertEquals(2, filePaths.size());

  }

  private void mergeFileFromRegion(DirectStoreFSWriteStrategy writeStrategy, HRegion regionToMerge,
      RegionInfo resultingMerge, HStoreFile file) throws IOException {
    Path mergedFile = writeStrategy.mergeStoreFile(resultingMerge, regionToMerge.getRegionInfo(),
      CF_NAME, file, regionToMerge.getRegionFileSystem().getMergesDir(),
      regionToMerge.getRegionFileSystem().getFileSystem());
    validateResultingFile(regionToMerge.getRegionInfo().getEncodedName(), mergedFile);
  }

  private void validateResultingFile(String originalRegion, Path result){
    assertEquals(originalRegion, result.getName().split("\\.")[1]);
    //asserts we are under the cf directory
    Path resultParent = result.getParent();
    assertEquals(CF_NAME, resultParent.getName());
  }

  private void putThreeRowsAndFlush(TableName table) throws IOException {
    Table tbl = UTIL.getConnection().getTable(table);
    Put put = new Put(Bytes.toBytes("001"));
    byte[] qualifier = Bytes.toBytes("1");
    put.addColumn(cf, qualifier, Bytes.toBytes(1));
    tbl.put(put);
    put = new Put(Bytes.toBytes("002"));
    put.addColumn(cf, qualifier, Bytes.toBytes(2));
    tbl.put(put);
    put = new Put(Bytes.toBytes("003"));
    put.addColumn(cf, qualifier, Bytes.toBytes(2));
    tbl.put(put);
    UTIL.flush(table);
  }

}
