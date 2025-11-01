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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.SplitTableRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestDirectStoreSplitsMerges {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDirectStoreSplitsMerges.class);

  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  public static final byte[] FAMILY_NAME = Bytes.toBytes("info");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSplitStoreDir() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, FAMILY_NAME);
    // first put some data in order to have a store file created
    putThreeRowsAndFlush(table);
    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(table).get(0);
    HRegionFileSystem regionFS = region.getStores().get(0).getRegionFileSystem();
    RegionInfo daughterA =
      RegionInfoBuilder.newBuilder(table).setStartKey(region.getRegionInfo().getStartKey())
        .setEndKey(Bytes.toBytes("002")).setSplit(false)
        .setRegionId(region.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
        .build();
    HStoreFile file = (HStoreFile) region.getStore(FAMILY_NAME).getStorefiles().toArray()[0];
    StoreFileTracker sft =
      StoreFileTrackerFactory.create(TEST_UTIL.getHBaseCluster().getMaster().getConfiguration(),
        true, region.getStores().get(0).getStoreContext());
    Path result = regionFS.splitStoreFile(daughterA, Bytes.toString(FAMILY_NAME), file,
      Bytes.toBytes("002"), false, region.getSplitPolicy(), sft).getPath();
    // asserts the reference file naming is correct
    validateResultingFile(region.getRegionInfo().getEncodedName(), result);
    // Additionally check if split region dir was created directly under table dir, not on .tmp
    Path resultGreatGrandParent = result.getParent().getParent().getParent();
    assertEquals(regionFS.getTableDir().getName(), resultGreatGrandParent.getName());
  }

  @Test
  public void testMergeStoreFile() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, FAMILY_NAME);
    // splitting the table first
    TEST_UTIL.getAdmin().split(table, Bytes.toBytes("002"));
    waitForSplitProcComplete(1000, 10);
    // Add data and flush to create files in the two different regions
    putThreeRowsAndFlush(table);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(table);
    HRegion first = regions.get(0);
    HRegion second = regions.get(1);
    HRegionFileSystem regionFS = first.getRegionFileSystem();

    RegionInfo mergeResult =
      RegionInfoBuilder.newBuilder(table).setStartKey(first.getRegionInfo().getStartKey())
        .setEndKey(second.getRegionInfo().getEndKey()).setSplit(false)
        .setRegionId(first.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
        .build();

    Configuration configuration = TEST_UTIL.getHBaseCluster().getMaster().getConfiguration();
    HRegionFileSystem mergeRegionFs = HRegionFileSystem.createRegionOnFileSystem(configuration,
      regionFS.getFileSystem(), regionFS.getTableDir(), mergeResult);

    // merge file from first region
    HStoreFile file = (HStoreFile) first.getStore(FAMILY_NAME).getStorefiles().toArray()[0];
    mergeFileFromRegion(mergeRegionFs, first, file, StoreFileTrackerFactory.create(configuration,
      true, first.getStore(FAMILY_NAME).getStoreContext()));
    // merge file from second region
    file = (HStoreFile) second.getStore(FAMILY_NAME).getStorefiles().toArray()[0];
    mergeFileFromRegion(mergeRegionFs, second, file, StoreFileTrackerFactory.create(configuration,
      true, second.getStore(FAMILY_NAME).getStoreContext()));
  }

  @Test
  public void testCommitDaughterRegionNoFiles() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, FAMILY_NAME);
    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(table).get(0);
    HRegionFileSystem regionFS = region.getStores().get(0).getRegionFileSystem();
    RegionInfo daughterA =
      RegionInfoBuilder.newBuilder(table).setStartKey(region.getRegionInfo().getStartKey())
        .setEndKey(Bytes.toBytes("002")).setSplit(false)
        .setRegionId(region.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
        .build();
    Path splitDir = regionFS.getSplitsDir(daughterA);
    MasterProcedureEnv env =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment();
    Path result = regionFS.commitDaughterRegion(daughterA, new ArrayList<>(), env);
    assertEquals(splitDir, result);
  }

  @Test
  public void testCommitDaughterRegionWithFiles() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, FAMILY_NAME);
    // first put some data in order to have a store file created
    putThreeRowsAndFlush(table);
    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(table).get(0);
    HRegionFileSystem regionFS = region.getStores().get(0).getRegionFileSystem();
    RegionInfo daughterA =
      RegionInfoBuilder.newBuilder(table).setStartKey(region.getRegionInfo().getStartKey())
        .setEndKey(Bytes.toBytes("002")).setSplit(false)
        .setRegionId(region.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
        .build();
    RegionInfo daughterB = RegionInfoBuilder.newBuilder(table).setStartKey(Bytes.toBytes("002"))
      .setEndKey(region.getRegionInfo().getEndKey()).setSplit(false)
      .setRegionId(region.getRegionInfo().getRegionId()).build();
    Path splitDirA = regionFS.getSplitsDir(daughterA);
    Path splitDirB = regionFS.getSplitsDir(daughterB);
    HStoreFile file = (HStoreFile) region.getStore(FAMILY_NAME).getStorefiles().toArray()[0];
    List<StoreFileInfo> filesA = new ArrayList<>();
    StoreFileTracker sft =
      StoreFileTrackerFactory.create(TEST_UTIL.getHBaseCluster().getMaster().getConfiguration(),
        true, region.getStores().get(0).getStoreContext());
    filesA.add(regionFS.splitStoreFile(daughterA, Bytes.toString(FAMILY_NAME), file,
      Bytes.toBytes("002"), false, region.getSplitPolicy(), sft));
    List<StoreFileInfo> filesB = new ArrayList<>();
    filesB.add(regionFS.splitStoreFile(daughterB, Bytes.toString(FAMILY_NAME), file,
      Bytes.toBytes("002"), true, region.getSplitPolicy(), sft));
    MasterProcedureEnv env =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment();
    Path resultA = regionFS.commitDaughterRegion(daughterA, filesA, env);
    Path resultB = regionFS.commitDaughterRegion(daughterB, filesB, env);
    assertEquals(splitDirA, resultA);
    assertEquals(splitDirB, resultB);
  }

  @Test
  public void testCommitMergedRegion() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, FAMILY_NAME);
    // splitting the table first
    TEST_UTIL.getAdmin().split(table, Bytes.toBytes("002"));
    waitForSplitProcComplete(1000, 10);
    // Add data and flush to create files in the two different regions
    putThreeRowsAndFlush(table);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(table);
    HRegion first = regions.get(0);
    HRegion second = regions.get(1);
    HRegionFileSystem regionFS = first.getRegionFileSystem();

    RegionInfo mergeResult =
      RegionInfoBuilder.newBuilder(table).setStartKey(first.getRegionInfo().getStartKey())
        .setEndKey(second.getRegionInfo().getEndKey()).setSplit(false)
        .setRegionId(first.getRegionInfo().getRegionId() + EnvironmentEdgeManager.currentTime())
        .build();

    Configuration configuration = TEST_UTIL.getHBaseCluster().getMaster().getConfiguration();
    HRegionFileSystem mergeRegionFs = HRegionFileSystem.createRegionOnFileSystem(configuration,
      regionFS.getFileSystem(), regionFS.getTableDir(), mergeResult);

    // merge file from first region
    HStoreFile file = (HStoreFile) first.getStore(FAMILY_NAME).getStorefiles().toArray()[0];
    mergeFileFromRegion(mergeRegionFs, first, file, StoreFileTrackerFactory.create(configuration,
      true, first.getStore(FAMILY_NAME).getStoreContext()));
    // merge file from second region
    file = (HStoreFile) second.getStore(FAMILY_NAME).getStorefiles().toArray()[0];
    List<StoreFileInfo> mergedFiles = new ArrayList<>();
    mergedFiles.add(mergeFileFromRegion(mergeRegionFs, second, file, StoreFileTrackerFactory
      .create(configuration, true, second.getStore(FAMILY_NAME).getStoreContext())));
    MasterProcedureEnv env =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment();
    mergeRegionFs.commitMergedRegion(mergedFiles, env);
  }

  private void waitForSplitProcComplete(int attempts, int waitTime) throws Exception {
    List<Procedure<?>> procedures = TEST_UTIL.getHBaseCluster().getMaster().getProcedures();
    if (procedures.size() > 0) {
      Procedure splitProc =
        procedures.stream().filter(p -> p instanceof SplitTableRegionProcedure).findFirst().get();
      int count = 0;
      while ((splitProc.isWaiting() || splitProc.isRunnable()) && count < attempts) {
        synchronized (splitProc) {
          splitProc.wait(waitTime);
        }
        count++;
      }
      assertTrue(splitProc.isSuccess());
    }
  }

  private StoreFileInfo mergeFileFromRegion(HRegionFileSystem regionFS, HRegion regionToMerge,
    HStoreFile file, StoreFileTracker sft) throws IOException {
    StoreFileInfo mergedFile = regionFS.mergeStoreFile(regionToMerge.getRegionInfo(),
      Bytes.toString(FAMILY_NAME), file, sft);
    validateResultingFile(regionToMerge.getRegionInfo().getEncodedName(), mergedFile.getPath());
    return mergedFile;
  }

  private void validateResultingFile(String originalRegion, Path result) {
    assertEquals(originalRegion, result.getName().split("\\.")[1]);
    // asserts we are under the cf directory
    Path resultParent = result.getParent();
    assertEquals(Bytes.toString(FAMILY_NAME), resultParent.getName());
  }

  private void putThreeRowsAndFlush(TableName table) throws IOException {
    Table tbl = TEST_UTIL.getConnection().getTable(table);
    Put put = new Put(Bytes.toBytes("001"));
    byte[] qualifier = Bytes.toBytes("1");
    put.addColumn(FAMILY_NAME, qualifier, Bytes.toBytes(1));
    tbl.put(put);
    put = new Put(Bytes.toBytes("002"));
    put.addColumn(FAMILY_NAME, qualifier, Bytes.toBytes(2));
    tbl.put(put);
    put = new Put(Bytes.toBytes("003"));
    put.addColumn(FAMILY_NAME, qualifier, Bytes.toBytes(2));
    tbl.put(put);
    TEST_UTIL.flush(table);
  }
}
