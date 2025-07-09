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

import static org.apache.hadoop.hbase.regionserver.CustomTieringMultiFileWriter.CUSTOM_TIERING_TIME_RANGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.compactions.CustomDateTieredCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactionRequest;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerForTest;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestCustomCellTieredCompactionPolicy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCustomCellTieredCompactionPolicy.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  public static final byte[] FAMILY = Bytes.toBytes("cf");

  private HStoreFile createFile(Path file, long minValue, long maxValue, long size, int seqId)
    throws IOException {
    return createFile(mockRegionInfo(), file, minValue, maxValue, size, seqId, 0);
  }

  private HStoreFile createFile(RegionInfo regionInfo, Path file, long minValue, long maxValue,
    long size, int seqId, long ageInDisk) throws IOException {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    HRegionFileSystem regionFileSystem =
      new HRegionFileSystem(TEST_UTIL.getConfiguration(), fs, file, regionInfo);
    StoreContext ctx = new StoreContext.Builder()
      .withColumnFamilyDescriptor(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build())
      .withRegionFileSystem(regionFileSystem).build();
    StoreFileTrackerForTest sftForTest =
      new StoreFileTrackerForTest(TEST_UTIL.getConfiguration(), true, ctx);
    MockHStoreFile msf =
      new MockHStoreFile(TEST_UTIL, file, size, ageInDisk, false, (long) seqId, sftForTest);
    TimeRangeTracker timeRangeTracker = TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC);
    timeRangeTracker.setMin(minValue);
    timeRangeTracker.setMax(maxValue);
    msf.setMetadataValue(CUSTOM_TIERING_TIME_RANGE, TimeRangeTracker.toByteArray(timeRangeTracker));
    return msf;
  }

  private CustomDateTieredCompactionPolicy mockAndCreatePolicy() throws Exception {
    RegionInfo mockedRegionInfo = mockRegionInfo();
    return mockAndCreatePolicy(mockedRegionInfo);
  }

  private CustomDateTieredCompactionPolicy mockAndCreatePolicy(RegionInfo regionInfo)
    throws Exception {
    StoreConfigInformation mockedStoreConfig = mock(StoreConfigInformation.class);
    when(mockedStoreConfig.getRegionInfo()).thenReturn(regionInfo);
    CustomDateTieredCompactionPolicy policy =
      new CustomDateTieredCompactionPolicy(TEST_UTIL.getConfiguration(), mockedStoreConfig);
    return policy;
  }

  private RegionInfo mockRegionInfo() {
    RegionInfo mockedRegionInfo = mock(RegionInfo.class);
    when(mockedRegionInfo.getEncodedName()).thenReturn("1234567890987654321");
    return mockedRegionInfo;
  }

  private Path preparePath() throws Exception {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Path file =
      new Path(TEST_UTIL.getDataTestDir(), UUID.randomUUID().toString().replaceAll("-", ""));
    fs.create(file);
    return file;
  }

  @Test
  public void testGetCompactBoundariesForMajorNoOld() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 0));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 1));
    assertEquals(1,
      ((DateTieredCompactionRequest) policy.selectMajorCompaction(files)).getBoundaries().size());
  }

  @Test
  public void testGetCompactBoundariesForMajorAllOld() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    // The default cut off age is 10 years, so any of the min/max value there should get in the old
    // tier
    files.add(createFile(file, 0, 1, 1024, 0));
    files.add(createFile(file, 2, 3, 1024, 1));
    assertEquals(2,
      ((DateTieredCompactionRequest) policy.selectMajorCompaction(files)).getBoundaries().size());
  }

  @Test
  public void testGetCompactBoundariesForMajorOneOnEachSide() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    files.add(createFile(file, 0, 1, 1024, 0));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 1));
    assertEquals(3,
      ((DateTieredCompactionRequest) policy.selectMajorCompaction(files)).getBoundaries().size());
  }

  @Test
  public void testGetCompactBoundariesForMajorOneCrossing() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    files.add(createFile(file, 0, EnvironmentEdgeManager.currentTime(), 1024, 0));
    assertEquals(3,
      ((DateTieredCompactionRequest) policy.selectMajorCompaction(files)).getBoundaries().size());
  }

  @FunctionalInterface
  interface PolicyValidator<T, U> {
    void accept(T t, U u) throws Exception;
  }

  private void testShouldPerformMajorCompaction(long min, long max, int numFiles,
    PolicyValidator<CustomDateTieredCompactionPolicy, ArrayList<HStoreFile>> validation)
    throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    RegionInfo mockedRegionInfo = mockRegionInfo();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    ManualEnvironmentEdge timeMachine = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(timeMachine);
    for (int i = 0; i < numFiles; i++) {
      MockHStoreFile mockedSFile = (MockHStoreFile) createFile(mockedRegionInfo, file, min, max,
        1024, 0, HConstants.DEFAULT_MAJOR_COMPACTION_PERIOD);
      mockedSFile.setIsMajor(true);
      files.add(mockedSFile);
    }
    EnvironmentEdgeManager.reset();
    validation.accept(policy, files);
  }

  @Test
  public void testShouldPerformMajorCompactionOneFileCrossing() throws Exception {
    long max = EnvironmentEdgeManager.currentTime();
    testShouldPerformMajorCompaction(0, max, 1,
      (p, f) -> assertTrue(p.shouldPerformMajorCompaction(f)));
  }

  @Test
  public void testShouldPerformMajorCompactionOneFileMinMaxLow() throws Exception {
    testShouldPerformMajorCompaction(0, 1, 1,
      (p, f) -> assertFalse(p.shouldPerformMajorCompaction(f)));
  }

  @Test
  public void testShouldPerformMajorCompactionOneFileMinMaxHigh() throws Exception {
    long currentTime = EnvironmentEdgeManager.currentTime();
    testShouldPerformMajorCompaction(currentTime, currentTime, 1,
      (p, f) -> assertFalse(p.shouldPerformMajorCompaction(f)));
  }

  @Test
  public void testShouldPerformMajorCompactionTwoFilesMinMaxHigh() throws Exception {
    long currentTime = EnvironmentEdgeManager.currentTime();
    testShouldPerformMajorCompaction(currentTime, currentTime, 2,
      (p, f) -> assertTrue(p.shouldPerformMajorCompaction(f)));
  }

  @Test
  public void testSelectMinorCompactionTwoFilesNoOld() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 0));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 1));
    // Shouldn't do minor compaction, as minimum number of files
    // for minor compactions is 3
    assertEquals(0, policy.selectMinorCompaction(files, true, true).getFiles().size());
  }

  @Test
  public void testSelectMinorCompactionThreeFilesNoOld() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 0));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 1));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 2));
    assertEquals(3, policy.selectMinorCompaction(files, true, true).getFiles().size());
  }

  @Test
  public void testSelectMinorCompactionThreeFilesAllOld() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    files.add(createFile(file, 0, 1, 1024, 0));
    files.add(createFile(file, 1, 2, 1024, 1));
    files.add(createFile(file, 3, 4, 1024, 2));
    assertEquals(3, policy.selectMinorCompaction(files, true, true).getFiles().size());
  }

  @Test
  public void testSelectMinorCompactionThreeFilesOneOldTwoNew() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    files.add(createFile(file, 0, 1, 1024, 0));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 1));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 2));
    assertEquals(3, policy.selectMinorCompaction(files, true, true).getFiles().size());
  }

  @Test
  public void testSelectMinorCompactionThreeFilesTwoOldOneNew() throws Exception {
    CustomDateTieredCompactionPolicy policy = mockAndCreatePolicy();
    Path file = preparePath();
    ArrayList<HStoreFile> files = new ArrayList<>();
    files.add(createFile(file, 0, 1, 1024, 0));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 1));
    files.add(createFile(file, EnvironmentEdgeManager.currentTime(),
      EnvironmentEdgeManager.currentTime(), 1024, 2));
    assertEquals(3, policy.selectMinorCompaction(files, true, true).getFiles().size());
  }
}
