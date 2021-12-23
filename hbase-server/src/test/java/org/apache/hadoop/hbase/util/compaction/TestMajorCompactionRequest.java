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
package org.apache.hadoop.hbase.util.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({SmallTests.class})
public class TestMajorCompactionRequest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMajorCompactionRequest.class);

  protected static final HBaseTestingUtility UTILITY = new HBaseTestingUtility();
  protected static final String FAMILY = "a";
  protected Path rootRegionDir;
  protected Path regionStoreDir;

  @Before public void setUp() throws Exception {
    rootRegionDir = UTILITY.getDataTestDirOnTestFS("TestMajorCompactionRequest");
    regionStoreDir = new Path(rootRegionDir, FAMILY);
  }

  @Test public void testStoresNeedingCompaction() throws Exception {
    // store files older than timestamp
    List<StoreFileInfo> storeFiles = mockStoreFiles(regionStoreDir, 5, 10);
    MajorCompactionRequest request = makeMockRequest(storeFiles, false);
    Optional<MajorCompactionRequest> result =
        request.createRequest(mock(Connection.class), Sets.newHashSet(FAMILY), 100);
    assertTrue(result.isPresent());

    // store files newer than timestamp
    storeFiles = mockStoreFiles(regionStoreDir, 5, 101);
    request = makeMockRequest(storeFiles, false);
    result = request.createRequest(mock(Connection.class), Sets.newHashSet(FAMILY), 100);
    assertFalse(result.isPresent());
  }

  @Test public void testIfWeHaveNewReferenceFilesButOldStoreFiles() throws Exception {
    // this tests that reference files that are new, but have older timestamps for the files
    // they reference still will get compacted.
    TableName table = TableName.valueOf("TestMajorCompactor");
    TableDescriptor htd = UTILITY.createTableDescriptor(table, Bytes.toBytes(FAMILY));
    RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
    HRegion region =
        HBaseTestingUtility.createRegionAndWAL(hri, rootRegionDir, UTILITY.getConfiguration(), htd);

    Connection connection = mock(Connection.class);
    // the reference file timestamp is newer
    List<StoreFileInfo> storeFiles = mockStoreFiles(regionStoreDir, 4, 101);
    List<Path> paths = storeFiles.stream().map(StoreFileInfo::getPath).collect(Collectors.toList());
    // the files that are referenced are older, thus we still compact.
    HRegionFileSystem fileSystem =
        mockFileSystem(region.getRegionInfo(), true, storeFiles, 50);
    MajorCompactionRequest majorCompactionRequest = spy(new MajorCompactionRequest(connection,
        region.getRegionInfo(), Sets.newHashSet(FAMILY)));
    doReturn(paths).when(majorCompactionRequest).getReferenceFilePaths(any(FileSystem.class),
        any(Path.class));
    doReturn(fileSystem).when(majorCompactionRequest).getFileSystem();
    Set<String> result =
        majorCompactionRequest.getStoresRequiringCompaction(Sets.newHashSet("a"), 100);
    assertEquals(FAMILY, Iterables.getOnlyElement(result));
  }

  protected HRegionFileSystem mockFileSystem(RegionInfo info, boolean hasReferenceFiles,
      List<StoreFileInfo> storeFiles) throws IOException {
    long timestamp = storeFiles.stream().findFirst().get().getModificationTime();
    return mockFileSystem(info, hasReferenceFiles, storeFiles, timestamp);
  }

  private HRegionFileSystem mockFileSystem(RegionInfo info, boolean hasReferenceFiles,
      List<StoreFileInfo> storeFiles, long referenceFileTimestamp) throws IOException {
    FileSystem fileSystem = mock(FileSystem.class);
    if (hasReferenceFiles) {
      FileStatus fileStatus = mock(FileStatus.class);
      doReturn(referenceFileTimestamp).when(fileStatus).getModificationTime();
      doReturn(fileStatus).when(fileSystem).getFileLinkStatus(isA(Path.class));
    }
    HRegionFileSystem mockSystem = mock(HRegionFileSystem.class);
    doReturn(info).when(mockSystem).getRegionInfo();
    doReturn(regionStoreDir).when(mockSystem).getStoreDir(FAMILY);
    doReturn(hasReferenceFiles).when(mockSystem).hasReferences(anyString());
    doReturn(storeFiles).when(mockSystem).getStoreFiles(anyString());
    doReturn(fileSystem).when(mockSystem).getFileSystem();
    return mockSystem;
  }

  protected List<StoreFileInfo> mockStoreFiles(Path regionStoreDir, int howMany, long timestamp)
      throws IOException {
    List<StoreFileInfo> infos = Lists.newArrayList();
    int i = 0;
    while (i < howMany) {
      StoreFileInfo storeFileInfo = mock(StoreFileInfo.class);
      doReturn(timestamp).doReturn(timestamp).when(storeFileInfo).getModificationTime();
      doReturn(new Path(regionStoreDir, RandomStringUtils.randomAlphabetic(10))).when(storeFileInfo)
          .getPath();
      infos.add(storeFileInfo);
      i++;
    }
    return infos;
  }

  private MajorCompactionRequest makeMockRequest(List<StoreFileInfo> storeFiles,
      boolean references) throws IOException {
    Connection connection = mock(Connection.class);
    RegionInfo regionInfo = mock(RegionInfo.class);
    when(regionInfo.getEncodedName()).thenReturn("HBase");
    when(regionInfo.getTable()).thenReturn(TableName.valueOf("foo"));
    MajorCompactionRequest request =
        new MajorCompactionRequest(connection, regionInfo, Sets.newHashSet("a"));
    MajorCompactionRequest spy = spy(request);
    HRegionFileSystem fileSystem = mockFileSystem(regionInfo, references, storeFiles);
    doReturn(fileSystem).when(spy).getFileSystem();
    return spy;
  }
}
