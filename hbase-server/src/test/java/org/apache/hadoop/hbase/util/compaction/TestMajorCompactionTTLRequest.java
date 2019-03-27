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

package org.apache.hadoop.hbase.util.compaction;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestMajorCompactionTTLRequest {

  private static final HBaseTestingUtility UTILITY = new HBaseTestingUtility();
  private static final String FAMILY = "a";
  private Path regionStoreDir;

  @Before
  public void setUp() throws Exception {
    Path rootRegionDir = UTILITY.getDataTestDirOnTestFS("TestMajorCompactionTTLRequest");
    regionStoreDir = new Path(rootRegionDir, FAMILY);
  }

  @Test
  public void testStoresNeedingCompaction() throws Exception {
    // store files older than timestamp 10
    List<StoreFileInfo> storeFiles1 = mockStoreFiles(regionStoreDir, 5, 10);
    // store files older than timestamp 100
    List<StoreFileInfo> storeFiles2 = mockStoreFiles(regionStoreDir, 5, 100);
    List<StoreFileInfo> storeFiles = Lists.newArrayList(storeFiles1);
    storeFiles.addAll(storeFiles2);

    MajorCompactionTTLRequest request = makeMockRequest(storeFiles);
    // All files are <= 100, so region should not be compacted.
    Optional<MajorCompactionRequest> result =
        request.createRequest(mock(Configuration.class), Sets.newHashSet(FAMILY), 10);
    assertFalse(result.isPresent());

    // All files are <= 100, so region should not be compacted yet.
    result = request.createRequest(mock(Configuration.class), Sets.newHashSet(FAMILY), 100);
    assertFalse(result.isPresent());

    // All files are <= 100, so they should be considered for compaction
    result = request.createRequest(mock(Configuration.class), Sets.newHashSet(FAMILY), 101);
    assertTrue(result.isPresent());
  }

  private MajorCompactionTTLRequest makeMockRequest(List<StoreFileInfo> storeFiles)
      throws IOException {
    Configuration configuration = mock(Configuration.class);
    HRegionInfo regionInfo = mock(HRegionInfo.class);
    when(regionInfo.getEncodedName()).thenReturn("HBase");
    when(regionInfo.getTable()).thenReturn(TableName.valueOf("foo"));
    MajorCompactionTTLRequest request = new MajorCompactionTTLRequest(configuration, regionInfo);
    MajorCompactionTTLRequest spy = spy(request);
    HRegionFileSystem fileSystem = mockFileSystem(regionInfo, false, storeFiles);
    doReturn(fileSystem).when(spy).getFileSystem(isA(Connection.class));
    doReturn(mock(Connection.class)).when(spy).getConnection(eq(configuration));
    return spy;
  }

  private HRegionFileSystem mockFileSystem(HRegionInfo info, boolean hasReferenceFiles,
      List<StoreFileInfo> storeFiles) throws IOException {
    Optional<StoreFileInfo> found = Optional.absent();
    for (StoreFileInfo storeFile : storeFiles) {
      found = Optional.of(storeFile);
      break;
    }
    long timestamp = found.get().getModificationTime();
    return mockFileSystem(info, hasReferenceFiles, storeFiles, timestamp);
  }

  private List<StoreFileInfo> mockStoreFiles(Path regionStoreDir, int howMany, long timestamp)
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

  private HRegionFileSystem mockFileSystem(HRegionInfo info, boolean hasReferenceFiles,
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
}