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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({SmallTests.class})
public class TestMajorCompactionTTLRequest extends TestMajorCompactionRequest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMajorCompactionTTLRequest.class);

  @Before
  @Override
  public void setUp() throws Exception {
    rootRegionDir = UTILITY.getDataTestDirOnTestFS("TestMajorCompactionTTLRequest");
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
        request.createRequest(mock(Connection.class), Sets.newHashSet(FAMILY), 10);
    assertFalse(result.isPresent());

    // All files are <= 100, so region should not be compacted yet.
    result = request.createRequest(mock(Connection.class), Sets.newHashSet(FAMILY), 100);
    assertFalse(result.isPresent());

    // All files are <= 100, so they should be considered for compaction
    result = request.createRequest(mock(Connection.class), Sets.newHashSet(FAMILY), 101);
    assertTrue(result.isPresent());
  }

  private MajorCompactionTTLRequest makeMockRequest(List<StoreFileInfo> storeFiles)
      throws IOException {
    Connection connection = mock(Connection.class);
    RegionInfo regionInfo = mock(RegionInfo.class);
    when(regionInfo.getEncodedName()).thenReturn("HBase");
    when(regionInfo.getTable()).thenReturn(TableName.valueOf("foo"));
    MajorCompactionTTLRequest request = new MajorCompactionTTLRequest(connection, regionInfo);
    MajorCompactionTTLRequest spy = spy(request);
    HRegionFileSystem fileSystem = mockFileSystem(regionInfo, false, storeFiles);
    doReturn(fileSystem).when(spy).getFileSystem();
    return spy;
  }
}
