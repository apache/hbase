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

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.util.List;

/**
 * Test class for the TestPersistedStoreFlushContext
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestPersistedEngineStoreFlusher {

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String DIR =
    TEST_UTIL.getDataTestDir("TestPersistedEngineStoreFlusher").toString();

  @Test
  public void testCreateWriter() throws Exception {
    Path filePath = new Path(DIR + name.getMethodName());
    HStore mockStore = mock(HStore.class);
    HRegionFileSystem mockRegionFS = mock(HRegionFileSystem.class);
    when(mockStore.getRegionFileSystem()).thenReturn(mockRegionFS);
    when(mockRegionFS.getRegionDir()).thenReturn(filePath);
    String cfName = name.getMethodName()+"-CF";
    when(mockStore.getColumnFamilyName()).thenReturn(cfName);
    HFileContext mockFileContext = mock(HFileContext.class);
    when(mockFileContext.getBytesPerChecksum()).thenReturn(100);
    StoreContext mockStoreContext = new StoreContext.Builder().build();
    when(mockStore.createFileContext(isNull(), anyBoolean(),
      anyBoolean(), isNull())).thenReturn(mockFileContext);
    when(mockStore.getStoreContext()).thenReturn(mockStoreContext);
    Configuration config = new Configuration();
    MemStoreSnapshot mockSnapshot = mock(MemStoreSnapshot.class);
    when(mockSnapshot.getCellsCount()).thenReturn(1);
    MonitoredTask mockStatus = mock(MonitoredTask.class);
    PersistedEngineStoreFlusher flusher = new PersistedEngineStoreFlusher(config, mockStore);
    when(mockStore.getHRegion()).thenReturn(mock(HRegion.class));
    ScanInfo mockScanInfo = mock(ScanInfo.class);
    when(mockStore.getScanInfo()).thenReturn(mockScanInfo);
    when(mockScanInfo.getComparator()).thenReturn(mock(CellComparator.class));
    ColumnFamilyDescriptor mockDesc = mock(ColumnFamilyDescriptor.class);
    when(mockDesc.getBloomFilterType()).thenReturn(BloomType.NONE);
    when(mockStore.getColumnFamilyDescriptor()).thenReturn(mockDesc);
    FileSystem mockFS = mock(FileSystem.class);
    when(mockFS.exists(any(Path.class))).thenReturn(true);
    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileStatus.isDirectory()).thenReturn(true);
    when(mockFS.getFileStatus(any(Path.class))).thenReturn(mockFileStatus);
    when(mockStore.getFileSystem()).thenReturn(mockFS);
    when(mockFS.getConf()).thenReturn(config);
    when(mockFS.create(any(Path.class), any(FsPermission.class), any(Boolean.class),
      any(Integer.class), any(Short.class), any(Long.class), any()))
        .thenReturn(mock(FSDataOutputStream.class));
    CacheConfig mockCacheConfig = mock(CacheConfig.class);
    when(mockCacheConfig.getByteBuffAllocator()).thenReturn(mock(ByteBuffAllocator.class));
    when(mockStore.getCacheConfig()).thenReturn(mockCacheConfig);
    when(mockFileContext.getEncryptionContext()).thenReturn(Encryption.Context.NONE);
    when(mockFileContext.getCompression()).thenReturn(Compression.Algorithm.NONE);
    when(mockFileContext.getChecksumType()).thenReturn(ChecksumType.NULL);
    when(mockFileContext.getCellComparator()).thenReturn(mock(CellComparator.class));
    when(mockStore.getRegionInfo()).thenReturn(mock(RegionInfo.class));
    List<Path> files = flusher.flushSnapshot(mockSnapshot, 0, mockStatus,
      null, FlushLifeCycleTracker.DUMMY);
    assertEquals(1, files.size());
    //asserts the file is created in the CF dir directly, instead of a temp dif
    assertEquals(new Path(filePath, cfName), files.get(0).getParent());
  }

}
