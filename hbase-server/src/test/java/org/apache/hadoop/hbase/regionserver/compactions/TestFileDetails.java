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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor.FileDetails;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFileDetails {

  @Test public void testLatestPutTs() throws IOException {
    List<HStoreFile> sfs = new ArrayList<>(3);
    Map<byte[], byte[]> fileInfo = new HashMap<>();
    TimeRangeTracker tracker = TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC, 0, 3000);
    fileInfo.put(HStoreFile.TIMERANGE_KEY, TimeRangeTracker.toByteArray(tracker));
    sfs.add(createStoreFile(fileInfo));
    tracker = TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC, 0, 2000);
    fileInfo.put(HStoreFile.TIMERANGE_KEY, TimeRangeTracker.toByteArray(tracker));
    sfs.add(createStoreFile(fileInfo));
    tracker = TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC, 0, 1000);
    fileInfo.put(HStoreFile.TIMERANGE_KEY, TimeRangeTracker.toByteArray(tracker));
    sfs.add(createStoreFile(fileInfo));

    FileDetails fd = Compactor.getFileDetails(sfs, HConstants.MIN_KEEP_SEQID_PERIOD, false, false,
        Compression.Algorithm.NONE, Compression.Algorithm.NONE);
    assertEquals(3000, fd.latestPutTs);

    // when TIMERANGE_KEY is null
    fileInfo.clear();
    sfs.add(createStoreFile(fileInfo));
    fd = Compactor.getFileDetails(sfs, HConstants.MIN_KEEP_SEQID_PERIOD, false, false,
        Compression.Algorithm.NONE, Compression.Algorithm.NONE);
    assertEquals(HConstants.LATEST_TIMESTAMP, fd.latestPutTs);
  }

  private static HStoreFile createStoreFile(Map<byte[], byte[]> fileInfo)
      throws IOException {
    HStoreFile sf = Mockito.mock(HStoreFile.class);
    Mockito.doReturn(System.currentTimeMillis()).when(sf).getModificationTimestamp();
    Mockito.doReturn(0L).when(sf).getMaxSequenceId();
    StoreFileReader reader = Mockito.mock(StoreFileReader.class);
    Mockito.doReturn(0L).when(reader).getEntries();
    Mockito.doReturn(new HashMap<>(fileInfo)).when(reader).loadFileInfo();
    Mockito.doReturn(0L).when(reader).length();
    Mockito.doReturn(false).when(reader).isBulkLoaded();
    Mockito.doReturn(BloomType.NONE).when(reader).getBloomFilterType();
    HFile.Reader hfr = Mockito.mock(HFile.Reader.class);
    Mockito.doReturn(DataBlockEncoding.NONE).when(hfr).getDataBlockEncoding();
    Mockito.doReturn(hfr).when(reader).getHFileReader();
    Mockito.doReturn(reader).when(sf).getReader();
    return sf;
  }

}
