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

package org.apache.hadoop.hbase.regionserver.compactions;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.util.StringUtils;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;

/**
 * Base class of objects that can create mock store files with a given size.
 */
class MockStoreFileGenerator {
  /** How many chars long the store file name will be. */
  private static final int FILENAME_LENGTH = 10;

  protected List<HStoreFile> createStoreFileList(final int[] fs) {
    List<HStoreFile> storeFiles = new LinkedList<>();
    for (int fileSize : fs) {
      storeFiles.add(createMockStoreFile(fileSize));
    }
    return storeFiles;
  }

  protected HStoreFile createMockStoreFile(final long size) {
    return createMockStoreFile(size * 1024 * 1024, -1L);
  }

  protected HStoreFile createMockStoreFileBytes(final long size) {
    return createMockStoreFile(size, -1L);
  }

  protected HStoreFile createMockStoreFile(final long sizeInBytes, final long seqId) {
    HStoreFile mockSf = mock(HStoreFile.class);
    StoreFileReader reader = mock(StoreFileReader.class);
    String stringPath = "/hbase/testTable/regionA/" +
        RandomStringUtils.random(FILENAME_LENGTH, 0, 0, true, true, null,
          ThreadLocalRandom.current());
    Path path = new Path(stringPath);

    when(reader.getSequenceID()).thenReturn(seqId);
    when(reader.getTotalUncompressedBytes()).thenReturn(sizeInBytes);
    when(reader.length()).thenReturn(sizeInBytes);

    when(mockSf.getPath()).thenReturn(path);
    when(mockSf.excludeFromMinorCompaction()).thenReturn(false);
    when(mockSf.isReference()).thenReturn(false); // TODO come back to
    // this when selection takes this into account
    when(mockSf.getReader()).thenReturn(reader);
    String toString = MoreObjects.toStringHelper("MockStoreFile")
        .add("isReference", false)
        .add("fileSize", StringUtils.humanReadableInt(sizeInBytes))
        .add("seqId", seqId)
        .add("path", stringPath).toString();
    when(mockSf.toString()).thenReturn(toString);

    return mockSf;
  }
}
