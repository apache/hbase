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

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.google.common.base.Objects;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.util.StringUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class of objects that can create mock store files with a given size.
 */
class MockStoreFileGenerator {
  /** How many chars long the store file name will be. */
  private static final int FILENAME_LENGTH = 10;
  /** The random number generator. */
  protected Random random;

  MockStoreFileGenerator(Class klass) {
    random = new Random(klass.getSimpleName().hashCode());
  }

  protected List<StoreFile> createStoreFileList(final int[] fs) {
    List<StoreFile> storeFiles = new LinkedList<StoreFile>();
    for (int fileSize : fs) {
      storeFiles.add(createMockStoreFile(fileSize));
    }
    return storeFiles;
  }

  protected StoreFile createMockStoreFile(final long size) {
    return createMockStoreFile(size * 1024 * 1024, -1L);
  }

  protected StoreFile createMockStoreFileBytes(final long size) {
    return createMockStoreFile(size, -1L);
  }

  protected StoreFile createMockStoreFile(final long sizeInBytes, final long seqId) {
    StoreFile mockSf = mock(StoreFile.class);
    StoreFileReader reader = mock(StoreFileReader.class);
    String stringPath = "/hbase/testTable/regionA/"
        + RandomStringUtils.random(FILENAME_LENGTH, 0, 0, true, true, null, random);
    Path path = new Path(stringPath);


    when(reader.getSequenceID()).thenReturn(seqId);
    when(reader.getTotalUncompressedBytes()).thenReturn(sizeInBytes);
    when(reader.length()).thenReturn(sizeInBytes);

    when(mockSf.getPath()).thenReturn(path);
    when(mockSf.excludeFromMinorCompaction()).thenReturn(false);
    when(mockSf.isReference()).thenReturn(false); // TODO come back to
    // this when selection takes this into account
    when(mockSf.getReader()).thenReturn(reader);
    String toString = Objects.toStringHelper("MockStoreFile")
        .add("isReference", false)
        .add("fileSize", StringUtils.humanReadableInt(sizeInBytes))
        .add("seqId", seqId)
        .add("path", stringPath).toString();
    when(mockSf.toString()).thenReturn(toString);

    return mockSf;
  }
}
