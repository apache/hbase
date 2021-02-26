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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, MediumTests.class })
public class TestRowIndexV1DataEncoder {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowIndexV1DataEncoder.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Configuration conf;
  private FileSystem fs;
  private DataBlockEncoding dataBlockEncoding;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    fs = FileSystem.get(conf);
    dataBlockEncoding = DataBlockEncoding.ROW_INDEX_V1;
  }

  @Test
  public void testBlockCountWritten() throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(), "testHFileFormatV3");
    final int entryCount = 10000;
    writeDataToHFile(hfilePath, entryCount);
  }

  private void writeDataToHFile(Path hfilePath, int entryCount) throws IOException {
    HFileContext context =
      new HFileContextBuilder().withBlockSize(1024).withDataBlockEncoding(dataBlockEncoding)
        .withCellComparator(CellComparatorImpl.COMPARATOR).build();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HFile.Writer writer =
      new HFile.WriterFactory(conf, cacheConfig).withPath(fs, hfilePath).withFileContext(context)
        .create();

    List<KeyValue> keyValues = new ArrayList<>(entryCount);

    writeKeyValues(entryCount, writer, keyValues);

    FSDataInputStream fsdis = fs.open(hfilePath);

    long fileSize = fs.getFileStatus(hfilePath).getLen();
    FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis, fileSize);

    // HBASE-23788
    // kv size = 24 bytes, block size = 1024 bytes
    // per row encoded data written = (4 (Row index) + 24 (Cell size) + 1 (MVCC)) bytes = 29 bytes
    // creating block size of (29 * 36) bytes = 1044 bytes
    // Number of blocks = ceil((29 * 10000) / 1044) = 278
    // Without the patch it would have produced 244 blocks (each block of 1236 bytes)
    // Earlier this would create blocks ~20% greater than the block size of 1024 bytes
    // After this patch actual block size is ~2% greater than the block size of 1024 bytes
    Assert.assertEquals(278, trailer.getDataIndexCount());
  }

  private void writeKeyValues(int entryCount, HFile.Writer writer, List<KeyValue> keyValues)
    throws IOException {
    for (int i = 0; i < entryCount; ++i) {
      byte[] keyBytes = intToBytes(i);

      byte[] valueBytes = new byte[0];
      KeyValue keyValue = new KeyValue(keyBytes, null, null, valueBytes);

      writer.append(keyValue);
      keyValues.add(keyValue);
    }
    writer.close();
  }

  private byte[] intToBytes(final int i) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(i);
    return bb.array();
  }
}
