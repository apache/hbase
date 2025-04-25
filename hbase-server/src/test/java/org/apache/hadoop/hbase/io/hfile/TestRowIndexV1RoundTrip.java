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

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SizeCachedNoTagsByteBufferKeyValue;
import org.apache.hadoop.hbase.SizeCachedNoTagsKeyValue;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, MediumTests.class })
public class TestRowIndexV1RoundTrip {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowIndexV1RoundTrip.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final DataBlockEncoding DATA_BLOCK_ENCODING = DataBlockEncoding.ROW_INDEX_V1;
  private static final int ENTRY_COUNT = 100;

  private Configuration conf;
  private FileSystem fs;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setLong(MIN_ALLOCATE_SIZE_KEY, 0);
    fs = FileSystem.get(conf);
  }

  @Test
  public void testReadMyWritesOnHeap() throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(), "testHFileFormatV3");
    writeDataToHFile(hfilePath, ENTRY_COUNT);
    readDataFromHFile(hfilePath, ENTRY_COUNT, true);
  }

  @Test
  public void testReadMyWritesOnDirectMem() throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(), "testHFileFormatV3");
    writeDataToHFile(hfilePath, ENTRY_COUNT);
    readDataFromHFile(hfilePath, ENTRY_COUNT, false);
  }

  private void writeDataToHFile(Path hfilePath, int entryCount) throws IOException {
    HFileContext context =
      new HFileContextBuilder().withBlockSize(1024).withDataBlockEncoding(DATA_BLOCK_ENCODING)
        .withCellComparator(CellComparatorImpl.COMPARATOR).build();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HFile.Writer writer = new HFile.WriterFactory(conf, cacheConfig).withPath(fs, hfilePath)
      .withFileContext(context).create();

    List<KeyValue> keyValues = new ArrayList<>(entryCount);

    writeKeyValues(entryCount, writer, keyValues);
  }

  private void writeKeyValues(int entryCount, HFile.Writer writer, List<KeyValue> keyValues)
    throws IOException {
    for (int i = 0; i < entryCount; ++i) {
      byte[] keyBytes = intToBytes(i);

      byte[] valueBytes = Bytes.toBytes(String.format("value %d", i));
      KeyValue keyValue = new KeyValue(keyBytes, null, null, valueBytes);

      writer.append(keyValue);
      keyValues.add(keyValue);
    }
    writer.close();
  }

  private void readDataFromHFile(Path hfilePath, int entryCount, boolean onHeap)
    throws IOException {
    CacheConfig cacheConfig;
    if (onHeap) {
      cacheConfig = new CacheConfig(conf);
    } else {
      ByteBuffAllocator allocator = ByteBuffAllocator.create(conf, true);
      cacheConfig = new CacheConfig(conf, null, null, allocator);
    }
    HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConfig, false, conf);
    HFileScanner scanner = reader.getScanner(conf, false, false);
    scanner.seekTo();
    int i = 1;
    while (scanner.next()) {
      byte[] keyBytes = intToBytes(i);
      // check row key from getKey() and getCell() separately because they use different code paths
      assertArrayEquals(keyBytes, CellUtil.cloneRow(scanner.getKey()));
      assertArrayEquals(keyBytes, CellUtil.cloneRow(scanner.getCell()));
      assertArrayEquals(Bytes.toBytes(String.format("value %d", i)),
        CellUtil.cloneValue(scanner.getCell()));
      if (onHeap) {
        assertTrue(scanner.getCell() instanceof SizeCachedNoTagsKeyValue);
      } else {
        assertTrue(scanner.getCell() instanceof SizeCachedNoTagsByteBufferKeyValue);
      }
      i += 1;
    }
    assertEquals(entryCount, i);
  }

  private byte[] intToBytes(final int i) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(i);
    return bb.array();
  }
}
