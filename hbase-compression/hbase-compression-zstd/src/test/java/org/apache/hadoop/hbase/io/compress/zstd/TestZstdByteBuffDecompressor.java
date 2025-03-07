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
package org.apache.hadoop.hbase.io.compress.zstd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestZstdByteBuffDecompressor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZstdByteBuffDecompressor.class);

  // "HBase is fun to use and very fast" compressed with zstd, and then prepended with metadata as a
  // BlockCompressorStream would. The phrase is split in three parts and put into the payload in
  // this structure:
  //   block 1:
  //     chunk 1: HBase is
  //     chunk 2: fun to use
  //   block 2:
  //     chunk 1: and very fast

  private static final byte[] COMPRESSED_PAYLOAD =
    Bytes.fromHex("000000130000001228b52ffd20094900004842617365206973200000001428b52ffd200b59000066756e20746f20757365200000000d0000001628b52ffd200d690000616e6420766572792066617374");

  @Test
  public void testCapabilities() {
    ByteBuff emptySingleHeapBuff = new SingleByteBuff(ByteBuffer.allocate(0));
    ByteBuff emptyMultiHeapBuff = new MultiByteBuff(ByteBuffer.allocate(0), ByteBuffer.allocate(0));
    ByteBuff emptySingleDirectBuff = new SingleByteBuff(ByteBuffer.allocateDirect(0));
    ByteBuff emptyMultiDirectBuff =
      new MultiByteBuff(ByteBuffer.allocateDirect(0), ByteBuffer.allocateDirect(0));

    try (ZstdByteBuffDecompressor decompressor = new ZstdByteBuffDecompressor(null)) {
      assertTrue(decompressor.canDecompress(emptySingleHeapBuff, emptySingleHeapBuff));
      assertTrue(decompressor.canDecompress(emptySingleDirectBuff, emptySingleDirectBuff));
      assertFalse(decompressor.canDecompress(emptySingleHeapBuff, emptySingleDirectBuff));
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptySingleHeapBuff));
      assertFalse(decompressor.canDecompress(emptyMultiHeapBuff, emptyMultiHeapBuff));
      assertFalse(decompressor.canDecompress(emptyMultiDirectBuff, emptyMultiDirectBuff));
      assertFalse(decompressor.canDecompress(emptySingleHeapBuff, emptyMultiHeapBuff));
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptyMultiDirectBuff));
    }
  }

  @Test
  public void testDecompressHeap() throws IOException {
    try (ZstdByteBuffDecompressor decompressor = new ZstdByteBuffDecompressor(null)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(COMPRESSED_PAYLOAD));
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast", Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void testDecompressDirect() throws IOException {
    try (ZstdByteBuffDecompressor decompressor = new ZstdByteBuffDecompressor(null)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.allocateDirect(COMPRESSED_PAYLOAD.length));
      input.put(COMPRESSED_PAYLOAD);
      input.rewind();
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast", Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

}
