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
package org.apache.hadoop.hbase.io.compress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
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
public class TestGzipByteBuffDecompressor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestGzipByteBuffDecompressor.class);

  /*
   * "HBase is fun to use and very fast" compressed as a single gzip member via GZIPOutputStream,
   * i.e. exactly the framing GzipByteBuffDecompressor expects: a fixed 10-byte header (no
   * FEXTRA/FNAME/FCOMMENT/FHCRC), a raw DEFLATE stream, and an 8-byte CRC32/ISIZE trailer.
   */
  private static final byte[] COMPRESSED_PAYLOAD = Bytes.fromHex(
    "1f8b08000000000000fff3704a2c4e55c82c56482bcd5328c9572805f212f35214ca528b2a15d2128b4b006edf170321000000");

  @Test
  public void testCapabilities() {
    ByteBuff emptySingleHeapBuff = new SingleByteBuff(ByteBuffer.allocate(0));
    ByteBuff emptyMultiHeapBuff = new MultiByteBuff(ByteBuffer.allocate(0), ByteBuffer.allocate(0));
    ByteBuff emptySingleDirectBuff = new SingleByteBuff(ByteBuffer.allocateDirect(0));
    ByteBuff emptyMultiDirectBuff =
      new MultiByteBuff(ByteBuffer.allocateDirect(0), ByteBuffer.allocateDirect(0));

    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      assertTrue(decompressor.canDecompress(emptySingleHeapBuff, emptySingleHeapBuff));
      assertTrue(decompressor.canDecompress(emptySingleDirectBuff, emptySingleDirectBuff));
      assertTrue(decompressor.canDecompress(emptySingleHeapBuff, emptySingleDirectBuff));
      assertTrue(decompressor.canDecompress(emptySingleDirectBuff, emptySingleHeapBuff));
      assertFalse(decompressor.canDecompress(emptyMultiHeapBuff, emptyMultiHeapBuff));
      assertFalse(decompressor.canDecompress(emptyMultiDirectBuff, emptyMultiDirectBuff));
      assertFalse(decompressor.canDecompress(emptySingleHeapBuff, emptyMultiHeapBuff));
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptyMultiDirectBuff));
    }
  }

  @Test
  public void testDecompressHeapToHeap() throws IOException {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(COMPRESSED_PAYLOAD));
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void testDecompressDirectToDirect() throws IOException {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.allocateDirect(COMPRESSED_PAYLOAD.length));
      input.put(COMPRESSED_PAYLOAD);
      input.rewind();
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void testDecompressDirectToHeap() throws IOException {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.allocateDirect(COMPRESSED_PAYLOAD.length));
      input.put(COMPRESSED_PAYLOAD);
      input.rewind();
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void testDecompressHeapToDirect() throws IOException {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(COMPRESSED_PAYLOAD));
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void testDecompressFailsOnTooShortInput() {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.allocate(10));
      decompressor.decompress(output, input, 10);
      fail("Expected an IOException because the input is too short to be a gzip member");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("too short to be a gzip member"));
    }
  }

  @Test
  public void testDecompressFailsOnBadMagicBytes() {
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[0] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(corrupted));
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException because the magic bytes are wrong");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("bad magic bytes"));
    }
  }

  @Test
  public void testDecompressFailsWhenOutputBufferTooSmall() {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(10));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(COMPRESSED_PAYLOAD));
      decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      fail("Expected an IOException because the output buffer is too small");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Output buffer is too small"));
    }
  }

  @Test
  public void testDecompressFailsOnCorruptedCrc32() {
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    // First 4 bytes of the 8-byte trailer are the CRC32, leave ISIZE (the last 4 bytes) alone.
    corrupted[corrupted.length - 8] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(corrupted));
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException because the trailer's CRC32 no longer matches");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("CRC32"));
    }
  }

  @Test
  public void testDecompressFailsOnCorruptedIsize() {
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    // Last 4 bytes of the 8-byte trailer are the ISIZE.
    corrupted[corrupted.length - 4] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(corrupted));
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException because the trailer's ISIZE no longer matches");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("ISIZE"));
    }
  }

  @Test
  public void testDecompressSucceedsRepeatedlyOnTheSameDecompressor() throws IOException {
    // Mirrors how CodecPool actually uses these: one instance is reused across many blocks, so the
    // trailer (CRC32/ISIZE) verification must produce a correct result on every call, not just the
    // first.
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      for (int i = 0; i < 3; i++) {
        ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
        ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(COMPRESSED_PAYLOAD));
        int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
        assertEquals("HBase is fun to use and very fast",
          Bytes.toString(output.toBytes(0, decompressedSize)));
      }
    }
  }

  @Test
  public void testDecompressorIsStillUsableAfterAPreviousCallThrows() throws IOException {
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    // First 4 bytes of the 8-byte trailer are the CRC32.
    corrupted[corrupted.length - 8] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff badOutput = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff badInput = new SingleByteBuff(ByteBuffer.wrap(corrupted));
      try {
        decompressor.decompress(badOutput, badInput, corrupted.length);
        fail("Expected an IOException because the trailer's CRC32 no longer matches");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("CRC32"));
      }

      // A prior failure must not leave the shared Inflater/CRC32 state corrupted for the next,
      // valid call.
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(COMPRESSED_PAYLOAD));
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  /**
   * This is the exact gate {@code HFileBlockDefaultDecodingContext#canDecompressViaByteBuff} relies
   * on to decide between ByteBuff decompression and the stream path, driven end-to-end from the
   * {@code hbase.io.compress.gz.allowByteBuffDecompression} config flag.
   */
  @Test
  public void testReinitControlsByteBuffDecompressionViaConfigFlag() {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(COMPRESSED_PAYLOAD));

      Configuration conf = new Configuration(false);
      conf.setBoolean("hbase.io.compress.gz.allowByteBuffDecompression", false);
      decompressor.reinit(GzipHFileDecompressionContext.fromConfiguration(conf));
      assertFalse("Block reader must fall back to stream decompression when the config flag "
        + "disables ByteBuff decompression", decompressor.canDecompress(output, input));

      conf.setBoolean("hbase.io.compress.gz.allowByteBuffDecompression", true);
      decompressor.reinit(GzipHFileDecompressionContext.fromConfiguration(conf));
      assertTrue("Block reader must use ByteBuff decompression when the config flag is enabled",
        decompressor.canDecompress(output, input));

      // The default, with no config value set, must also allow ByteBuff decompression.
      decompressor
        .reinit(GzipHFileDecompressionContext.fromConfiguration(new Configuration(false)));
      assertTrue(decompressor.canDecompress(output, input));
    }
  }

  @Test
  public void testReinitWithNullContextIsNoOp() {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.wrap(COMPRESSED_PAYLOAD));

      Configuration conf = new Configuration(false);
      conf.setBoolean("hbase.io.compress.gz.allowByteBuffDecompression", false);
      decompressor.reinit(GzipHFileDecompressionContext.fromConfiguration(conf));
      assertFalse(decompressor.canDecompress(output, input));

      decompressor.reinit(null);
      assertFalse("reinit(null) must not reset allowByteBuffDecompression back to the default",
        decompressor.canDecompress(output, input));
    }
  }

  @Test
  public void testReinitFailsOnWrongContextType() {
    Compression.HFileDecompressionContext wrongContext =
      new Compression.HFileDecompressionContext() {
        @Override
        public void close() {
        }

        @Override
        public long heapSize() {
          return 0;
        }
      };
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor()) {
      decompressor.reinit(wrongContext);
      fail("Expected an IllegalArgumentException because the context was not a "
        + "GzipHFileDecompressionContext");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("GzipHFileDecompressionContext"));
    }
  }

}
