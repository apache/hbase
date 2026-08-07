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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestGzipByteBuffDecompressor {

  /*
   * "HBase is fun to use and very fast" compressed as a single gzip member via GZIPOutputStream,
   * matching the framing that ReusableStreamGzipCodec produces on the compression side.
   */
  private static final byte[] COMPRESSED_PAYLOAD = Bytes.fromHex(
    "1f8b08000000000000fff3704a2c4e55c82c56482bcd5328c9572805f212f35214ca528b2a15d2128b4b006edf170321000000");

  /**
   * GzipByteBuffDecompressor is backed by Hadoop's native zlib binding, so actually decompressing
   * anything requires that native library to be loaded on this JVM.
   */
  private static void assumeNativeZlibLoaded() {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded(),
      "Hadoop's native code is not loaded on this JVM, skipping");
  }

  @Test
  public void testCapabilitiesWithoutNativeZlibLoaded() {
    // Deliberately constructed as if native zlib is unavailable, regardless of this JVM's actual
    // environment, so this test is deterministic everywhere.
    ByteBuff emptySingleDirectBuff = new SingleByteBuff(ByteBuffer.allocateDirect(0));
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptySingleDirectBuff),
        "Without native zlib there is no way to decompress via ByteBuffs");
    }
  }

  @Test
  public void testCapabilitiesWithNativeZlibLoaded() {
    assumeNativeZlibLoaded();
    ByteBuff emptySingleHeapBuff = new SingleByteBuff(ByteBuffer.allocate(0));
    ByteBuff emptyMultiHeapBuff = new MultiByteBuff(ByteBuffer.allocate(0), ByteBuffer.allocate(0));
    ByteBuff emptySingleDirectBuff = new SingleByteBuff(ByteBuffer.allocateDirect(0));
    ByteBuff emptyMultiDirectBuff =
      new MultiByteBuff(ByteBuffer.allocateDirect(0), ByteBuffer.allocateDirect(0));

    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      assertTrue(decompressor.canDecompress(emptySingleDirectBuff, emptySingleDirectBuff));
      // The native zlib binding reads/writes buffer memory directly, so only direct buffers are
      // supported; heap buffers must fall back to stream-based decompression instead.
      assertFalse(decompressor.canDecompress(emptySingleHeapBuff, emptySingleHeapBuff));
      assertFalse(decompressor.canDecompress(emptySingleHeapBuff, emptySingleDirectBuff));
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptySingleHeapBuff));
      assertFalse(decompressor.canDecompress(emptyMultiHeapBuff, emptyMultiHeapBuff));
      assertFalse(decompressor.canDecompress(emptyMultiDirectBuff, emptyMultiDirectBuff));
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptyMultiDirectBuff));
    }
  }

  private static ByteBuff directBuffWith(byte[] data) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
    buffer.put(data);
    buffer.rewind();
    return new SingleByteBuff(buffer);
  }

  @Test
  public void testDecompressDirectToDirect() throws IOException {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void testDecompressFailsOnTooShortInput() throws IOException {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.allocateDirect(10));
      decompressor.decompress(output, input, 10);
      fail("Expected an IOException because the input is too short to be a gzip member");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("too short to be a gzip member"));
    }
  }

  @Test
  public void testDecompressFailsOnBadMagicBytes() throws IOException {
    assumeNativeZlibLoaded();
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[0] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException because the magic bytes are wrong");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Invalid gzip stream"));
    }
  }

  @Test
  public void testDecompressFailsWhenOutputBufferTooSmall() throws IOException {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(10));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);
      decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      fail("Expected an IOException because the output buffer is too small");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Output buffer is too small"));
    }
  }

  @Test
  public void testDecompressFailsOnCorruptedCrc32() throws IOException {
    assumeNativeZlibLoaded();
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    // First 4 bytes of the 8-byte trailer are the CRC32, leave ISIZE (the last 4 bytes) alone.
    corrupted[corrupted.length - 8] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException because the trailer's CRC32 no longer matches");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Invalid gzip stream"));
    }
  }

  @Test
  public void testDecompressFailsOnCorruptedIsize() throws IOException {
    assumeNativeZlibLoaded();
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    // Last 4 bytes of the 8-byte trailer are the ISIZE.
    corrupted[corrupted.length - 4] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException because the trailer's ISIZE no longer matches");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Invalid gzip stream"));
    }
  }

  @Test
  public void testDecompressSucceedsRepeatedlyOnTheSameDecompressor() throws IOException {
    assumeNativeZlibLoaded();
    // Mirrors how CodecPool actually uses these: one instance is reused across many blocks, so the
    // native decompressor must produce a correct result on every call, not just the first.
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      for (int i = 0; i < 3; i++) {
        ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
        ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);
        int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
        assertEquals("HBase is fun to use and very fast",
          Bytes.toString(output.toBytes(0, decompressedSize)));
      }
    }
  }

  @Test
  public void testDecompressorIsStillUsableAfterAPreviousCallThrows() throws IOException {
    assumeNativeZlibLoaded();
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    // First 4 bytes of the 8-byte trailer are the CRC32.
    corrupted[corrupted.length - 8] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff badOutput = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff badInput = directBuffWith(corrupted);
      try {
        decompressor.decompress(badOutput, badInput, corrupted.length);
        fail("Expected an IOException because the trailer's CRC32 no longer matches");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("Invalid gzip stream"));
      }

      // A prior failure must not leave the shared native decompressor state corrupted for the
      // next, valid call.
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);
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
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);

      Configuration conf = new Configuration(false);
      conf.setBoolean("hbase.io.compress.gz.allowByteBuffDecompression", false);
      decompressor.reinit(GzipHFileDecompressionContext.fromConfiguration(conf));
      assertFalse(decompressor.canDecompress(output, input),
        "Block reader must fall back to stream decompression when the config flag "
          + "disables ByteBuff decompression");

      conf.setBoolean("hbase.io.compress.gz.allowByteBuffDecompression", true);
      decompressor.reinit(GzipHFileDecompressionContext.fromConfiguration(conf));
      assertTrue(decompressor.canDecompress(output, input),
        "Block reader must use ByteBuff decompression when the config flag is enabled");

      // The default, with no config value set, must also allow ByteBuff decompression.
      decompressor
        .reinit(GzipHFileDecompressionContext.fromConfiguration(new Configuration(false)));
      assertTrue(decompressor.canDecompress(output, input));
    }
  }

  @Test
  public void testReinitWithNullContextIsNoOp() {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);

      Configuration conf = new Configuration(false);
      conf.setBoolean("hbase.io.compress.gz.allowByteBuffDecompression", false);
      decompressor.reinit(GzipHFileDecompressionContext.fromConfiguration(conf));
      assertFalse(decompressor.canDecompress(output, input));

      decompressor.reinit(null);
      assertFalse(decompressor.canDecompress(output, input),
        "reinit(null) must not reset allowByteBuffDecompression back to the default");
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
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      decompressor.reinit(wrongContext);
      fail("Expected an IllegalArgumentException because the context was not a "
        + "GzipHFileDecompressionContext");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("GzipHFileDecompressionContext"));
    }
  }

}
