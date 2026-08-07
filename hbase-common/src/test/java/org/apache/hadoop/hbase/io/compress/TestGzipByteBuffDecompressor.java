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
    ByteBuff emptySingleHeapBuff = new SingleByteBuff(ByteBuffer.allocate(0));
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptySingleDirectBuff),
        "Without native zlib, direct-to-direct decompression is not available");
      assertTrue(decompressor.canDecompress(emptySingleHeapBuff, emptySingleHeapBuff),
        "On-heap decompression uses Java Inflater and works without native zlib");
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
      assertTrue(decompressor.canDecompress(emptySingleHeapBuff, emptySingleHeapBuff),
        "On-heap decompression is supported when both buffers are heap SingleByteBuffs");
      // Mixed (one direct, one heap) is not supported: decompressOnHeap() requires both to have
      // backing arrays, and decompressOffHeap() requires both to be direct.
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

  private static ByteBuff heapBuffWith(byte[] data) {
    ByteBuffer buffer = ByteBuffer.allocate(data.length);
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

  // ---------------------------------------------------------------------------
  // On-heap (heap→heap) decompression tests
  //
  // On-heap decompression uses Java's Inflater (raw DEFLATE) and does not rely on the native zlib
  // library, so these tests use GzipByteBuffDecompressor(false) and are deterministic everywhere.
  // ---------------------------------------------------------------------------

  @Test
  public void itDecompressesHeapToHeapSuccessfully() throws IOException {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void itDecompressesHeapToHeapSuccessfullyWhenNativeZlibIsAlsoAvailable()
    throws IOException {
    assumeNativeZlibLoaded();
    // The on-heap path always uses Java Inflater regardless of native availability; this confirms
    // that routing to on-heap doesn't accidentally use the native decompressor.
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void itDecompressesHeapToHeapFailsWhenOutputBufferTooSmall() throws IOException {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(10));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      fail("Expected an IOException because the output buffer is too small");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Output buffer is too small"));
    }
  }

  @Test
  public void itDecompressesHeapToHeapFailsOnTooShortInput() throws IOException {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = new SingleByteBuff(ByteBuffer.allocate(10));
      decompressor.decompress(output, input, 10);
      fail("Expected an IOException because the input is too short to be a gzip member");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("too short to be a gzip member"));
    }
  }

  @Test
  public void itDecompressesHeapToHeapFailsOnCorruptedDeflateBody() throws IOException {
    // Corrupt a byte inside the DEFLATE payload (bytes 10 through len-9). Depending on where the
    // bit-flip lands, the Java Inflater either throws DataFormatException immediately ("Invalid
    // gzip stream") or produces wrong output that the subsequent CRC32 check catches ("Gzip CRC32
    // mismatch"). Either way the corruption must not silently pass.
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[15] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException because the DEFLATE payload is corrupted");
    } catch (IOException e) {
      assertTrue(
        e.getMessage().contains("Invalid gzip stream") || e.getMessage().contains("Gzip CRC32 mismatch"),
        "Expected an IOException about corrupted gzip data, got: " + e.getMessage());
    }
  }

  @Test
  public void itDecompressesHeapToHeapFailsOnCrc32Mismatch() throws IOException {
    // The on-heap path verifies the CRC32 field in the trailer itself (the Java Inflater only
    // handles raw DEFLATE and never inspects the gzip envelope). Corrupt the first 4 bytes of the
    // 8-byte trailer (CRC32), leaving ISIZE intact, to trigger exactly the CRC32 check.
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[corrupted.length - 8] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException due to a CRC32 mismatch in the gzip trailer");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Gzip CRC32 mismatch"));
    }
  }

  @Test
  public void itDecompressesHeapToHeapFailsOnIsizeMismatch() throws IOException {
    // Corrupt the last 4 bytes of the 8-byte trailer (ISIZE), leaving CRC32 intact, to trigger
    // exactly the size check. This catches truncated or padded output that slipped past CRC.
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[corrupted.length - 4] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException due to an ISIZE mismatch in the gzip trailer");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Gzip size mismatch"));
    }
  }

  @Test
  public void itDecompressesHeapToHeapSucceedsRepeatedly() throws IOException {
    // The Java Inflater must be reset between calls; verify multiple sequential decompressions on
    // the same instance all produce correct output.
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      for (int i = 0; i < 3; i++) {
        ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
        ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
        int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
        assertEquals("HBase is fun to use and very fast",
          Bytes.toString(output.toBytes(0, decompressedSize)));
      }
    }
  }

  @Test
  public void itDecompressesHeapToHeapIsStillUsableAfterAPreviousCallThrows() throws IOException {
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[corrupted.length - 8] ^= (byte) 0xff;
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      ByteBuff badOutput = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff badInput = heapBuffWith(corrupted);
      try {
        decompressor.decompress(badOutput, badInput, corrupted.length);
        fail("Expected an IOException because the CRC32 is corrupted");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("Gzip CRC32 mismatch"));
      }

      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

}
