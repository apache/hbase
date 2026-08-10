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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
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

  // A single gzip member, reused as decompressor input across the tests.
  private static final byte[] COMPRESSED_PAYLOAD = gzip("HBase is fun to use and very fast");

  /**
   * GzipByteBuffDecompressor is backed by Hadoop's native zlib binding, so actually decompressing
   * anything requires that native library to be loaded on this JVM.
   */
  private static void assumeNativeZlibLoaded() {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded(),
      "Hadoop's native code is not loaded on this JVM, skipping");
  }

  @Test
  public void itReportsCorrectCapabilitiesWithoutNativeZlib() {
    ByteBuff emptySingleDirectBuff = new SingleByteBuff(ByteBuffer.allocateDirect(0));
    ByteBuff emptySingleHeapBuff = new SingleByteBuff(ByteBuffer.allocate(0));
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptySingleDirectBuff),
        "Without native zlib, direct-to-direct decompression is not available");
      assertFalse(decompressor.canDecompress(emptySingleHeapBuff, emptySingleHeapBuff),
        "Without native zlib, heap decompression is not available");
    }
  }

  @Test
  public void itReportsCorrectCapabilitiesWithNativeZlib() {
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

  private static byte[] gzip(String text) {
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (GZIPOutputStream out = new GZIPOutputStream(compressed)) {
      out.write(Bytes.toBytes(text));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return compressed.toByteArray();
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] combined = new byte[first.length + second.length];
    System.arraycopy(first, 0, combined, 0, first.length);
    System.arraycopy(second, 0, combined, first.length, second.length);
    return combined;
  }

  @Test
  public void itDecompressesDirectToDirectSuccessfully() throws IOException {
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
  public void itDecompressDirectFailsOnTooShortInput() throws IOException {
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
  public void itDecompressDirectFailsOnBadMagicBytes() throws IOException {
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
  public void itDecompressDirectFailsWhenOutputBufferTooSmall() throws IOException {
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
  public void itDecompressDirectFailsOnCorruptedCrc32() throws IOException {
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
  public void itDecompressDirectFailsOnCorruptedIsize() throws IOException {
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
  public void itDecompressesDirectSuccessfullyOnRepeatedCalls() throws IOException {
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
  public void itDecompressDirectIsStillUsableAfterAPreviousCallThrows() throws IOException {
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

  @Test
  public void itDecompressesDirectToDirectWithNonZeroBufferPosition() throws IOException {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuffer rawOutput = ByteBuffer.allocateDirect(128);
      rawOutput.position(32);

      ByteBuffer rawInput = ByteBuffer.allocateDirect(16 + COMPRESSED_PAYLOAD.length);
      for (int i = 0; i < 16; i++) {
        rawInput.put((byte) 0);
      }
      rawInput.put(COMPRESSED_PAYLOAD);
      rawInput.position(16);

      ByteBuff output = new SingleByteBuff(rawOutput);
      ByteBuff input = new SingleByteBuff(rawInput);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);

      byte[] result = new byte[decompressedSize];
      rawOutput.position(32);
      rawOutput.get(result);
      assertEquals("HBase is fun to use and very fast", Bytes.toString(result));
    }
  }

  @Test
  public void itDecompressDirectFailsOnTruncatedGzipStream() throws IOException {
    assumeNativeZlibLoaded();
    byte[] truncated = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length - 4);
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(truncated);
      decompressor.decompress(output, input, truncated.length);
      fail("Expected an IOException because the gzip stream is truncated");
    } catch (IOException e) {
      // Expected: the decompressor must not report finished() on an incomplete stream
    }
  }

  @Test
  public void itDecompressesOnlyTheDelimitedMemberFromAMultiMemberPayload() throws IOException {
    assumeNativeZlibLoaded();
    // Two distinct members concatenated: we must decode only the one delimited by inputLen.
    byte[] firstMember = gzip("first member");
    byte[] secondMember = gzip("second member");
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(concat(firstMember, secondMember));
      int decompressedSize = decompressor.decompress(output, input, firstMember.length);
      assertEquals("first member", Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  /**
   * This is the exact gate {@code HFileBlockDefaultDecodingContext#canDecompressViaByteBuff} relies
   * on to decide between ByteBuff decompression and the stream path, driven end-to-end from the
   * {@code GzipHFileDecompressionContext#ALLOW_BYTE_BUFF_DECOMPRESSION_KEY} config flag.
   */
  @Test
  public void itReinitControlsByteBuffDecompressionViaConfigFlag() {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);

      Configuration conf = new Configuration(false);
      conf.setBoolean(GzipHFileDecompressionContext.ALLOW_BYTE_BUFF_DECOMPRESSION_KEY, false);
      decompressor.reinit(GzipHFileDecompressionContext.fromConfiguration(conf));
      assertFalse(decompressor.canDecompress(output, input),
        "Block reader must fall back to stream decompression when the config flag "
          + "disables ByteBuff decompression");

      conf.setBoolean(GzipHFileDecompressionContext.ALLOW_BYTE_BUFF_DECOMPRESSION_KEY, true);
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
  public void itReinitWithNullContextIsNoOp() {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);

      Configuration conf = new Configuration(false);
      conf.setBoolean(GzipHFileDecompressionContext.ALLOW_BYTE_BUFF_DECOMPRESSION_KEY, false);
      decompressor.reinit(GzipHFileDecompressionContext.fromConfiguration(conf));
      assertFalse(decompressor.canDecompress(output, input));

      decompressor.reinit(null);
      assertFalse(decompressor.canDecompress(output, input),
        "reinit(null) must not reset allowByteBuffDecompression back to the default");
    }
  }

  @Test
  public void itReinitFailsOnWrongContextType() {
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

  @Test
  public void itDecompressThrowsWhenPassedAMultiByteBuff() throws IOException {
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(false)) {
      ByteBuff multiOutput = new MultiByteBuff(ByteBuffer.allocate(64), ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      decompressor.decompress(multiOutput, input, COMPRESSED_PAYLOAD.length);
      fail("Expected an IllegalStateException when output is a MultiByteBuff");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("not a SingleByteBuff"));
    }
  }

  // On-heap (heap -> heap) decompression tests; these also require native zlib.

  @Test
  public void itDecompressesHeapToHeapSuccessfully() throws IOException {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
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
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
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
    // Corrupt a byte inside the DEFLATE payload (bytes 10 through len-9). ZlibDecompressor with
    // GZIP_FORMAT will reject the corrupted data via native zlib's CRC/format checks.
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[15] ^= (byte) 0xff;
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException because the DEFLATE payload is corrupted");
    } catch (IOException e) {
      // Expected; the exact message depends on native zlib internals
    }
  }

  @Test
  public void itDecompressesHeapToHeapFailsOnCrc32Mismatch() throws IOException {
    // Corrupt the first 4 bytes of the 8-byte trailer (CRC32), leaving ISIZE intact.
    // ZlibDecompressor with GZIP_FORMAT verifies the CRC32 field via native zlib.
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[corrupted.length - 8] ^= (byte) 0xff;
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException due to a CRC32 mismatch in the gzip trailer");
    } catch (IOException e) {
      // Expected; ZlibDecompressor delegates CRC32 verification to native zlib
    }
  }

  @Test
  public void itDecompressesHeapToHeapFailsOnIsizeMismatch() throws IOException {
    // Corrupt the last 4 bytes of the 8-byte trailer (ISIZE), leaving CRC32 intact.
    // ZlibDecompressor with GZIP_FORMAT verifies the ISIZE field via native zlib.
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[corrupted.length - 4] ^= (byte) 0xff;
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(corrupted);
      decompressor.decompress(output, input, corrupted.length);
      fail("Expected an IOException due to an ISIZE mismatch in the gzip trailer");
    } catch (IOException e) {
      // Expected; ZlibDecompressor delegates ISIZE verification to native zlib
    }
  }

  @Test
  public void itDecompressesHeapToHeapSucceedsRepeatedly() throws IOException {
    // ZlibDecompressor must be reset between calls; verify multiple sequential decompressions on
    // the same instance all produce correct output.
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
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
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff badOutput = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff badInput = heapBuffWith(corrupted);
      try {
        decompressor.decompress(badOutput, badInput, corrupted.length);
        fail("Expected an IOException because the CRC32 is corrupted");
      } catch (IOException e) {
        // Expected
      }

      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void itDecompressesHeapToHeapSuccessfullyWithNonZeroBufferPosition() throws IOException {
    assumeNativeZlibLoaded();
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuffer rawOutput = ByteBuffer.allocate(128);
      rawOutput.position(32);

      ByteBuffer rawInput = ByteBuffer.allocate(16 + COMPRESSED_PAYLOAD.length);
      rawInput.put(new byte[16]);
      rawInput.put(COMPRESSED_PAYLOAD);
      rawInput.position(16);

      ByteBuff output = new SingleByteBuff(rawOutput);
      ByteBuff input = new SingleByteBuff(rawInput);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);

      assertEquals("HBase is fun to use and very fast",
        Bytes.toString(rawOutput.array(), rawOutput.arrayOffset() + 32, decompressedSize));
    }
  }

  @Test
  public void itDecompressesHeapToHeapFailsOnTruncatedGzipStream() throws IOException {
    assumeNativeZlibLoaded();
    byte[] truncated = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length - 4);
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(truncated);
      decompressor.decompress(output, input, truncated.length);
      fail("Expected an IOException because the gzip stream is truncated");
    } catch (IOException e) {
      // Expected: the decompressor must not report finished() on an incomplete stream
    }
  }

  @Test
  public void itDecompressesOnlyTheDelimitedMemberFromAMultiMemberPayloadOnHeap()
    throws IOException {
    assumeNativeZlibLoaded();
    // Two distinct members concatenated, on the on-heap path: decode only the delimited one.
    byte[] firstMember = gzip("first member");
    byte[] secondMember = gzip("second member");
    try (GzipByteBuffDecompressor decompressor = new GzipByteBuffDecompressor(true)) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(concat(firstMember, secondMember));
      int decompressedSize = decompressor.decompress(output, input, firstMember.length);
      assertEquals("first member", Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

}
