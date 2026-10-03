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
package org.apache.hadoop.hbase.io.compress.xerial;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestSnappyByteBuffDecompressor {

  private static final String TEST_TEXT = "HBase is fun to use and very fast";
  // A single BlockCompressorStream-framed Snappy member, reused as decompressor input across tests.
  private static byte[] COMPRESSED_PAYLOAD;

  @BeforeAll
  public static void setUpBeforeClass() {
    assumeTrue(SnappyCodec.isLoaded());
    COMPRESSED_PAYLOAD = snappyCompress(TEST_TEXT);
  }

  private static byte[] snappyCompress(String text) {
    SnappyCodec codec = new SnappyCodec();
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (CompressionOutputStream out = codec.createOutputStream(compressed)) {
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
  public void itReportsCorrectCapabilities() {
    ByteBuff emptySingleHeapBuff = new SingleByteBuff(ByteBuffer.allocate(0));
    ByteBuff emptyMultiHeapBuff = new MultiByteBuff(ByteBuffer.allocate(0), ByteBuffer.allocate(0));
    ByteBuff emptySingleDirectBuff = new SingleByteBuff(ByteBuffer.allocateDirect(0));
    ByteBuff emptyMultiDirectBuff =
      new MultiByteBuff(ByteBuffer.allocateDirect(0), ByteBuffer.allocateDirect(0));

    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      assertTrue(decompressor.canDecompress(emptySingleHeapBuff, emptySingleHeapBuff),
        "heap-to-heap is supported via the byte array API");
      assertTrue(decompressor.canDecompress(emptySingleDirectBuff, emptySingleDirectBuff),
        "direct-to-direct is supported via the ByteBuffer API");
      // Xerial Snappy has no mixed direct/heap overload.
      assertFalse(decompressor.canDecompress(emptySingleHeapBuff, emptySingleDirectBuff),
        "heap output with direct input is not supported");
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptySingleHeapBuff),
        "direct output with heap input is not supported");
      assertFalse(decompressor.canDecompress(emptyMultiHeapBuff, emptyMultiHeapBuff));
      assertFalse(decompressor.canDecompress(emptyMultiDirectBuff, emptyMultiDirectBuff));
      assertFalse(decompressor.canDecompress(emptySingleHeapBuff, emptyMultiHeapBuff));
      assertFalse(decompressor.canDecompress(emptySingleDirectBuff, emptyMultiDirectBuff));
    }
  }

  @Test
  public void itDecompressesHeapToHeap() throws IOException {
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(128));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals(TEST_TEXT, Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void itDecompressesDirectToDirect() throws IOException {
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(128));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals(TEST_TEXT, Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void itDecompressesDirectToDirectWithNonZeroBufferPosition() throws IOException {
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
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
      assertEquals(TEST_TEXT, Bytes.toString(result));
    }
  }

  @Test
  public void itDecompressesHeapToHeapWithNonZeroBufferPosition() throws IOException {
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      byte[] paddedInput = concat(new byte[16], COMPRESSED_PAYLOAD);
      ByteBuffer rawInput = ByteBuffer.wrap(paddedInput);
      rawInput.position(16);

      ByteBuffer rawOutput = ByteBuffer.allocate(128);
      rawOutput.position(32);

      ByteBuff output = new SingleByteBuff(rawOutput);
      ByteBuff input = new SingleByteBuff(rawInput);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);

      byte[] result = new byte[decompressedSize];
      rawOutput.position(32);
      rawOutput.get(result);
      assertEquals(TEST_TEXT, Bytes.toString(result));
    }
  }

  @Test
  public void itDecompressesDirectToDirectSuccessfullyOnRepeatedCalls() throws IOException {
    // Mirrors how CodecPool reuses decompressors across many blocks.
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      for (int i = 0; i < 3; i++) {
        ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(128));
        ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);
        int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
        assertEquals(TEST_TEXT, Bytes.toString(output.toBytes(0, decompressedSize)));
      }
    }
  }

  @Test
  public void itDecompressesHeapToHeapSuccessfullyOnRepeatedCalls() throws IOException {
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      for (int i = 0; i < 3; i++) {
        ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(128));
        ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
        int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
        assertEquals(TEST_TEXT, Bytes.toString(output.toBytes(0, decompressedSize)));
      }
    }
  }

  @Test
  public void itDecompressesDirectOnlyTheDelimitedMemberFromAMultiMemberPayload()
    throws IOException {
    // Two distinct BlockCompressorStream members concatenated: only the first (bounded by inputLen)
    // must be decompressed. The second member's bytes must not be consumed.
    byte[] firstMember = snappyCompress("first member");
    byte[] secondMember = snappyCompress("second member");
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(64));
      ByteBuff input = directBuffWith(concat(firstMember, secondMember));
      int decompressedSize = decompressor.decompress(output, input, firstMember.length);
      assertEquals("first member", Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void itDecompressesHeapOnlyTheDelimitedMemberFromAMultiMemberPayload() throws IOException {
    byte[] firstMember = snappyCompress("first member");
    byte[] secondMember = snappyCompress("second member");
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(concat(firstMember, secondMember));
      int decompressedSize = decompressor.decompress(output, input, firstMember.length);
      assertEquals("first member", Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void itDecompressDirectIsStillUsableAfterAPreviousCallThrows() throws IOException {
    // Corrupt the Snappy payload bytes (past the 8-byte BlockCompressorStream header) to force a
    // decompression error, then verify the decompressor is still functional for a subsequent call.
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[corrupted.length / 2] ^= (byte) 0xff;
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff badOutput = new SingleByteBuff(ByteBuffer.allocateDirect(128));
      ByteBuff badInput = directBuffWith(corrupted);
      try {
        decompressor.decompress(badOutput, badInput, corrupted.length);
        fail("Expected an IOException for corrupted Snappy data");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("Snappy decompression failed"));
      }

      // A prior failure must not leave the decompressor in a broken state.
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(128));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals(TEST_TEXT, Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  @Test
  public void itDecompressHeapIsStillUsableAfterAPreviousCallThrows() throws IOException {
    byte[] corrupted = Arrays.copyOf(COMPRESSED_PAYLOAD, COMPRESSED_PAYLOAD.length);
    corrupted[corrupted.length / 2] ^= (byte) 0xff;
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff badOutput = new SingleByteBuff(ByteBuffer.allocate(128));
      ByteBuff badInput = heapBuffWith(corrupted);
      try {
        decompressor.decompress(badOutput, badInput, corrupted.length);
        fail("Expected an IOException for corrupted Snappy data");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("Snappy decompression failed"));
      }

      ByteBuff output = new SingleByteBuff(ByteBuffer.allocate(128));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      int decompressedSize = decompressor.decompress(output, input, COMPRESSED_PAYLOAD.length);
      assertEquals(TEST_TEXT, Bytes.toString(output.toBytes(0, decompressedSize)));
    }
  }

  /**
   * This is the exact gate {@code HFileBlockDefaultDecodingContext#canDecompressViaByteBuff} relies
   * on to decide between ByteBuff decompression and the stream path, driven end-to-end from the
   * {@code SnappyHFileDecompressionContext#ALLOW_BYTE_BUFF_DECOMPRESSION_KEY} config flag.
   */
  @Test
  public void itReinitControlsByteBuffDecompressionViaConfigFlag() {
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(128));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);

      Configuration conf = new Configuration(false);
      conf.setBoolean(SnappyHFileDecompressionContext.ALLOW_BYTE_BUFF_DECOMPRESSION_KEY, false);
      decompressor.reinit(SnappyHFileDecompressionContext.fromConfiguration(conf));
      assertFalse(decompressor.canDecompress(output, input),
        "Block reader must fall back to stream decompression when the config flag "
          + "disables ByteBuff decompression");

      conf.setBoolean(SnappyHFileDecompressionContext.ALLOW_BYTE_BUFF_DECOMPRESSION_KEY, true);
      decompressor.reinit(SnappyHFileDecompressionContext.fromConfiguration(conf));
      assertTrue(decompressor.canDecompress(output, input),
        "Block reader must use ByteBuff decompression when the config flag is enabled");

      // The default, with no config value set, must also allow ByteBuff decompression.
      decompressor
        .reinit(SnappyHFileDecompressionContext.fromConfiguration(new Configuration(false)));
      assertTrue(decompressor.canDecompress(output, input));
    }
  }

  @Test
  public void itReinitWithNullContextIsNoOp() {
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff output = new SingleByteBuff(ByteBuffer.allocateDirect(128));
      ByteBuff input = directBuffWith(COMPRESSED_PAYLOAD);

      Configuration conf = new Configuration(false);
      conf.setBoolean(SnappyHFileDecompressionContext.ALLOW_BYTE_BUFF_DECOMPRESSION_KEY, false);
      decompressor.reinit(SnappyHFileDecompressionContext.fromConfiguration(conf));
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
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      decompressor.reinit(wrongContext);
      fail("Expected an IllegalArgumentException because the context was not a "
        + "SnappyHFileDecompressionContext");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("SnappyHFileDecompressionContext"));
    }
  }

  @Test
  public void itDecompressThrowsWhenPassedAMultiByteBuff() throws IOException {
    try (SnappyByteBuffDecompressor decompressor = new SnappyByteBuffDecompressor()) {
      ByteBuff multiOutput = new MultiByteBuff(ByteBuffer.allocate(64), ByteBuffer.allocate(64));
      ByteBuff input = heapBuffWith(COMPRESSED_PAYLOAD);
      decompressor.decompress(multiOutput, input, COMPRESSED_PAYLOAD.length);
      fail("Expected an IllegalStateException when output is a MultiByteBuff");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("not a SingleByteBuff"));
    }
  }
}
