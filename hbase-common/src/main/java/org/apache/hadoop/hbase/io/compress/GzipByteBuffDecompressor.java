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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Glue for ByteBuffDecompressor on top of {@link Inflater}. Only supports gzip members with the
 * fixed ten-byte header that {@link ReusableStreamGzipCodec} (and Hadoop's native zlib gzip
 * compressor) always writes, i.e. no FEXTRA/FNAME/FCOMMENT/FHCRC, since that is the only format
 * HBase ever produces on the compression side.
 */
@InterfaceAudience.Private
public class GzipByteBuffDecompressor implements ByteBuffDecompressor {

  private static final int GZIP_HEADER_LENGTH = 10;
  private static final int GZIP_TRAILER_LENGTH = 8;
  private static final byte GZIP_MAGIC_0 = (byte) 0x1f;
  private static final byte GZIP_MAGIC_1 = (byte) 0x8b;

  private final Inflater inflater = new Inflater(true);
  // Intended to be set to false by some unit tests
  private boolean allowByteBuffDecompression;

  GzipByteBuffDecompressor() {
    allowByteBuffDecompression = true;
  }

  @Override
  public boolean canDecompress(ByteBuff output, ByteBuff input) {
    return allowByteBuffDecompression && output instanceof SingleByteBuff
      && input instanceof SingleByteBuff;
  }

  @Override
  public int decompress(ByteBuff output, ByteBuff input, int inputLen) throws IOException {
    if (!(output instanceof SingleByteBuff) || !(input instanceof SingleByteBuff)) {
      throw new IllegalStateException(
        "At least one buffer is not a SingleByteBuff, this is not supported");
    }
    if (inputLen < GZIP_HEADER_LENGTH + GZIP_TRAILER_LENGTH) {
      throw new IOException("Input of length " + inputLen + " is too short to be a gzip member");
    }

    ByteBuffer nioInput = input.nioByteBuffers()[0];
    int inputStart = nioInput.position();
    if (nioInput.get(inputStart) != GZIP_MAGIC_0 || nioInput.get(inputStart + 1) != GZIP_MAGIC_1) {
      throw new IOException("Not a gzip member, bad magic bytes");
    }

    ByteBuffer nioOutput = output.nioByteBuffers()[0];

    // Isolate the raw DEFLATE payload (strip the fixed header and the CRC32/ISIZE trailer) into
    // its own view so Inflater can consume it without disturbing nioInput's own position/limit.
    ByteBuffer deflateStream = nioInput.duplicate();
    deflateStream.limit(inputStart + inputLen - GZIP_TRAILER_LENGTH);
    deflateStream.position(inputStart + GZIP_HEADER_LENGTH);

    inflater.reset();
    inflater.setInput(deflateStream);
    int outputStart = nioOutput.position();
    try {
      while (!inflater.finished()) {
        if (inflater.inflate(nioOutput) == 0) {
          if (inflater.finished()) {
            break;
          }
          if (inflater.needsInput()) {
            throw new IOException("Unexpected end of gzip stream");
          }
          if (!nioOutput.hasRemaining()) {
            throw new IOException("Output buffer is too small for the decompressed gzip stream");
          }
        }
      }
    } catch (DataFormatException e) {
      throw new IOException("Invalid gzip stream", e);
    }

    int decompressedLength = nioOutput.position() - outputStart;
    verifyTrailer(nioInput, inputStart, inputLen, nioOutput, outputStart, decompressedLength);

    nioInput.position(inputStart + inputLen);
    return decompressedLength;
  }

  /**
   * {@link Inflater} runs in nowrap mode and never looks at the gzip header or trailer, so this is
   * the only place the CRC32 and ISIZE fields of the trailer are ever checked. Catches the case
   * where the raw DEFLATE payload decoded "successfully" (no {@link DataFormatException}) but
   * produced the wrong bytes or the wrong number of bytes.
   */
  private void verifyTrailer(ByteBuffer nioInput, int inputStart, int inputLen,
    ByteBuffer nioOutput, int outputStart, int decompressedLength) throws IOException {
    ByteBuffer trailer = nioInput.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    trailer.position(inputStart + inputLen - GZIP_TRAILER_LENGTH);
    int expectedCrc32 = trailer.getInt();
    int expectedISize = trailer.getInt();

    if (decompressedLength != expectedISize) {
      throw new IOException("Decompressed length " + decompressedLength
        + " does not match gzip trailer ISIZE " + expectedISize);
    }

    CRC32 crc32 = new CRC32();
    ByteBuffer writtenOutput = nioOutput.duplicate();
    writtenOutput.limit(nioOutput.position());
    writtenOutput.position(outputStart);
    crc32.update(writtenOutput);
    if ((int) crc32.getValue() != expectedCrc32) {
      throw new IOException(
        "Decompressed data's CRC32 does not match gzip trailer CRC32, " + "data is corrupt");
    }
  }

  @Override
  public void reinit(@Nullable Compression.HFileDecompressionContext newHFileDecompressionContext) {
    if (newHFileDecompressionContext == null) {
      return;
    }
    if (!(newHFileDecompressionContext instanceof GzipHFileDecompressionContext)) {
      throw new IllegalArgumentException(
        "GzipByteBuffDecompressor#reinit() was given an HFileDecompressionContext that was not "
          + "a GzipHFileDecompressionContext, this should never happen");
    }
    GzipHFileDecompressionContext gzipContext =
      (GzipHFileDecompressionContext) newHFileDecompressionContext;
    allowByteBuffDecompression = gzipContext.isAllowByteBuffDecompression();
  }

  @Override
  public void close() {
    inflater.end();
  }

}
