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
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Glue for ByteBuffDecompressor on top of Hadoop's native
 * {@link ZlibDecompressor.ZlibDirectDecompressor}. Only direct-to-direct decompression is
 * supported, which is zero-copy; callers with on-heap buffers fall back to the stream path.
 */
@InterfaceAudience.Private
public class GzipByteBuffDecompressor implements ByteBuffDecompressor {

  private static final int GZIP_HEADER_LENGTH = 10;
  private static final int GZIP_TRAILER_LENGTH = 8;

  @Nullable
  private final ZlibDecompressor.ZlibDirectDecompressor decompressor;

  private boolean allowByteBuffDecompression;

  GzipByteBuffDecompressor(boolean nativeZlibLoaded) {
    decompressor = nativeZlibLoaded
      ? new ZlibDecompressor.ZlibDirectDecompressor(ZlibDecompressor.CompressionHeader.GZIP_FORMAT,
        0)
      : null;
    allowByteBuffDecompression = true;
  }

  @Override
  public boolean canDecompress(ByteBuff output, ByteBuff input) {
    if (!allowByteBuffDecompression) {
      return false;
    }
    if (!(output instanceof SingleByteBuff) || !(input instanceof SingleByteBuff)) {
      return false;
    }
    // Only direct-to-direct decompression is supported.
    return input.nioByteBuffers()[0].isDirect() && output.nioByteBuffers()[0].isDirect()
      && decompressor != null;
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
    ByteBuffer nioOutput = output.nioByteBuffers()[0];
    if (!nioInput.isDirect() || !nioOutput.isDirect() || decompressor == null) {
      throw new IllegalStateException(
        "GzipByteBuffDecompressor only supports direct-to-direct decompression with native zlib "
          + "loaded, this should never happen since canDecompress() would have returned false");
    }
    return decompressOffHeap(nioInput, nioOutput, inputLen);
  }

  private int decompressOffHeap(ByteBuffer nioInput, ByteBuffer nioOutput, int inputLen)
    throws IOException {
    int inputStart = nioInput.position();
    int outputStart = nioOutput.position();

    ByteBuffer gzipMember = nioInput.duplicate();
    gzipMember.limit(inputStart + inputLen);

    decompressor.reset();
    try {
      decompressor.decompress(gzipMember, nioOutput);
    } catch (IOException e) {
      throw new IOException("Invalid gzip stream: " + e.getMessage(), e);
    }
    if (!decompressor.finished()) {
      if (!nioOutput.hasRemaining()) {
        throw new IOException("Output buffer is too small for the decompressed gzip stream");
      }
      throw new IOException("Unexpected end of gzip stream");
    }
    if (gzipMember.hasRemaining()) {
      throw new IOException("Unexpected trailing bytes after decompressing gzip stream");
    }

    nioInput.position(inputStart + inputLen);

    return nioOutput.position() - outputStart;
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
    if (decompressor != null) {
      decompressor.end();
    }
  }

}
