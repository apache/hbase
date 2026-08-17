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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.compress.BlockDecompressorHelper;
import org.apache.hadoop.hbase.io.compress.ByteBuffDecompressor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.yetus.audience.InterfaceAudience;
import org.xerial.snappy.Snappy;

@InterfaceAudience.Private
public class SnappyByteBuffDecompressor implements ByteBuffDecompressor {

  private boolean allowByteBuffDecompression;

  SnappyByteBuffDecompressor() {
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
    ByteBuffer nioInput = input.nioByteBuffers()[0];
    ByteBuffer nioOutput = output.nioByteBuffers()[0];
    // Xerial Snappy's ByteBuffer API only supports direct-to-direct; the byte array API handles
    // heap-to-heap. Mixed direct/heap is not supported.
    return (nioInput.isDirect() && nioOutput.isDirect())
      || (nioInput.hasArray() && nioOutput.hasArray());
  }

  @Override
  public int decompress(ByteBuff output, ByteBuff input, int inputLen) throws IOException {
    return BlockDecompressorHelper.decompress(output, input, inputLen, this::decompressRaw);
  }

  private int decompressRaw(ByteBuff output, ByteBuff input, int inputLen) throws IOException {
    if (!(output instanceof SingleByteBuff) || !(input instanceof SingleByteBuff)) {
      throw new IllegalStateException(
        "At least one buffer is not a SingleByteBuff, this is not supported");
    }
    ByteBuffer nioOutput = output.nioByteBuffers()[0];
    ByteBuffer nioInput = input.nioByteBuffers()[0];
    int origOutputPos = nioOutput.position();
    int origInputPos = nioInput.position();
    int bytesDecompressed;
    if (nioInput.isDirect() && nioOutput.isDirect()) {
      bytesDecompressed = decompressRawDirect(nioInput, nioOutput, inputLen);
    } else if (nioInput.hasArray() && nioOutput.hasArray()) {
      bytesDecompressed = decompressRawHeap(nioInput, nioOutput, inputLen);
    } else {
      throw new IllegalStateException(
        "SnappyByteBuffDecompressor only supports direct-to-direct or heap-to-heap decompression,"
          + " this should never happen since canDecompress() would have returned false");
    }
    nioOutput.position(origOutputPos + bytesDecompressed);
    nioInput.position(origInputPos + inputLen);
    return bytesDecompressed;
  }
  

  private int decompressRawDirect(ByteBuffer nioInput, ByteBuffer nioOutput, int inputLen)
    throws IOException {
    if (!nioInput.isDirect() || !nioOutput.isDirect()) {
      throw new IllegalStateException(
        "decompressRawDirect called with non-direct buffers, this should never happen");
    }
    int savedInputLimit = nioInput.limit();
    nioInput.limit(nioInput.position() + inputLen);
    try {
      return Snappy.uncompress(nioInput, nioOutput);
    } catch (IOException e) {
      throw new IOException("Snappy decompression failed: " + e.getMessage(), e);
    } finally {
      nioInput.limit(savedInputLimit);
    }
  }

  private int decompressRawHeap(ByteBuffer nioInput, ByteBuffer nioOutput, int inputLen)
    throws IOException {
    if (!nioInput.hasArray() || !nioOutput.hasArray()) {
      throw new IllegalStateException(
        "decompressRawHeap called with non-heap buffers, this should never happen");
    }
    try {
      return Snappy.uncompress(nioInput.array(), nioInput.arrayOffset() + nioInput.position(),
        inputLen, nioOutput.array(), nioOutput.arrayOffset() + nioOutput.position());
    } catch (IOException e) {
      throw new IOException("Snappy decompression failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void reinit(@Nullable Compression.HFileDecompressionContext newHFileDecompressionContext) {
    if (newHFileDecompressionContext == null) {
      return;
    }
    if (!(newHFileDecompressionContext instanceof SnappyHFileDecompressionContext)) {
      throw new IllegalArgumentException(
        "SnappyByteBuffDecompressor#reinit() was given an HFileDecompressionContext that was not "
          + "a SnappyHFileDecompressionContext, this should never happen");
    }
    SnappyHFileDecompressionContext ctx =
      (SnappyHFileDecompressionContext) newHFileDecompressionContext;
    allowByteBuffDecompression = ctx.isAllowByteBuffDecompression();
  }

  @Override
  public void close() {
  }
}
