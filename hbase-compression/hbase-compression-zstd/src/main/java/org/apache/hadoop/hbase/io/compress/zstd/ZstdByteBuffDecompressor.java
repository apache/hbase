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

import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictDecompress;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.compress.BlockDecompressorHelper;
import org.apache.hadoop.hbase.io.compress.ByteBuffDecompressor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Glue for ByteBuffDecompressor on top of zstd-jni
 */
@InterfaceAudience.Private
public class ZstdByteBuffDecompressor implements ByteBuffDecompressor {

  protected int dictId;
  protected ZstdDecompressCtx ctx;
  // Intended to be set to false by some unit tests
  private boolean allowByteBuffDecompression;

  ZstdByteBuffDecompressor(@Nullable byte[] dictionaryBytes) {
    ctx = new ZstdDecompressCtx();
    if (dictionaryBytes != null) {
      this.ctx.loadDict(new ZstdDictDecompress(dictionaryBytes));
      dictId = ZstdCodec.getDictionaryId(dictionaryBytes);
    }
    allowByteBuffDecompression = true;
  }

  @Override
  public boolean canDecompress(ByteBuff output, ByteBuff input) {
    return allowByteBuffDecompression && output instanceof SingleByteBuff
      && input instanceof SingleByteBuff;
  }

  @Override
  public int decompress(ByteBuff output, ByteBuff input, int inputLen) throws IOException {
    return BlockDecompressorHelper.decompress(output, input, inputLen, this::decompressRaw);
  }

  private int decompressRaw(ByteBuff output, ByteBuff input, int inputLen) throws IOException {
    if (output instanceof SingleByteBuff && input instanceof SingleByteBuff) {
      ByteBuffer nioOutput = output.nioByteBuffers()[0];
      ByteBuffer nioInput = input.nioByteBuffers()[0];
      int origOutputPos = nioOutput.position();
      int n;
      if (nioOutput.isDirect() && nioInput.isDirect()) {
        n = ctx.decompressDirectByteBuffer(nioOutput, nioOutput.position(),
          nioOutput.limit() - nioOutput.position(), nioInput, nioInput.position(), inputLen);
      } else if (!nioOutput.isDirect() && !nioInput.isDirect()) {
        n = ctx.decompressByteArray(nioOutput.array(),
          nioOutput.arrayOffset() + nioOutput.position(), nioOutput.limit() - nioOutput.position(),
          nioInput.array(), nioInput.arrayOffset() + nioInput.position(), inputLen);
      } else if (nioOutput.isDirect() && !nioInput.isDirect()) {
        n = ctx.decompressByteArrayToDirectByteBuffer(nioOutput, nioOutput.position(),
          nioOutput.limit() - nioOutput.position(), nioInput.array(),
          nioInput.arrayOffset() + nioInput.position(), inputLen);
      } else if (!nioOutput.isDirect() && nioInput.isDirect()) {
        n = ctx.decompressDirectByteBufferToByteArray(nioOutput.array(),
          nioOutput.arrayOffset() + nioOutput.position(), nioOutput.limit() - nioOutput.position(),
          nioInput, nioInput.position(), inputLen);
      } else {
        throw new IllegalStateException("Unreachable line");
      }

      nioOutput.position(origOutputPos + n);
      nioInput.position(input.position() + inputLen);

      return n;
    } else {
      throw new IllegalStateException(
        "At least one buffer is not a SingleByteBuff, this is not supported");
    }
  }

  @Override
  public void reinit(@Nullable Compression.HFileDecompressionContext newHFileDecompressionContext) {
    if (newHFileDecompressionContext != null) {
      if (newHFileDecompressionContext instanceof ZstdHFileDecompressionContext) {
        ZstdHFileDecompressionContext zstdContext =
          (ZstdHFileDecompressionContext) newHFileDecompressionContext;
        allowByteBuffDecompression = zstdContext.isAllowByteBuffDecompression();
        if (zstdContext.getDict() == null && dictId != 0) {
          ctx.loadDict((byte[]) null);
          dictId = 0;
        } else if (zstdContext.getDictId() != dictId) {
          this.ctx.loadDict(zstdContext.getDict());
          this.dictId = zstdContext.getDictId();
        }
      } else {
        throw new IllegalArgumentException(
          "ZstdByteBuffDecompression#reinit() was given an HFileDecompressionContext that was not "
            + "a ZstdHFileDecompressionContext, this should never happen");
      }
    }
  }

  @Override
  public void close() {
    ctx.close();
  }

}
