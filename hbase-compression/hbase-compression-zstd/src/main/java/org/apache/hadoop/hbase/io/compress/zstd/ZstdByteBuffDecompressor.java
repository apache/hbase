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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.BlockDecompressorHelper;
import org.apache.hadoop.hbase.io.compress.ByteBuffDecompressor;
import org.apache.hadoop.hbase.io.compress.CanReinit;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Glue for ByteBuffDecompressor on top of zstd-jni
 */
@InterfaceAudience.Private
public class ZstdByteBuffDecompressor implements ByteBuffDecompressor, CanReinit {

  protected int dictId;
  @Nullable
  protected ZstdDictDecompress dict;
  protected ZstdDecompressCtx ctx;
  // Intended to be set to false by some unit tests
  private boolean allowByteBuffDecompression;

  ZstdByteBuffDecompressor(@Nullable byte[] dictionary) {
    ctx = new ZstdDecompressCtx();
    if (dictionary != null) {
      this.dictId = ZstdCodec.getDictionaryId(dictionary);
      this.dict = new ZstdDictDecompress(dictionary);
      this.ctx.loadDict(this.dict);
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
  public void close() {
    ctx.close();
    if (dict != null) {
      dict.close();
    }
  }

  @Override
  public void reinit(Configuration conf) {
    if (conf != null) {
      // Dictionary may have changed
      byte[] b = ZstdCodec.getDictionary(conf);
      if (b != null) {
        // Don't casually create dictionary objects; they consume native memory
        int thisDictId = ZstdCodec.getDictionaryId(b);
        if (dict == null || dictId != thisDictId) {
          dictId = thisDictId;
          ZstdDictDecompress oldDict = dict;
          dict = new ZstdDictDecompress(b);
          ctx.loadDict(dict);
          if (oldDict != null) {
            oldDict.close();
          }
        }
      } else {
        ZstdDictDecompress oldDict = dict;
        dict = null;
        dictId = 0;
        // loadDict((byte[]) accepts null to clear the dictionary
        ctx.loadDict((byte[]) null);
        if (oldDict != null) {
          oldDict.close();
        }
      }

      // unit test helper
      this.allowByteBuffDecompression =
        conf.getBoolean("hbase.io.compress.zstd.allowByteBuffDecompression", true);
    }
  }
}
