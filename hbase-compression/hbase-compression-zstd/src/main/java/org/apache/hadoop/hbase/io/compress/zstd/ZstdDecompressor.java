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
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.CanReinit;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop decompressor glue for zstd-java.
 */
@InterfaceAudience.Private
public class ZstdDecompressor implements CanReinit, Decompressor {

  protected ByteBuffer inBuf, outBuf;
  protected int bufferSize;
  protected int inLen;
  protected boolean finished;
  protected int dictId;
  protected ZstdDictDecompress dict;
  protected ZstdDecompressCtx ctx;

  ZstdDecompressor(final int bufferSize, final byte[] dictionary) {
    this.bufferSize = bufferSize;
    this.inBuf = ByteBuffer.allocateDirect(bufferSize);
    this.outBuf = ByteBuffer.allocateDirect(bufferSize);
    this.outBuf.position(bufferSize);
    this.ctx = new ZstdDecompressCtx();
    if (dictionary != null) {
      this.dictId = ZstdCodec.getDictionaryId(dictionary);
      this.dict = new ZstdDictDecompress(dictionary);
      this.ctx.loadDict(this.dict);
    }
  }

  ZstdDecompressor(final int bufferSize) {
    this(bufferSize, null);
  }

  @Override
  public int decompress(final byte[] b, final int off, final int len) throws IOException {
    if (outBuf.hasRemaining()) {
      int remaining = outBuf.remaining(), n = Math.min(remaining, len);
      outBuf.get(b, off, n);
      return n;
    }
    if (inBuf.position() > 0) {
      inBuf.flip();
      int remaining = inBuf.remaining();
      inLen -= remaining;
      outBuf.clear();
      int written = ctx.decompress(outBuf, inBuf);
      inBuf.clear();
      outBuf.flip();
      int n = Math.min(written, len);
      outBuf.get(b, off, n);
      return n;
    }
    finished = true;
    return 0;
  }

  @Override
  public void end() {
  }

  @Override
  public boolean finished() {
    return finished;
  }

  @Override
  public int getRemaining() {
    return inLen;
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public void reset() {
    inBuf.clear();
    inLen = 0;
    outBuf.clear();
    outBuf.position(outBuf.capacity());
    finished = false;
    ctx.reset();
    if (dict != null) {
      ctx.loadDict(dict);
    } else {
      // loadDict((byte[]) accepts null to clear the dictionary
      ctx.loadDict((byte[]) null);
    }
  }

  @Override
  public boolean needsInput() {
    return (inBuf.position() == 0);
  }

  @Override
  public void setDictionary(final byte[] b, final int off, final int len) {
    throw new UnsupportedOperationException("setDictionary is not supported");
  }

  @Override
  public void setInput(final byte[] b, final int off, final int len) {
    if (inBuf.remaining() < len) {
      // Get a new buffer that can accomodate the accumulated input plus the additional
      // input that would cause a buffer overflow without reallocation.
      // This condition should be fortunately rare, because it is expensive.
      final int needed = CompressionUtil.roundInt2(inBuf.capacity() + len);
      ByteBuffer newBuf = ByteBuffer.allocateDirect(needed);
      inBuf.flip();
      newBuf.put(inBuf);
      inBuf = newBuf;
    }
    inBuf.put(b, off, len);
    inLen += len;
    finished = false;
  }

  @Override
  public void reinit(final Configuration conf) {
    if (conf != null) {
      // Dictionary may have changed
      byte[] b = ZstdCodec.getDictionary(conf);
      if (b != null) {
        // Don't casually create dictionary objects; they consume native memory
        int thisDictId = ZstdCodec.getDictionaryId(b);
        if (dict == null || dictId != thisDictId) {
          dictId = thisDictId;
          dict = new ZstdDictDecompress(b);
        }
      } else {
        dict = null;
      }
      // Buffer size might have changed
      int newBufferSize = ZstdCodec.getBufferSize(conf);
      if (bufferSize != newBufferSize) {
        bufferSize = newBufferSize;
        this.inBuf = ByteBuffer.allocateDirect(bufferSize);
        this.outBuf = ByteBuffer.allocateDirect(bufferSize);
      }
    }
    reset();
  }

}
