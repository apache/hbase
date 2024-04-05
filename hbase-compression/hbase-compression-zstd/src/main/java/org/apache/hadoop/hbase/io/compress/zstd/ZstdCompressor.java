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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.CanReinit;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop compressor glue for zstd-jni.
 */
@InterfaceAudience.Private
public class ZstdCompressor implements CanReinit, Compressor {

  protected int level, bufferSize;
  protected ByteBuffer inBuf, outBuf;
  protected boolean finish, finished;
  protected long bytesRead, bytesWritten;
  protected int dictId;
  protected ZstdDictCompress dict;
  protected ZstdCompressCtx ctx;

  ZstdCompressor(final int level, final int bufferSize, final byte[] dictionary) {
    this.level = level;
    this.bufferSize = bufferSize;
    this.inBuf = ByteBuffer.allocateDirect(bufferSize);
    this.outBuf = ByteBuffer.allocateDirect(bufferSize);
    this.outBuf.position(bufferSize);
    this.ctx = new ZstdCompressCtx();
    this.ctx.setLevel(level);
    if (dictionary != null) {
      this.dictId = ZstdCodec.getDictionaryId(dictionary);
      this.dict = new ZstdDictCompress(dictionary, level);
    }
    this.ctx.loadDict(this.dict);
  }

  ZstdCompressor(final int level, final int bufferSize) {
    this(level, bufferSize, null);
  }

  @Override
  public int compress(final byte[] b, final int off, final int len) throws IOException {
    // If we have previously compressed our input and still have some buffered bytes
    // remaining, provide them to the caller.
    if (outBuf.hasRemaining()) {
      int remaining = outBuf.remaining(), n = Math.min(remaining, len);
      outBuf.get(b, off, n);
      return n;
    }
    // We don't actually begin compression until our caller calls finish().
    if (finish) {
      if (inBuf.position() > 0) {
        inBuf.flip();
        int uncompressed = inBuf.remaining();
        // If we don't have enough capacity in our currently allocated output buffer,
        // allocate a new one which does.
        int needed = maxCompressedLength(uncompressed);
        if (outBuf.capacity() < needed) {
          needed = CompressionUtil.roundInt2(needed);
          outBuf = ByteBuffer.allocateDirect(needed);
        } else {
          outBuf.clear();
        }
        int written = ctx.compress(outBuf, inBuf);
        bytesWritten += written;
        inBuf.clear();
        finished = true;
        outBuf.flip();
        int n = Math.min(written, len);
        outBuf.get(b, off, n);
        return n;
      } else {
        finished = true;
      }
    }
    return 0;
  }

  @Override
  public void end() {
  }

  @Override
  public void finish() {
    finish = true;
  }

  @Override
  public boolean finished() {
    return finished && !outBuf.hasRemaining();
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public boolean needsInput() {
    return !finished();
  }

  @Override
  public void reinit(final Configuration conf) {
    if (conf != null) {
      // Level might have changed
      boolean levelChanged = false;
      int newLevel = ZstdCodec.getLevel(conf);
      if (level != newLevel) {
        level = newLevel;
        levelChanged = true;
      }
      // Dictionary may have changed
      byte[] b = ZstdCodec.getDictionary(conf);
      if (b != null) {
        // Don't casually create dictionary objects; they consume native memory
        int thisDictId = ZstdCodec.getDictionaryId(b);
        if (dict == null || dictId != thisDictId || levelChanged) {
          dictId = thisDictId;
          dict = new ZstdDictCompress(b, level);
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

  @Override
  public void reset() {
    inBuf.clear();
    outBuf.clear();
    outBuf.position(outBuf.capacity());
    bytesRead = 0;
    bytesWritten = 0;
    finish = false;
    finished = false;
    ctx.setLevel(level);
    // loadDict() accepts null to clear the dictionary
    ctx.loadDict(dict);
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
      int needed = CompressionUtil.roundInt2(inBuf.capacity() + len);
      ByteBuffer newBuf = ByteBuffer.allocateDirect(needed);
      inBuf.flip();
      newBuf.put(inBuf);
      inBuf = newBuf;
    }
    inBuf.put(b, off, len);
    bytesRead += len;
    finished = false;
  }

  // Package private

  static int maxCompressedLength(final int len) {
    return (int) Zstd.compressBound(len);
  }

}
