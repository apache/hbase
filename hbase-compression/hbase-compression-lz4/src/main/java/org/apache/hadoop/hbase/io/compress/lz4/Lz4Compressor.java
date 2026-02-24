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
package org.apache.hadoop.hbase.io.compress.lz4;

import java.io.IOException;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.CanReinit;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop compressor glue for lz4-java.
 */
@InterfaceAudience.Private
public class Lz4Compressor implements CanReinit, Compressor {

  protected LZ4Compressor compressor;
  protected ByteBuffer inBuf, outBuf;
  protected int bufferSize;
  protected boolean finish, finished;
  protected long bytesRead, bytesWritten;

  private final boolean useNative = Lz4Native.available();

  Lz4Compressor(int bufferSize) {
    compressor = LZ4Factory.fastestInstance().fastCompressor();
    this.bufferSize = bufferSize;
    this.inBuf = ByteBuffer.allocateDirect(bufferSize);
    this.outBuf = ByteBuffer.allocateDirect(bufferSize);
    this.outBuf.position(bufferSize);
  }

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
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

        if (useNative && inBuf.isDirect() && outBuf.isDirect()) {
          // 如果你现在用的是 heap ByteBuffer，改成 direct 分配（见 reset/reinit 部分）
          int needed = Lz4Native.maxCompressedLength(uncompressed);
          if (outBuf.capacity() < needed) {
            needed = org.apache.hadoop.hbase.io.compress.CompressionUtil.roundInt2(needed);
            outBuf = ByteBuffer.allocateDirect(needed);
          } else {
            outBuf.clear();
          }
          int written = Lz4Native.compressDirect(inBuf, inBuf.position(), uncompressed,
              outBuf, outBuf.position(), outBuf.remaining());
          if (written < 0)
            throw new IOException("LZ4 native compress failed: " + written);
          bytesWritten += written;
          inBuf.clear();
          finished = true;
          outBuf.limit(outBuf.position() + written);
          int n = Math.min(written, len);
          outBuf.get(b, off, n);
          return n;
        } else {
          // 走原 lz4-java 路径
          int needed = maxCompressedLength(uncompressed);
          ByteBuffer writeBuffer;
          boolean direct = false;
          if (len <= needed) {
            writeBuffer = ByteBuffer.wrap(b, off, len);
            direct = true;
          } else {
            if (outBuf.capacity() < needed) {
              needed = org.apache.hadoop.hbase.io.compress.CompressionUtil.roundInt2(needed);
              outBuf = ByteBuffer.allocate(needed);
            } else {
              outBuf.clear();
            }
            writeBuffer = outBuf;
          }
          final int oldPos = writeBuffer.position();
          compressor.compress(inBuf, writeBuffer);
          final int written = writeBuffer.position() - oldPos;
          bytesWritten += written;
          inBuf.clear();
          finished = true;
          if (!direct) {
            outBuf.flip();
            int n = Math.min(written, len);
            outBuf.get(b, off, n);
            return n;
          } else {
            return written;
          }
        }
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
  public void reinit(Configuration conf) {
    if (conf != null) {
      // Buffer size might have changed
      int newBufferSize = Lz4Codec.getBufferSize(conf);
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
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException("setDictionary is not supported");
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    if (inBuf.remaining() < len) {
      // Get a new buffer that can accomodate the accumulated input plus the
      // additional
      // input that would cause a buffer overflow without reallocation.
      // This condition should be fortunately rare, because it is expensive.
      int needed = CompressionUtil.roundInt2(inBuf.capacity() + len);
      ByteBuffer newBuf = ByteBuffer.allocate(needed);
      inBuf.flip();
      newBuf.put(inBuf);
      inBuf = newBuf;
    }
    inBuf.put(b, off, len);
    bytesRead += len;
    finished = false;
  }

  // Package private

  int maxCompressedLength(int len) {
    return compressor.maxCompressedLength(len);
  }

}
