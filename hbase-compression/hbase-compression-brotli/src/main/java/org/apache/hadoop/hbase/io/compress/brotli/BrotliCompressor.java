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
package org.apache.hadoop.hbase.io.compress.brotli;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.encoder.Encoder;
import com.aayushatharva.brotli4j.encoder.Encoders;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.CanReinit;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop compressor glue for Brotli4j
 */
@InterfaceAudience.Private
public class BrotliCompressor implements CanReinit, Compressor {

  protected ByteBuffer inBuf, outBuf;
  protected int bufferSize;
  protected boolean finish, finished;
  protected long bytesRead, bytesWritten;
  protected Encoder.Parameters params;

  static {
    Brotli4jLoader.ensureAvailability();
  }

  BrotliCompressor(int level, int window, int bufferSize) {
    this.bufferSize = bufferSize;
    this.inBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf.position(bufferSize);
    params = new Encoder.Parameters();
    params.setQuality(level);
    params.setWindow(window);
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
        // If we don't have enough capacity in our currently allocated output buffer,
        // allocate a new one which does.
        int needed = maxCompressedLength(uncompressed);
        // Can we compress directly into the provided array?
        boolean direct = false;
        ByteBuffer writeBuf;
        if (len <= needed) {
          direct = true;
          writeBuf = ByteBuffer.wrap(b, off, len);
        } else {
          if (outBuf.capacity() < needed) {
            needed = CompressionUtil.roundInt2(needed);
            outBuf = ByteBuffer.allocate(needed);
          } else {
            outBuf.clear();
          }
          writeBuf = outBuf;
        }
        final int oldPos = writeBuf.position();
        Encoders.compress(inBuf, writeBuf, params);
        final int written = writeBuf.position() - oldPos;
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
      // Quality or window settings might have changed
      params.setQuality(BrotliCodec.getLevel(conf));
      params.setWindow(BrotliCodec.getWindow(conf));
      // Buffer size might have changed
      int newBufferSize = BrotliCodec.getBufferSize(conf);
      if (bufferSize != newBufferSize) {
        bufferSize = newBufferSize;
        this.inBuf = ByteBuffer.allocate(bufferSize);
        this.outBuf = ByteBuffer.allocate(bufferSize);
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
      // Get a new buffer that can accomodate the accumulated input plus the additional
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
    return len + CompressionUtil.compressionOverhead(len);
  }

}
