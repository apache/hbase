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
package org.apache.hadoop.hbase.io.compress.xz;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.yetus.audience.InterfaceAudience;
import org.tukaani.xz.ArrayCache;
import org.tukaani.xz.BasicArrayCache;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.LZMAOutputStream;
import org.tukaani.xz.UnsupportedOptionsException;

/**
 * Hadoop compressor glue for XZ for Java.
 */
@InterfaceAudience.Private
public class LzmaCompressor implements Compressor {

  protected static final ArrayCache ARRAY_CACHE = new BasicArrayCache();
  protected ByteBuffer inBuf;
  protected ByteBuffer outBuf;
  protected int bufferSize;
  protected boolean finish, finished;
  protected long bytesRead, bytesWritten;
  protected LZMA2Options lzOptions;

  LzmaCompressor(int level, int bufferSize) {
    this.bufferSize = bufferSize;
    this.inBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf.position(bufferSize);
    this.lzOptions = new LZMA2Options();
    try {
      this.lzOptions.setPreset(level);
    } catch (UnsupportedOptionsException e) {
      throw new RuntimeException(e);
    }
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
        // Can we decompress directly into the provided array?
        ByteBuffer writeBuffer;
        boolean direct = false;
        if (len <= needed) {
          writeBuffer = ByteBuffer.wrap(b, off, len);
          direct = true;
        } else {
          if (outBuf.capacity() < needed) {
            needed = CompressionUtil.roundInt2(needed);
            outBuf = ByteBuffer.allocate(needed);
          } else {
            outBuf.clear();
          }
          writeBuffer = outBuf;
        }
        int oldPos = writeBuffer.position();
        // This is pretty ugly. I don't see how to do it better. Stream to byte buffers back to
        // stream back to byte buffers... if only XZ for Java had a public block compression
        // API. It does not. Fortunately the algorithm is so slow, especially at higher levels,
        // that inefficiencies here may not matter.
        try (ByteBufferOutputStream lowerOut = new ByteBufferOutputStream(writeBuffer) {
          @Override
          // ByteBufferOutputStream will reallocate the output buffer if it is too small. We
          // do not want that behavior here.
          protected void checkSizeAndGrow(int extra) {
            long capacityNeeded = curBuf.position() + (long) extra;
            if (capacityNeeded > curBuf.limit()) {
              throw new BufferOverflowException();
            }
          }
        }) {
          try (LZMAOutputStream out =
            new LZMAOutputStream(lowerOut, lzOptions, uncompressed, ARRAY_CACHE)) {
            out.write(inBuf.array(), inBuf.arrayOffset(), uncompressed);
          }
        }
        int written = writeBuffer.position() - oldPos;
        bytesWritten += written;
        inBuf.clear();
        finished = true;
        outBuf.flip();
        if (!direct) {
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
      // Level might have changed
      try {
        int level = LzmaCodec.getLevel(conf);
        this.lzOptions = new LZMA2Options();
        this.lzOptions.setPreset(level);
      } catch (UnsupportedOptionsException e) {
        throw new RuntimeException(e);
      }
      // Buffer size might have changed
      int newBufferSize = LzmaCodec.getBufferSize(conf);
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
