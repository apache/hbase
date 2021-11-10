/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.compress.lz4;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.CanReinit;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * Hadoop compressor glue for lz4-java.
 */
@InterfaceAudience.Private
public class Lz4Compressor implements CanReinit, Compressor {

  protected static final Logger LOG = LoggerFactory.getLogger(Lz4Compressor.class);
  protected LZ4Compressor compressor;
  protected ByteBuffer inBuf, outBuf;
  protected int bufferSize;
  protected boolean finish, finished;
  protected long bytesRead, bytesWritten;

  Lz4Compressor(int bufferSize) {
    compressor = LZ4Factory.fastestInstance().fastCompressor();
    this.bufferSize = bufferSize;
    this.inBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf.position(bufferSize);
  }

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    // If we have previously compressed our input and still have some buffered bytes
    // remaining, provide them to the caller.
    if (outBuf.hasRemaining()) {
      int remaining = outBuf.remaining(), n = Math.min(remaining, len);
      outBuf.get(b, off, n);
      LOG.trace("compress: {} bytes from outBuf", n);
      return n;
    }
    // We don't actually begin compression until our caller calls finish().
    if (finish) {
      if (inBuf.position() > 0) {
        inBuf.flip();
        int uncompressed = inBuf.remaining();
        int needed = maxCompressedLength(uncompressed);
        // Can we decompress directly into the provided array?
        ByteBuffer writeBuffer;
        boolean direct = false;
        if (len <= needed) {
          writeBuffer = ByteBuffer.wrap(b, off, len);
          direct = true;
        } else {
          // If we don't have enough capacity in our currently allocated output buffer,
          // allocate a new one which does.
          if (outBuf.capacity() < needed) {
            needed = CompressionUtil.roundInt2(needed);
            LOG.trace("compress: resize outBuf {}", needed);
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
        LOG.trace("compress: compressed {} -> {}", uncompressed, written);
        finished = true;
        if (!direct) {
          outBuf.flip();
          int n = Math.min(written, len);
          outBuf.get(b, off, n);
          LOG.trace("compress: {} bytes", n);
          return n;
        } else {
          LOG.trace("compress: {} bytes direct", written);
          return written;
        }
      } else {
        finished = true;
      }
    }
    LOG.trace("No output");
    return 0;
  }

  @Override
  public void end() {
    LOG.trace("end");
  }

  @Override
  public void finish() {
    LOG.trace("finish");
    finish = true;
  }

  @Override
  public boolean finished() {
    boolean b = finished && !outBuf.hasRemaining();
    LOG.trace("finished: {}", b);
    return b;
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
    boolean b = !finished();
    LOG.trace("needsInput: {}", b);
    return b;
  }

  @Override
  public void reinit(Configuration conf) {
    LOG.trace("reinit");
    if (conf != null) {
      // Buffer size might have changed
      int newBufferSize = Lz4Codec.getBufferSize(conf);
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
    LOG.trace("reset");
    compressor = LZ4Factory.fastestInstance().fastCompressor();
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
    LOG.trace("setInput: off={} len={}", off, len);
    if (inBuf.remaining() < len) {
      // Get a new buffer that can accomodate the accumulated input plus the additional
      // input that would cause a buffer overflow without reallocation.
      // This condition should be fortunately rare, because it is expensive.
      int needed = CompressionUtil.roundInt2(inBuf.capacity() + len);
      LOG.trace("setInput: resize inBuf {}", needed);
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
