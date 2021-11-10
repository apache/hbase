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
package org.apache.hadoop.hbase.io.compress.xerial;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Hadoop decompressor glue for Xerial Snappy.
 */
@InterfaceAudience.Private
public class SnappyDecompressor implements Decompressor {

  protected static final Logger LOG = LoggerFactory.getLogger(SnappyDecompressor.class);
  protected ByteBuffer inBuf, outBuf;
  protected int inLen;
  protected boolean finished;

  SnappyDecompressor(int bufferSize) {
    this.inBuf = ByteBuffer.allocateDirect(bufferSize);
    this.outBuf = ByteBuffer.allocateDirect(bufferSize);
    this.outBuf.position(bufferSize);
  }

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    if (outBuf.hasRemaining()) {
      int remaining = outBuf.remaining(), n = Math.min(remaining, len);
      outBuf.get(b, off, n);
      LOG.trace("decompress: {} bytes from outBuf", n);
      return n;
    }
    if (inBuf.position() > 0) {
      inBuf.flip();
      int remaining = inBuf.remaining();
      inLen -= remaining;
      outBuf.clear();
      int written = Snappy.uncompress(inBuf, outBuf);
      inBuf.clear();
      LOG.trace("decompress: decompressed {} -> {}", remaining, written);
      int n = Math.min(written, len);
      outBuf.get(b, off, n);
      LOG.trace("decompress: {} bytes", n);
      return n;
    }
    LOG.trace("decompress: No output, finished");
    finished = true;
    return 0;
  }

  @Override
  public void end() {
    LOG.trace("end");
  }

  @Override
  public boolean finished() {
    LOG.trace("finished");
    return finished;
  }

  @Override
  public int getRemaining() {
    LOG.trace("getRemaining: {}", inLen);
    return inLen;
  }

  @Override
  public boolean needsDictionary() {
    LOG.trace("needsDictionary");
    return false;
  }

  @Override
  public void reset() {
    LOG.trace("reset");
    inBuf.clear();
    inLen = 0;
    outBuf.clear();
    outBuf.position(outBuf.capacity());
    finished = false;
  }

  @Override
  public boolean needsInput() {
    boolean b = (inBuf.position() == 0);
    LOG.trace("needsInput: {}", b);
    return b;
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
      ByteBuffer newBuf = ByteBuffer.allocateDirect(needed);
      inBuf.flip();
      newBuf.put(inBuf);
      inBuf = newBuf;
    }
    inBuf.put(b, off, len);
    inLen += len;
    finished = false;
  }

}
