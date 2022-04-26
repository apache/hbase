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
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tukaani.xz.ArrayCache;
import org.tukaani.xz.BasicArrayCache;
import org.tukaani.xz.LZMAInputStream;

/**
 * Hadoop decompressor glue for XZ for Java.
 */
@InterfaceAudience.Private
public class LzmaDecompressor implements Decompressor {

  protected static final Logger LOG = LoggerFactory.getLogger(LzmaDecompressor.class);
  protected static final ArrayCache ARRAY_CACHE = new BasicArrayCache() {
    @Override
    public byte[] getByteArray(int size, boolean fillWithZeros) {
      // Work around a bug in XZ decompression if cached byte arrays are not cleared by
      // always clearing them.
      return super.getByteArray(size, true);
    }
  };
  protected ByteBuffer inBuf, outBuf;
  protected int inLen;
  protected boolean finished;

  LzmaDecompressor(int bufferSize) {
    this.inBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf.position(bufferSize);
  }

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    if (outBuf.hasRemaining()) {
      int remaining = outBuf.remaining(), n = Math.min(remaining, len);
      outBuf.get(b, off, n);
      LOG.trace("decompress: read {} remaining bytes from outBuf", n);
      return n;
    }
    if (inBuf.position() > 0) {
      inBuf.flip();
      int remaining = inBuf.remaining();
      inLen -= remaining;
      // This is pretty ugly. I don't see how to do it better. Stream to byte buffers back to
      // stream back to byte buffers... if only XZ for Java had a public block compression API.
      // It does not. LZMA decompression speed is reasonably good, so inefficiency here is a
      // shame.
      // Perhaps we could look at using reflection to make package protected classes for block
      // compression in XZ for Java accessible here, that library can be expected to rarely
      // change, if at all.
      outBuf.clear();
      try (ByteBufferInputStream lowerIn = new ByteBufferInputStream(inBuf)) {
        final byte[] buf = new byte[8192];
        try (LZMAInputStream in = new LZMAInputStream(lowerIn, ARRAY_CACHE)) {
          int read;
          do {
            read = in.read(buf);
            if (read > 0) {
              outBuf.put(buf, 0, read);
            }
          } while (read > 0);
        }
      }
      int written = outBuf.position();
      outBuf.flip();
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
      ByteBuffer newBuf = ByteBuffer.allocate(needed);
      inBuf.flip();
      newBuf.put(inBuf);
      inBuf = newBuf;
    }
    inBuf.put(b, off, len);
    inLen += len;
    finished = false;
  }

}
