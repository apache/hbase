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
import com.aayushatharva.brotli4j.decoder.Decoder;
import com.aayushatharva.brotli4j.decoder.DirectDecompress;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop decompressor glue for Brotli4j
 */
@InterfaceAudience.Private
public class BrotliDecompressor implements Decompressor {

  protected ByteBuffer inBuf, outBuf;
  protected int inLen;
  protected boolean finished;

  static {
    Brotli4jLoader.ensureAvailability();
  }

  BrotliDecompressor(int bufferSize) {
    this.inBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf = ByteBuffer.allocate(bufferSize);
    this.outBuf.position(bufferSize);
  }

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    if (outBuf.hasRemaining()) {
      int remaining = outBuf.remaining(), n = Math.min(remaining, len);
      outBuf.get(b, off, n);
      return n;
    }
    if (inBuf.position() > 0) {
      inBuf.flip();
      int remaining = inBuf.remaining();
      inLen -= remaining;
      outBuf.rewind();
      outBuf.limit(outBuf.capacity());
      // TODO: More inefficient than it could be, but it doesn't impact decompression speed
      // terribly and the brotli4j API alternatives do not seem to work correctly.
      // Maybe something more clever can be done as a future improvement.
      final byte[] inb = new byte[remaining];
      inBuf.get(inb);
      DirectDecompress result = Decoder.decompress(inb);
      outBuf.put(result.getDecompressedDataByteBuf().nioBuffer());
      final int written = outBuf.position();
      inBuf.rewind();
      inBuf.limit(inBuf.capacity());
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
  }

  @Override
  public boolean needsInput() {
    return inBuf.position() == 0;
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
    inLen += len;
    finished = false;
  }

}
