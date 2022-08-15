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
package org.apache.hadoop.hbase.io.compress.aircompressor;

import io.airlift.compress.Decompressor;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop decompressor glue for aircompressor decompressors.
 */
@InterfaceAudience.Private
public class HadoopDecompressor<T extends Decompressor>
  implements org.apache.hadoop.io.compress.Decompressor {

  protected T decompressor;
  protected ByteBuffer inBuf, outBuf;
  protected int inLen;
  protected boolean finished;

  HadoopDecompressor(T decompressor, int bufferSize) {
    this.decompressor = decompressor;
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
      decompressor.decompress(inBuf, outBuf);
      inBuf.rewind();
      inBuf.limit(inBuf.capacity());
      final int written = outBuf.position();
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
    inBuf.rewind();
    inBuf.limit(inBuf.capacity());
    inLen = 0;
    outBuf.rewind();
    outBuf.limit(0);
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
