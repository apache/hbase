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

package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is a stream that will only supply bytes from its delegate up to a certain limit.
 * When there is an attempt to set the position beyond that it will signal that the input
 * is finished.
 */
@InterfaceAudience.Private
public class BoundedDelegatingInputStream extends DelegatingInputStream {

  protected long limit;
  protected long pos;

  public BoundedDelegatingInputStream(InputStream in, long limit) {
    super(in);
    this.limit = limit;
    this.pos = 0;
  }

  public void setDelegate(InputStream in, long limit) {
    this.in = in;
    this.limit = limit;
    this.pos = 0;
  }

  /**
   * Call the delegate's {@code read()} method if the current position is less than the limit.
   * @return the byte read or -1 if the end of stream or the limit has been reached.
   */
  @Override
  public int read() throws IOException {
    if (pos >= limit) {
      return -1;
    }
    int result = in.read();
    pos++;
    return result;
  }

  /**
   * Call the delegate's {@code read(byte[], int, int)} method if the current position is less
   * than the limit.
   * @param b read buffer
   * @param off Start offset
   * @param len The number of bytes to read
   * @return the number of bytes read or -1 if the end of stream or the limit has been reached.
   */
  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (pos >= limit) {
      return -1;
    }
    long readLen = Math.min(len, limit - pos);
    int read = in.read(b, off, (int)readLen);
    if (read < 0) {
      return -1;
    }
    pos += read;
    return read;
  }

  /**
   * Call the delegate's {@code skip(long)} method.
   * @param len the number of bytes to skip
   * @return the actual number of bytes skipped
   */
  @Override
  public long skip(final long len) throws IOException {
    long skipped = in.skip(Math.min(len, limit - pos));
    pos += skipped;
    return skipped;
  }

  /**
   * @return the remaining bytes within the bound if the current position is less than the
   *   limit, or 0 otherwise.
   */
  @Override
  public int available() throws IOException {
    if (pos >= limit) {
      return 0;
    }
    // Do not call the delegate's available() method. Data in a bounded input stream is assumed
    // available up to the limit and that is the contract we have with our callers. Regardless
    // of what we do here, read() and skip() will behave as expected when EOF is encountered if
    // the underlying stream is closed early or otherwise could not provide enough bytes.
    // Note: This class is used to supply buffers to compression codecs during WAL tailing and
    // successful decompression depends on this behavior.
    return (int) (limit - pos);
  }

}
