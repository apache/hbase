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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is only used by WAL ValueCompressor for decompression.
 * <p>
 * <strong>WARNING: </strong>The implementation is very tricky and does not follow typical
 * InputStream pattern, so do not use it in any other places.
 */
@InterfaceAudience.Private
class WALDecompressionBoundedDelegatingInputStream extends InputStream {

  private static final Logger LOG =
    LoggerFactory.getLogger(WALDecompressionBoundedDelegatingInputStream.class);

  private InputStream in;

  private long pos;

  private long limit;

  public void reset(InputStream in, long limit) {
    this.in = in;
    this.limit = limit;
    this.pos = 0;
  }

  @Override
  public int read() throws IOException {
    if (pos >= limit) {
      return -1;
    }
    int result = in.read();
    if (result < 0) {
      return -1;
    }
    pos++;
    return result;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (pos >= limit) {
      return -1;
    }
    int readLen = (int) Math.min(len, limit - pos);
    try {
      IOUtils.readFully(in, b, off, readLen);
    } catch (EOFException e) {
      // This is trick here, we will always try to read enough bytes to fill the buffer passed in,
      // or we reach the end of this compression block, if there are not enough bytes, we just
      // return -1 to let the upper layer fail with EOF
      // In WAL value decompression this is OK as if we can not read all the data, we will finally
      // get an EOF somewhere
      LOG.debug("Got EOF while we want to read {} bytes from stream", readLen, e);
      return -1;
    }
    return readLen;
  }

  @Override
  public long skip(final long len) throws IOException {
    long skipped = in.skip(Math.min(len, limit - pos));
    pos += skipped;
    return skipped;
  }

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
