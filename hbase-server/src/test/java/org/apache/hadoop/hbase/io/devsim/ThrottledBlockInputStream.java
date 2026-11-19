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
package org.apache.hadoop.hbase.io.devsim;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps a block data {@link InputStream} returned by the DataNode's {@code getBlockInputStream}
 * with per-byte BW and IOPS throttling against the volume's {@link EBSVolumeDevice}. Tracks the
 * read position for sequential-IO coalescing detection.
 * <p>
 * Also charges the per-DataNode instance-level BW budget if configured.
 */
public class ThrottledBlockInputStream extends InputStream {

  private final InputStream delegate;
  private final EBSVolumeDevice volume;
  private final EBSDevice.DataNodeContext dnContext;
  private long position;

  /**
   * @param delegate  the real block input stream from FsDatasetImpl
   * @param volume    the EBS volume device this block resides on
   * @param dnContext the DataNode context for instance-level BW budget (may be null)
   * @param offset    the initial seek offset into the block
   */
  public ThrottledBlockInputStream(InputStream delegate, EBSVolumeDevice volume,
    EBSDevice.DataNodeContext dnContext, long offset) {
    this.delegate = delegate;
    this.volume = volume;
    this.dnContext = dnContext;
    this.position = offset;
  }

  @Override
  public int read() throws IOException {
    int b = delegate.read();
    if (b >= 0) {
      accountRead(1);
    }
    return b;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    int bytesRead = delegate.read(buf, off, len);
    if (bytesRead > 0) {
      accountRead(bytesRead);
    }
    return bytesRead;
  }

  @Override
  public long skip(long n) throws IOException {
    long skipped = delegate.skip(n);
    if (skipped > 0) {
      volume.flushPendingReadIops(this);
      position += skipped;
    }
    return skipped;
  }

  @Override
  public int available() throws IOException {
    return delegate.available();
  }

  @Override
  public void close() throws IOException {
    volume.flushPendingReadIops(this);
    delegate.close();
  }

  private void accountRead(int bytes) {
    volume.accountRead(this, position, bytes);
    position += bytes;
    if (dnContext != null) {
      dnContext.consumeInstanceBw(bytes);
    }
  }
}
