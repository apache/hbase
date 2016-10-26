/**
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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;

/**
 * An OutputStream which writes data into ByteBuffers. It will try to get ByteBuffer, as and when
 * needed, from the passed pool. When pool is not giving a ByteBuffer it will create one on heap.
 * Make sure to call {@link #releaseResources()} method once the Stream usage is over and
 * data is transferred to the wanted destination.
 * Not thread safe!
 */
@InterfaceAudience.Private
public class ByteBufferListOutputStream extends ByteBufferOutputStream {
  private static final Log LOG = LogFactory.getLog(ByteBufferListOutputStream.class);

  private ByteBufferPool pool;
  // Keep track of the BBs where bytes written to. We will first try to get a BB from the pool. If
  // it is not available will make a new one our own and keep writing to that. We keep track of all
  // the BBs that we got from pool, separately so that on closeAndPutbackBuffers, we can make sure
  // to return back all of them to pool
  protected List<ByteBuffer> allBufs = new ArrayList<ByteBuffer>();
  protected List<ByteBuffer> bufsFromPool = new ArrayList<ByteBuffer>();

  private boolean lastBufFlipped = false;// Indicate whether the curBuf/lastBuf is flipped already

  public ByteBufferListOutputStream(ByteBufferPool pool) {
    this.pool = pool;
    allocateNewBuffer();
  }

  private void allocateNewBuffer() {
    if (this.curBuf != null) {
      this.curBuf.flip();// On the current buf set limit = pos and pos = 0.
    }
    // Get an initial BB to work with from the pool
    this.curBuf = this.pool.getBuffer();
    if (this.curBuf == null) {
      // No free BB at this moment. Make a new one. The pool returns off heap BBs. Don't make off
      // heap BB on demand. It is difficult to account for all such and so proper sizing of Max
      // direct heap size. See HBASE-15525 also for more details.
      // Make BB with same size of pool's buffer size.
      this.curBuf = ByteBuffer.allocate(this.pool.getBufferSize());
    } else {
      this.bufsFromPool.add(this.curBuf);
    }
    this.allBufs.add(this.curBuf);
  }

  @Override
  public int size() {
    int s = 0;
    for (int i = 0; i < this.allBufs.size() - 1; i++) {
      s += this.allBufs.get(i).remaining();
    }
    // On the last BB, it might not be flipped yet if getByteBuffers is not yet called
    if (this.lastBufFlipped) {
      s += this.curBuf.remaining();
    } else {
      s += this.curBuf.position();
    }
    return s;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    throw new UnsupportedOperationException("This stream is not backed by a single ByteBuffer");
  }

  @Override
  protected void checkSizeAndGrow(int extra) {
    long capacityNeeded = curBuf.position() + (long) extra;
    if (capacityNeeded > curBuf.limit()) {
      allocateNewBuffer();
    }
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    // No usage of this API in code. Just making it as an Unsupported operation as of now
    throw new UnsupportedOperationException();
  }

  /**
   * Release the resources it uses (The ByteBuffers) which are obtained from pool. Call this only
   * when all the data is fully used. And it must be called at the end of usage else we will leak
   * ByteBuffers from pool.
   */
  public void releaseResources() {
    try {
      close();
    } catch (IOException e) {
      LOG.debug(e);
    }
    // Return back all the BBs to pool
    if (this.bufsFromPool != null) {
      for (int i = 0; i < this.bufsFromPool.size(); i++) {
        this.pool.putbackBuffer(this.bufsFromPool.get(i));
      }
      this.bufsFromPool = null;
    }
    this.allBufs = null;
    this.curBuf = null;
  }

  @Override
  public byte[] toByteArray(int offset, int length) {
    // No usage of this API in code. Just making it as an Unsupported operation as of now
    throw new UnsupportedOperationException();
  }

  /**
   * We can be assured that the buffers returned by this method are all flipped
   * @return list of bytebuffers
   */
  public List<ByteBuffer> getByteBuffers() {
    if (!this.lastBufFlipped) {
      this.lastBufFlipped = true;
      // All the other BBs are already flipped while moving to the new BB.
      curBuf.flip();
    }
    return this.allBufs;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int toWrite = 0;
    while (len > 0) {
      toWrite = Math.min(len, this.curBuf.remaining());
      ByteBufferUtils.copyFromArrayToBuffer(this.curBuf, b, off, toWrite);
      off += toWrite;
      len -= toWrite;
      if (len > 0) {
        allocateNewBuffer();// The curBuf is over. Let us move to the next one
      }
    }
  }

  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    int toWrite = 0;
    while (len > 0) {
      toWrite = Math.min(len, this.curBuf.remaining());
      ByteBufferUtils.copyFromBufferToBuffer(b, this.curBuf, off, toWrite);
      off += toWrite;
      len -= toWrite;
      if (len > 0) {
        allocateNewBuffer();// The curBuf is over. Let us move to the next one
      }
    }
  }
}
