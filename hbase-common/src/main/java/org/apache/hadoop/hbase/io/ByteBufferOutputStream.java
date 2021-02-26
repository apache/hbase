/*
 *
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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Not thread safe!
 */
@InterfaceAudience.Public
public class ByteBufferOutputStream extends OutputStream
    implements ByteBufferWriter {
  
  // Borrowed from openJDK:
  // http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/8-b132/java/util/ArrayList.java#221
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  protected ByteBuffer curBuf = null;

  ByteBufferOutputStream() {

  }

  public ByteBufferOutputStream(int capacity) {
    this(capacity, false);
  }

  public ByteBufferOutputStream(int capacity, boolean useDirectByteBuffer) {
    this(allocate(capacity, useDirectByteBuffer));
  }

  /**
   * @param bb ByteBuffer to use. If too small, will be discarded and a new one allocated in its
   * place; i.e. the passed in BB may NOT BE RETURNED!! Minimally it will be altered. SIDE EFFECT!!
   * If you want to get the newly allocated ByteBuffer, you'll need to pick it up when
   * done with this instance by calling {@link #getByteBuffer()}. All this encapsulation violation
   * is so we can recycle buffers rather than allocate each time; it can get expensive especially
   * if the buffers are big doing allocations each time or having them undergo resizing because
   * initial allocation was small.
   * @see #getByteBuffer()
   */
  public ByteBufferOutputStream(final ByteBuffer bb) {
    assert bb.order() == ByteOrder.BIG_ENDIAN;
    this.curBuf = bb;
    this.curBuf.clear();
  }

  public int size() {
    return curBuf.position();
  }

  private static ByteBuffer allocate(final int capacity, final boolean useDirectByteBuffer) {
    if (capacity > MAX_ARRAY_SIZE) { // avoid OutOfMemoryError
      throw new BufferOverflowException();
    }
    return useDirectByteBuffer? ByteBuffer.allocateDirect(capacity): ByteBuffer.allocate(capacity);
  }

  /**
   * This flips the underlying BB so be sure to use it _last_!
   * @return ByteBuffer
   */
  public ByteBuffer getByteBuffer() {
    curBuf.flip();
    return curBuf;
  }

  protected void checkSizeAndGrow(int extra) {
    long capacityNeeded = curBuf.position() + (long) extra;
    if (capacityNeeded > curBuf.limit()) {
      // guarantee it's possible to fit
      if (capacityNeeded > MAX_ARRAY_SIZE) {
        throw new BufferOverflowException();
      }
      // double until hit the cap
      long nextCapacity = Math.min(curBuf.capacity() * 2L, MAX_ARRAY_SIZE);
      // but make sure there is enough if twice the existing capacity is still too small
      nextCapacity = Math.max(nextCapacity, capacityNeeded);
      ByteBuffer newBuf = allocate((int) nextCapacity, curBuf.isDirect());
      curBuf.flip();
      ByteBufferUtils.copyFromBufferToBuffer(curBuf, newBuf);
      curBuf = newBuf;
    }
  }

  // OutputStream
  @Override
  public void write(int b) throws IOException {
    checkSizeAndGrow(Bytes.SIZEOF_BYTE);
    curBuf.put((byte)b);
  }

 /**
  * Writes the complete contents of this byte buffer output stream to
  * the specified output stream argument.
  *
  * @param      out   the output stream to which to write the data.
  * @exception  IOException  if an I/O error occurs.
  */
  public void writeTo(OutputStream out) throws IOException {
    WritableByteChannel channel = Channels.newChannel(out);
    ByteBuffer bb = curBuf.duplicate();
    bb.flip();
    channel.write(bb);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkSizeAndGrow(len);
    ByteBufferUtils.copyFromArrayToBuffer(curBuf, b, off, len);
  }

  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    checkSizeAndGrow(len);
    ByteBufferUtils.copyFromBufferToBuffer(b, curBuf, off, len);
  }

  /**
   * Writes an <code>int</code> to the underlying output stream as four
   * bytes, high byte first.
   * @param i the <code>int</code> to write
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public void writeInt(int i) throws IOException {
    checkSizeAndGrow(Bytes.SIZEOF_INT);
    ByteBufferUtils.putInt(this.curBuf, i);
  }

  @Override
  public void flush() throws IOException {
    // noop
  }

  @Override
  public void close() throws IOException {
    // noop again. heh
  }

  public byte[] toByteArray(int offset, int length) {
    ByteBuffer bb = curBuf.duplicate();
    bb.flip();

    byte[] chunk = new byte[length];

    bb.position(offset);
    bb.get(chunk, 0, length);
    return chunk;
  }
}
