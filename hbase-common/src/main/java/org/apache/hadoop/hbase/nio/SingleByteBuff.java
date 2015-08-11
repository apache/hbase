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
package org.apache.hadoop.hbase.nio;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.ObjectIntPair;

/**
 * An implementation of ByteBuff where a single BB backs the BBI. This just acts
 * as a wrapper over a normal BB - offheap or onheap
 */
@InterfaceAudience.Private
public class SingleByteBuff extends ByteBuff {

  // Underlying BB
  private final ByteBuffer buf;

  public SingleByteBuff(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public int position() {
    return this.buf.position();
  }

  @Override
  public SingleByteBuff position(int position) {
    this.buf.position(position);
    return this;
  }

  @Override
  public SingleByteBuff skip(int len) {
    this.buf.position(this.buf.position() + len);
    return this;
  }

  @Override
  public SingleByteBuff moveBack(int len) {
    this.buf.position(this.buf.position() - len);
    return this;
  }

  @Override
  public int capacity() {
    return this.buf.capacity();
  }

  @Override
  public int limit() {
    return this.buf.limit();
  }

  @Override
  public SingleByteBuff limit(int limit) {
    this.buf.limit(limit);
    return this;
  }

  @Override
  public SingleByteBuff rewind() {
    this.buf.rewind();
    return this;
  }

  @Override
  public SingleByteBuff mark() {
    this.buf.mark();
    return this;
  }

  @Override
  public ByteBuffer asSubByteBuffer(int length) {
    // Just return the single BB that is available
    return this.buf;
  }

  @Override
  public void asSubByteBuffer(int offset, int length, ObjectIntPair<ByteBuffer> pair) {
    // Just return the single BB that is available
    pair.setFirst(this.buf);
    pair.setSecond(offset);
  }

  @Override
  public int remaining() {
    return this.buf.remaining();
  }

  @Override
  public boolean hasRemaining() {
    return buf.hasRemaining();
  }

  @Override
  public SingleByteBuff reset() {
    this.buf.reset();
    return this;
  }

  @Override
  public SingleByteBuff slice() {
    return new SingleByteBuff(this.buf.slice());
  }

  @Override
  public SingleByteBuff duplicate() {
    return new SingleByteBuff(this.buf.duplicate());
  }

  @Override
  public byte get() {
    return buf.get();
  }

  @Override
  public byte get(int index) {
    return ByteBufferUtils.toByte(this.buf, index);
  }

  @Override
  public byte getByteAfterPosition(int offset) {
    return ByteBufferUtils.toByte(this.buf, this.buf.position() + offset);
  }

  @Override
  public SingleByteBuff put(byte b) {
    this.buf.put(b);
    return this;
  }

  @Override
  public SingleByteBuff put(int index, byte b) {
    buf.put(index, b);
    return this;
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    ByteBufferUtils.copyFromBufferToArray(dst, buf, buf.position(), offset, length);
    buf.position(buf.position() + length);
  }

  @Override
  public void get(byte[] dst) {
    get(dst, 0, dst.length);
  }

  @Override
  public SingleByteBuff put(int offset, ByteBuff src, int srcOffset, int length) {
    if (src instanceof SingleByteBuff) {
      ByteBufferUtils.copyFromBufferToBuffer(((SingleByteBuff) src).buf, this.buf, srcOffset,
          offset, length);
    } else {
      // TODO we can do some optimization here? Call to asSubByteBuffer might
      // create a copy.
      ObjectIntPair<ByteBuffer> pair = new ObjectIntPair<ByteBuffer>();
      src.asSubByteBuffer(srcOffset, length, pair);
      ByteBufferUtils.copyFromBufferToBuffer(pair.getFirst(), this.buf, pair.getSecond(), offset,
          length);
    }
    return this;
  }

  @Override
  public SingleByteBuff put(byte[] src, int offset, int length) {
    ByteBufferUtils.copyFromArrayToBuffer(this.buf, src, offset, length);
    return this;
  }

  @Override
  public SingleByteBuff put(byte[] src) {
    return put(src, 0, src.length);
  }

  @Override
  public boolean hasArray() {
    return this.buf.hasArray();
  }

  @Override
  public byte[] array() {
    return this.buf.array();
  }

  @Override
  public int arrayOffset() {
    return this.buf.arrayOffset();
  }

  @Override
  public short getShort() {
    return this.buf.getShort();
  }

  @Override
  public short getShort(int index) {
    return ByteBufferUtils.toShort(this.buf, index);
  }

  @Override
  public short getShortAfterPosition(int offset) {
    return ByteBufferUtils.toShort(this.buf, this.buf.position() + offset);
  }

  @Override
  public int getInt() {
    return this.buf.getInt();
  }

  @Override
  public SingleByteBuff putInt(int value) {
    ByteBufferUtils.putInt(this.buf, value);
    return this;
  }

  @Override
  public int getInt(int index) {
    return ByteBufferUtils.toInt(this.buf, index);
  }

  @Override
  public int getIntAfterPosition(int offset) {
    return ByteBufferUtils.toInt(this.buf, this.buf.position() + offset);
  }

  @Override
  public long getLong() {
    return this.buf.getLong();
  }

  @Override
  public SingleByteBuff putLong(long value) {
    ByteBufferUtils.putLong(this.buf, value);
    return this;
  }

  @Override
  public long getLong(int index) {
    return ByteBufferUtils.toLong(this.buf, index);
  }

  @Override
  public long getLongAfterPosition(int offset) {
    return ByteBufferUtils.toLong(this.buf, this.buf.position() + offset);
  }

  @Override
  public byte[] toBytes(int offset, int length) {
    byte[] output = new byte[length];
    ByteBufferUtils.copyFromBufferToArray(output, buf, offset, 0, length);
    return output;
  }

  @Override
  public void get(ByteBuffer out, int sourceOffset, int length) {
    ByteBufferUtils.copyFromBufferToBuffer(buf, out, sourceOffset, length);
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof SingleByteBuff)) return false;
    return this.buf.equals(((SingleByteBuff)obj).buf);
  }

  @Override
  public int hashCode() {
    return this.buf.hashCode();
  }

  /**
   * @return the ByteBuffer which this wraps.
   */
  ByteBuffer getEnclosingByteBuffer() {
    return this.buf;
  }
}
