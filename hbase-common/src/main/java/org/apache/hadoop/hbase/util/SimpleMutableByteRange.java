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
 *
 * The portion of this file denoted by 'Copied from com.google.protobuf.CodedOutputStream'
 * is from Protocol Buffers v2.5.0 under the following license
 *
 * Copyright 2008 Google Inc.  All rights reserved.
 * http://code.google.com/p/protobuf/
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.hadoop.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A basic mutable {@link ByteRange} implementation.
 */
@InterfaceAudience.Public
public class SimpleMutableByteRange extends AbstractByteRange {

  /**
   * Create a new {@code ByteRange} lacking a backing array and with an
   * undefined viewport.
   */
  public SimpleMutableByteRange() {
    unset();
  }

  /**
   * Create a new {@code ByteRange} over a new backing array of size
   * {@code capacity}. The range's offset and length are 0 and {@code capacity},
   * respectively.
   * 
   * @param capacity
   *          the size of the backing array.
   */
  public SimpleMutableByteRange(int capacity) {
    this(new byte[capacity]);
  }

  /**
   * Create a new {@code ByteRange} over the provided {@code bytes}.
   * 
   * @param bytes
   *          The array to wrap.
   */
  public SimpleMutableByteRange(byte[] bytes) {
    set(bytes);
  }

  /**
   * Create a new {@code ByteRange} over the provided {@code bytes}.
   *
   * @param bytes
   *          The array to wrap.
   * @param offset
   *          The offset into {@code bytes} considered the beginning of this
   *          range.
   * @param length
   *          The length of this range.
   */
  public SimpleMutableByteRange(byte[] bytes, int offset, int length) {
    set(bytes, offset, length);
  }

  @Override
  public ByteRange unset() {
    clearHashCache();
    bytes = null;
    offset = 0;
    length = 0;
    return this;
  }

  @Override
  public ByteRange put(int index, byte val) {
    bytes[offset + index] = val;
    clearHashCache();
    return this;
  }

  @Override
  public ByteRange put(int index, byte[] val) {
    if (0 == val.length)
      return this;
    return put(index, val, 0, val.length);
  }

  @Override
  public ByteRange put(int index, byte[] val, int offset, int length) {
    if (0 == length)
      return this;
    System.arraycopy(val, offset, this.bytes, this.offset + index, length);
    clearHashCache();
    return this;
  }

  @Override
  public ByteRange putShort(int index, short val) {
    // This writing is same as BB's putShort. When byte[] is wrapped in a BB and
    // call putShort(),
    // one can get the same result.
    bytes[offset + index + 1] = (byte) val;
    val >>= 8;
    bytes[offset + index] = (byte) val;
    clearHashCache();
    return this;
  }

  @Override
  public ByteRange putInt(int index, int val) {
    // This writing is same as BB's putInt. When byte[] is wrapped in a BB and
    // call getInt(), one
    // can get the same result.
    for (int i = Bytes.SIZEOF_INT - 1; i > 0; i--) {
      bytes[offset + index + i] = (byte) val;
      val >>>= 8;
    }
    bytes[offset + index] = (byte) val;
    clearHashCache();
    return this;
  }

  @Override
  public ByteRange putLong(int index, long val) {
    // This writing is same as BB's putLong. When byte[] is wrapped in a BB and
    // call putLong(), one
    // can get the same result.
    for (int i = Bytes.SIZEOF_LONG - 1; i > 0; i--) {
      bytes[offset + index + i] = (byte) val;
      val >>>= 8;
    }
    bytes[offset + index] = (byte) val;
    clearHashCache();
    return this;
  }

  // Copied from com.google.protobuf.CodedOutputStream v2.5.0 writeRawVarint64
  @Override
  public int putVLong(int index, long val) {
    int rPos = 0;
    while (true) {
      if ((val & ~0x7F) == 0) {
        bytes[offset + index + rPos] = (byte) val;
        break;
      } else {
        bytes[offset + index + rPos] = (byte) ((val & 0x7F) | 0x80);
        val >>>= 7;
      }
      rPos++;
    }
    clearHashCache();
    return rPos + 1;
  }
  // end copied from protobuf

  @Override
  public ByteRange deepCopy() {
    SimpleMutableByteRange clone = new SimpleMutableByteRange(deepCopyToNewArray());
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  @Override
  public ByteRange shallowCopy() {
    SimpleMutableByteRange clone = new SimpleMutableByteRange(bytes, offset, length);
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  @Override
  public ByteRange shallowCopySubRange(int innerOffset, int copyLength) {
    SimpleMutableByteRange clone = new SimpleMutableByteRange(bytes, offset + innerOffset,
        copyLength);
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  @Override
  public boolean equals(Object thatObject) {
    if (thatObject == null) {
      return false;
    }
    if (this == thatObject) {
      return true;
    }
    if (hashCode() != thatObject.hashCode()) {
      return false;
    }
    if (!(thatObject instanceof SimpleMutableByteRange)) {
      return false;
    }
    SimpleMutableByteRange that = (SimpleMutableByteRange) thatObject;
    return Bytes.equals(bytes, offset, length, that.bytes, that.offset, that.length);
  }

}
