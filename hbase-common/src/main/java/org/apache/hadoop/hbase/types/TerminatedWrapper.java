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
package org.apache.hadoop.hbase.types;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;

/**
 * Wraps an existing {@code DataType} implementation as a terminated
 * version of itself. This has the useful side-effect of turning an existing
 * {@code DataType} which is not {@code skippable} into a
 * {@code skippable} variant.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TerminatedWrapper<T> implements DataType<T> {

  protected final DataType<T> wrapped;
  protected final byte[] term;

  /**
   * Create a terminated version of the {@code wrapped}.
   * @throws IllegalArgumentException when {@code term} is null or empty.
   */
  public TerminatedWrapper(DataType<T> wrapped, byte[] term) {
    if (null == term || term.length == 0)
      throw new IllegalArgumentException("terminator must be non-null and non-empty.");
    this.wrapped = wrapped;
    wrapped.getOrder().apply(term);
    this.term = term;
  }

  /**
   * Create a terminated version of the {@code wrapped}.
   * {@code term} is converted to a {@code byte[]} using
   * {@link Bytes#toBytes(String)}.
   * @throws IllegalArgumentException when {@code term} is null or empty.
   */
  public TerminatedWrapper(DataType<T> wrapped, String term) {
    this(wrapped, Bytes.toBytes(term));
  }

  @Override
  public boolean isOrderPreserving() { return wrapped.isOrderPreserving(); }

  @Override
  public Order getOrder() { return wrapped.getOrder(); }

  @Override
  public boolean isNullable() { return wrapped.isNullable(); }

  @Override
  public boolean isSkippable() { return true; }

  @Override
  public int encodedLength(T val) {
    return wrapped.encodedLength(val) + term.length;
  }

  @Override
  public Class<T> encodedClass() { return wrapped.encodedClass(); }

  /**
   * Return the position at which {@code term} begins within {@code src},
   * or {@code -1} if {@code term} is not found.
   */
  protected int terminatorPosition(PositionedByteRange src) {
    byte[] a = src.getBytes();
    final int offset = src.getOffset();
    int i;
    SKIP: for (i = src.getPosition(); i < src.getLength(); i++) {
      if (a[offset + i] != term[0]) continue;
      int j;
      for (j = 1; j < term.length && offset + j < src.getLength(); j++) {
        if (a[offset + i + j] != term[j]) continue SKIP;
      }
      if (j == term.length) return i; // success
    }
    return -1;
  }

  /**
   * Skip {@code src}'s position forward over one encoded value.
   * @param src the buffer containing the encoded value.
   * @return number of bytes skipped.
   * @throws IllegalArgumentException when the terminator sequence is not found.
   */
  @Override
  public int skip(PositionedByteRange src) {
    if (wrapped.isSkippable()) {
      int ret = wrapped.skip(src);
      src.setPosition(src.getPosition() + term.length);
      return ret + term.length;
    } else {
      // find the terminator position
      final int start = src.getPosition();
      int skipped = terminatorPosition(src);
      if (-1 == skipped) throw new IllegalArgumentException("Terminator sequence not found.");
      skipped += term.length;
      src.setPosition(skipped);
      return skipped - start;
    }
  }

  @Override
  public T decode(PositionedByteRange src) {
    if (wrapped.isSkippable()) {
      T ret = wrapped.decode(src);
      src.setPosition(src.getPosition() + term.length);
      return ret;
    } else {
      // find the terminator position
      int term = terminatorPosition(src);
      if (-1 == term) throw new IllegalArgumentException("Terminator sequence not found.");
      byte[] b = new byte[term - src.getPosition()];
      src.get(b);
      // TODO: should we assert that b.position == b.length?
      T ret = wrapped.decode(new SimplePositionedMutableByteRange(b));
      src.get(this.term);
      return ret;
    }
  }

  /**
   * Write instance {@code val} into buffer {@code dst}.
   * @throws IllegalArgumentException when the encoded representation of
   *           {@code val} contains the {@code term} sequence.
   */
  @Override
  public int encode(PositionedByteRange dst, T val) {
    final int start = dst.getPosition();
    int written = wrapped.encode(dst, val);
    PositionedByteRange b = dst.shallowCopy();
    b.setLength(dst.getPosition());
    b.setPosition(start);
    if (-1 != terminatorPosition(b)) {
      dst.setPosition(start);
      throw new IllegalArgumentException("Encoded value contains terminator sequence.");
    }
    dst.put(term);
    return written + term.length;
  }
}
