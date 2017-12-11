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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An {@link Iterator} over encoded {@code Struct} members.
 * <p>
 * This iterates over each serialized {@code Struct} field from the specified
 * {@code DataTypes<?>[]} definition. It allows you to read the field or skip
 * over its serialized bytes using {@link #next()} and {@link #skip()},
 * respectively. This is in contrast to the {@code Struct} method which allow
 * you to {@link Struct#decode(PositionedByteRange)} or
 * {@link Struct#skip(PositionedByteRange)} over the entire {@code Struct} at
 * once.
 * </p>
 * <p>
 * This iterator may also be used to read bytes from any {@code Struct} for
 * which the specified {@code DataType<?>[]} is a prefix. For example, if the
 * specified {@code Struct} definition has a {@link RawInteger} and a
 * {@link RawStringTerminated} field, you may parse the serialized output
 * of a {@code Struct} whose fields are {@link RawInteger},
 * {@link RawStringTerminated}, and {@link RawBytes}. The iterator would
 * return a number followed by a {@code String}. The trailing {@code byte[]}
 * would be ignored.
 * </p>
 */
@InterfaceAudience.Public
public class StructIterator implements Iterator<Object> {

  protected final PositionedByteRange src;
  protected int idx = 0;
  @SuppressWarnings("rawtypes")
  protected final DataType[] types;

  /**
   * Construct {@code StructIterator} over the values encoded in {@code src}
   * using the specified {@code types} definition.
   * @param src The buffer from which to read encoded values.
   * @param types The sequence of types to use as the schema for this
   *          {@code Struct}.
   */
  public StructIterator(PositionedByteRange src, @SuppressWarnings("rawtypes") DataType[] types) {
    this.src = src;
    this.types = types;
  }

  @Override
  public boolean hasNext() {
    // hasNext can return true when position == length in the case of a
    // nullable field trailing a struct.
    return idx < types.length && src.getPosition() <= src.getLength();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    DataType<?> t = types[idx++];
    if (src.getPosition() == src.getLength() && t.isNullable()) {
      return null;
    }
    return t.decode(src);
  }

  /**
   * Bypass the next encoded value.
   * @return the number of bytes skipped.
   */
  public int skip() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    DataType<?> t = types[idx++];
    if (src.getPosition() == src.getLength() && t.isNullable()) {
      return 0;
    }
    return t.skip(src);
  }
}
