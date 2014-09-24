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
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.io.Writable;

/**
 * <p>
 * {@code DataType} is the base class for all HBase data types. Data
 * type implementations are designed to be serialized to and deserialized from
 * byte[]. Serialized representations can retain the natural sort ordering of
 * the source object, when a suitable encoding is supported by the underlying
 * implementation. This is a desirable feature for use in rowkeys and column
 * qualifiers.
 * </p>
 * <p>
 * {@code DataType}s are different from Hadoop {@link Writable}s in two
 * significant ways. First, {@code DataType} describes how to serialize a
 * value, it does not encapsulate a serialized value. Second, {@code DataType}
 * implementations provide hints to consumers about relationships between the
 * POJOs they represent and richness of the encoded representation.
 * </p>
 * <p>
 * Data type instances are designed to be stateless, thread-safe, and reused.
 * Implementations should provide {@code static final} instances corresponding
 * to each variation on configurable parameters. This is to encourage and
 * simplify instance reuse. For instance, order-preserving types should provide
 * static ASCENDING and DESCENDING instances. It is also encouraged for
 * implementations operating on Java primitive types to provide primitive
 * implementations of the {@code encode} and {@code decode} methods. This
 * advice is a performance consideration to clients reading and writing values
 * in tight loops.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DataType<T> {

  /**
   * Indicates whether this instance writes encoded {@code byte[]}'s
   * which preserve the natural sort order of the unencoded value.
   * @return {@code true} when natural order is preserved,
   *         {@code false} otherwise.
   */
  public boolean isOrderPreserving();

  /**
   * Retrieve the sort {@link Order} imposed by this data type, or null when
   * natural ordering is not preserved. Value is either ascending or
   * descending. Default is assumed to be {@link Order#ASCENDING}.
   */
  public Order getOrder();

  /**
   * Indicates whether this instance supports encoding null values. This
   * depends on the implementation details of the encoding format. All
   * {@code DataType}s that support null should treat null as comparing
   * less than any non-null value for default sort ordering purposes.
   * @return {@code true} when null is supported, {@code false} otherwise.
   */
  public boolean isNullable();

  /**
   * Indicates whether this instance is able to skip over it's encoded value.
   * {@code DataType}s that are not skippable can only be used as the
   * right-most field of a {@link Struct}.
   */
  public boolean isSkippable();

  /**
   * Inform consumers how long the encoded {@code byte[]} will be.
   * @param val The value to check.
   * @return the number of bytes required to encode {@code val}.a
   */
  public int encodedLength(T val);

  /**
   * Inform consumers over what type this {@code DataType} operates. Useful
   * when working with bare {@code DataType} instances.
   */
  public Class<T> encodedClass();

  /**
   * Skip {@code src}'s position forward over one encoded value.
   * @param src the buffer containing the encoded value.
   * @return number of bytes skipped.
   */
  public int skip(PositionedByteRange src);

  /**
   * Read an instance of {@code T} from the buffer {@code src}.
   * @param src the buffer containing the encoded value.
   */
  public T decode(PositionedByteRange src);

  /**
   * Write instance {@code val} into buffer {@code dst}.
   * @param dst the buffer containing the encoded value.
   * @param val the value to encode onto {@code dst}.
   * @return number of bytes written.
   */
  public int encode(PositionedByteRange dst, T val);
}
