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

/**
 * The {@code Union} family of {@link DataType}s encode one of a fixed
 * set of {@code Object}s. They provide convenience methods which handle
 * type casting on your behalf.
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Union2<A, B> implements DataType<Object> {

  protected final DataType<A> typeA;
  protected final DataType<B> typeB;

  /**
   * Create an instance of {@code Union2} over the set of specified
   * types.
   */
  public Union2(DataType<A> typeA, DataType<B> typeB) {
    this.typeA = typeA;
    this.typeB = typeB;
  }

  @Override
  public boolean isOrderPreserving() {
    return typeA.isOrderPreserving() && typeB.isOrderPreserving();
  }

  @Override
  public Order getOrder() { return null; }

  @Override
  public boolean isNullable() {
    return typeA.isNullable() && typeB.isNullable();
  }

  @Override
  public boolean isSkippable() {
    return typeA.isSkippable() && typeB.isSkippable();
  }

  @Override
  public Class<Object> encodedClass() {
    throw new UnsupportedOperationException(
      "Union types do not expose a definitive encoded class.");
  }

  /**
   * Read an instance of the first type parameter from buffer {@code src}.
   */
  public A decodeA(PositionedByteRange src) {
    return (A) decode(src);
  }

  /**
   * Read an instance of the second type parameter from buffer {@code src}.
   */
  public B decodeB(PositionedByteRange src) {
    return (B) decode(src);
  }
}
