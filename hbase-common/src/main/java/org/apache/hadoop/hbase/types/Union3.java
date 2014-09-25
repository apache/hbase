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
 * collection of Objects. They provide convenience methods which handle type
 * casting on your behalf.
 * @see Union2
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Union3<A, B, C> extends Union2<A, B> {

  protected final DataType<C> typeC;

  /**
   * Create an instance of {@code Union3} over the set of specified
   * types.
   */
  public Union3(DataType<A> typeA, DataType<B> typeB, DataType<C> typeC) {
    super(typeA, typeB);
    this.typeC = typeC;
  }

  @Override
  public boolean isOrderPreserving() {
    return super.isOrderPreserving() && typeC.isOrderPreserving();
  }

  @Override
  public Order getOrder() { return null; }

  @Override
  public boolean isNullable() {
    return super.isNullable() && typeC.isNullable();
  }

  @Override
  public boolean isSkippable() {
    return super.isSkippable() && typeC.isSkippable();
  }

  /**
   * Read an instance of the third type parameter from buffer {@code src}.
   */
  public C decodeC(PositionedByteRange src) {
    return (C) decode(src);
  }
}
