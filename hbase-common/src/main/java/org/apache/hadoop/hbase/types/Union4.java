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

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The {@code Union} family of {@link DataType}s encode one of a fixed
 * collection of Objects. They provide convenience methods which handle type
 * casting on your behalf.
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Public
public abstract class Union4<A, B, C, D> extends Union3<A, B, C> {

  protected final DataType<D> typeD;

  /**
   * Create an instance of {@code Union4} over the set of specified
   * types.
   */
  public Union4(DataType<A> typeA, DataType<B> typeB, DataType<C> typeC, DataType<D> typeD) {
    super(typeA, typeB, typeC);
    this.typeD = typeD;
  }

  @Override
  public boolean isOrderPreserving() {
    return super.isOrderPreserving() && typeD.isOrderPreserving();
  }

  @Override
  public Order getOrder() {
    return null;
  }

  @Override
  public boolean isNullable() {
    return super.isNullable() && typeD.isNullable();
  }

  @Override
  public boolean isSkippable() {
    return super.isSkippable() && typeD.isSkippable();
  }

  /**
   * Read an instance of the fourth type parameter from buffer {@code src}.
   */
  public D decodeD(PositionedByteRange src) {
    return (D) decode(src);
  }
}
