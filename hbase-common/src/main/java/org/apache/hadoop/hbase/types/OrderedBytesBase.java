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
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;

/**
 * Base class for data types backed by the {@link OrderedBytes} encoding
 * implementations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class OrderedBytesBase<T> implements DataType<T> {

  protected final Order order;

  protected OrderedBytesBase(Order order) { this.order = order; }

  @Override
  public boolean isOrderPreserving() { return true; }

  @Override
  public Order getOrder() { return order; }

  // almost all OrderedBytes implementations are nullable.
  @Override
  public boolean isNullable() { return true; }

  // almost all OrderedBytes implementations are skippable.
  @Override
  public boolean isSkippable() { return true; }

  @Override
  public int skip(PositionedByteRange src) {
    return OrderedBytes.skip(src);
  }
}
