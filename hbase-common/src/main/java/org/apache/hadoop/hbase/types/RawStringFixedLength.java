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

/**
 * An {@code DataType} that encodes fixed-length values encoded using
 * {@link Bytes#toBytes(String)}. Intended to make it easier to transition
 * away from direct use of {@link Bytes}.
 * @see Bytes#toBytes(String)
 * @see Bytes#toString(byte[], int, int)
 * @see RawString
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RawStringFixedLength extends FixedLengthWrapper<String> {

  /**
   * Create a {@code RawStringFixedLength} using the specified
   * {@code order} and {@code length}.
   */
  public RawStringFixedLength(Order order, int length) {
    super(new RawString(order), length);
  }

  /**
   * Create a {@code RawStringFixedLength} of the specified {@code length}.
   */
  public RawStringFixedLength(int length) {
    super(new RawString(), length);
  }
}
