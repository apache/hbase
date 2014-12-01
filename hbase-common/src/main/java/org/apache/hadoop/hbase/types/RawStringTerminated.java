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

/**
 * An {@code DataType} that encodes variable-length values encoded using
 * {@link org.apache.hadoop.hbase.util.Bytes#toBytes(String)}. 
 * Includes a termination marker following the raw {@code byte[]} value. 
 * Intended to make it easier to transition away from direct use of 
 * {@link org.apache.hadoop.hbase.util.Bytes}.
 * @see org.apache.hadoop.hbase.util.Bytes#toBytes(String)
 * @see org.apache.hadoop.hbase.util.Bytes#toString(byte[], int, int)
 * @see RawString
 * @see OrderedString
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RawStringTerminated extends TerminatedWrapper<String> {

  /**
   * Create a {@code RawStringTerminated} using the specified terminator and
   * {@code order}.
   * @throws IllegalArgumentException if {@code term} is {@code null} or empty.
   */
  public RawStringTerminated(Order order, byte[] term) {
    super(new RawString(order), term);
  }

  /**
   * Create a {@code RawStringTerminated} using the specified terminator and
   * {@code order}.
   * @throws IllegalArgumentException if {@code term} is {@code null} or empty.
   */
  public RawStringTerminated(Order order, String term) {
    super(new RawString(order), term);
  }

  /**
   * Create a {@code RawStringTerminated} using the specified terminator.
   * @throws IllegalArgumentException if {@code term} is {@code null} or empty.
   */
  public RawStringTerminated(byte[] term) {
    super(new RawString(), term);
  }

  /**
   * Create a {@code RawStringTerminated} using the specified terminator.
   * @throws IllegalArgumentException if {@code term} is {@code null} or empty.
   */
  public RawStringTerminated(String term) {
    super(new RawString(), term);
  }
}
