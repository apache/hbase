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
 */
package org.apache.hadoop.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Interface for computing 64-bit hash values.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface Hash64 {
  /**
   * Computes a 64-bit hash from the given {@code HashKey} using a seed of 0.
   * @param hashKey the input key providing byte access
   * @return the computed 64-bit hash value
   */
  default <T> long hash64(HashKey<T> hashKey) {
    return hash64(hashKey, 0L);
  }

  /**
   * Computes a 64-bit hash from the given {@code HashKey} and seed.
   * @param hashKey the input key providing byte access
   * @param seed    the 64-bit seed value
   * @return the computed 64-bit hash value
   */
  <T> long hash64(HashKey<T> hashKey, long seed);
}
