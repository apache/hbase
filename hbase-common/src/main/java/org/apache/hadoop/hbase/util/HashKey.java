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

/**
 * Used to calculate the hash {@link Hash} algorithms for Bloomfilters.
 * @param <T> the type of HashKey
 */
@InterfaceAudience.Private
public abstract class HashKey<T> {
  protected final T t;

  public HashKey(T t) {
    this.t = t;
  }

  /**
   * n * @return The byte at the given position in this HashKey
   */
  public abstract byte get(int pos);

  /**
   * @return The number of bytes in this HashKey
   */
  public abstract int length();
}
