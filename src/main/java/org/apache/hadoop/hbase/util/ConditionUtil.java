/**
 * Copyright The Apache Software Foundation
 *
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


import com.google.common.base.Preconditions;

/**
 * Extends the patterns in {@link Preconditions}
 */
public class ConditionUtil {

  /**
   * Checks if a specified offset is >= 0
   * @param offset The offset to check
   * @return The specified offset if it is >= 0
   * @throws IndexOutOfBoundsException If specified offset is negative
   */
  public static long checkPositiveOffset(long offset) {
    return checkOffset(offset, -1);
  }

  /**
   * Check if an offset is >= 0 but less than a maximum limit (if one is
   * specified).
   * @see {@link Preconditions#checkPositionIndex(int, int)}
   * @param offset The offset to check
   * @param limit The maximum limit or -1 if none
   * @return The specified offset if it is positive and if the a limit is
   *         specified lower than that limit.
   * @throws IllegalStateException If the offset is negative, or if a limit
   *         is specified and the offset is greater than the limit.
   */
  public static long checkOffset(long offset, long limit) {
    if (offset < 0) {
      throw new IndexOutOfBoundsException("Negative offset: " + offset);
    }
    if (limit != -1 && offset >= limit) {
      throw new IndexOutOfBoundsException("Offset (" + offset +
          ") is greater than limit (" + limit + ")");
    }
    return offset;
  }
}
