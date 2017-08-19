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
package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for compaction window implementation.
 */
@InterfaceAudience.Private
public abstract class CompactionWindow {

  /**
   * Compares the window to a timestamp.
   * @param timestamp the timestamp to compare.
   * @return a negative integer, zero, or a positive integer as the window lies before, covering, or
   *         after than the timestamp.
   */
  public abstract int compareToTimestamp(long timestamp);

  /**
   * Move to the new window of the same tier or of the next tier, which represents an earlier time
   * span.
   * @return The next earlier window
   */
  public abstract CompactionWindow nextEarlierWindow();

  /**
   * Inclusive lower bound
   */
  public abstract long startMillis();

  /**
   * Exclusive upper bound
   */
  public abstract long endMillis();

  @Override
  public String toString() {
    return "[" + startMillis() + ", " + endMillis() + ")";
  }
}
