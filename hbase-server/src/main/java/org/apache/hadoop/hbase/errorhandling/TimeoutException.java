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
package org.apache.hadoop.hbase.errorhandling;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Exception for timeout of a task.
 * @see TimeoutExceptionInjector
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class TimeoutException extends Exception {

  private final String sourceName;
  private final long start;
  private final long end;
  private final long expected;

  /**
   * Exception indicating that an operation attempt has timed out
   * @param start time the operation started (ms since epoch)
   * @param end time the timeout was triggered (ms since epoch)
   * @param expected expected amount of time for the operation to complete (ms) (ideally, expected <= end-start)
   */
  public TimeoutException(String sourceName, long start, long end, long expected) {
    super("Timeout elapsed! Source:" + sourceName + " Start:" + start + ", End:" + end
        + ", diff:" + (end - start) + ", max:" + expected + " ms");
    this.sourceName = sourceName;
    this.start = start;
    this.end = end;
    this.expected = expected;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public long getMaxAllowedOperationTime() {
    return expected;
  }

  public String getSourceName() {
    return sourceName;
  }
}
