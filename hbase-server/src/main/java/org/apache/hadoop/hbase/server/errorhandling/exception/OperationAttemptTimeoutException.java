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
package org.apache.hadoop.hbase.server.errorhandling.exception;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;

/**
 * Exception for a timeout of a task.
 * @see OperationAttemptTimer
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class OperationAttemptTimeoutException extends Exception {

  /**
   * Exception indicating that an operation attempt has timed out
   * @param start time the operation started (ms since epoch)
   * @param end time the timeout was triggered (ms since epoch)
   * @param allowed max allow amount of time for the operation to complete (ms)
   */
  public OperationAttemptTimeoutException(long start, long end, long allowed) {
    super("Timeout elapsed! Start:" + start + ", End:" + end + ", diff:" + (end - start) + ", max:"
        + allowed + " ms");
  }
}