/**
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

package org.apache.hadoop.hbase.exceptions;

import java.net.ConnectException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Thrown when the client believes that we are trying to communicate to has
 * been repeatedly unresponsive for a while.
 *
 * On receiving such an exception. The HConnectionManager will skip all
 * retries and fast fail the operation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PreemptiveFastFailException extends ConnectException {
  private static final long serialVersionUID = 7129103682617007177L;
  private long failureCount, timeOfFirstFailureMilliSec, timeOfLatestAttemptMilliSec;

  // If set, we guarantee that no modifications went to server
  private boolean guaranteedClientSideOnly;

  /**
   * @param count num of consecutive failures
   * @param timeOfFirstFailureMilliSec when first failure happened
   * @param timeOfLatestAttemptMilliSec when last attempt happened
   * @param serverName server we failed to connect to
   */
  public PreemptiveFastFailException(long count, long timeOfFirstFailureMilliSec,
      long timeOfLatestAttemptMilliSec, ServerName serverName) {
    super("Exception happened " + count + " times. to" + serverName);
    this.failureCount = count;
    this.timeOfFirstFailureMilliSec = timeOfFirstFailureMilliSec;
    this.timeOfLatestAttemptMilliSec = timeOfLatestAttemptMilliSec;
  }

  /**
   * @param count num of consecutive failures
   * @param timeOfFirstFailureMilliSec when first failure happened
   * @param timeOfLatestAttemptMilliSec when last attempt happened
   * @param serverName server we failed to connect to
   * @param guaranteedClientSideOnly if true, guarantees that no mutations
   *   have been applied on the server
   */
  public PreemptiveFastFailException(long count, long timeOfFirstFailureMilliSec,
                                     long timeOfLatestAttemptMilliSec, ServerName serverName,
                                     boolean guaranteedClientSideOnly) {
    super("Exception happened " + count + " times. to" + serverName);
    this.failureCount = count;
    this.timeOfFirstFailureMilliSec = timeOfFirstFailureMilliSec;
    this.timeOfLatestAttemptMilliSec = timeOfLatestAttemptMilliSec;
    this.guaranteedClientSideOnly = guaranteedClientSideOnly;
  }

  /**
   * @return time of the fist failure
   */
  public long getFirstFailureAt() {
    return timeOfFirstFailureMilliSec;
  }

  /**
   * @return time of the latest attempt
   */
  public long getLastAttemptAt() {
    return timeOfLatestAttemptMilliSec;
  }

  /**
   * @return failure count
   */
  public long getFailureCount() {
    return failureCount;
  }

  /**
   * @return true if operation was attempted by server, false otherwise.
   */
  public boolean wasOperationAttemptedByServer() {
    return false;
  }

  /**
   * @return true if we know no mutation made it to the server, false otherwise.
   */
  public boolean isGuaranteedClientSideOnly() {
    return guaranteedClientSideOnly;
  }
}
