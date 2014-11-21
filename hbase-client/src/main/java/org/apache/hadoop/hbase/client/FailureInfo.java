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
package org.apache.hadoop.hbase.client;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Keeps track of repeated failures to any region server. Multiple threads manipulate the contents
 * of this thread.
 *
 * Access to the members is guarded by the concurrent nature of the members inherently.
 * 
 */
@InterfaceAudience.Private
class FailureInfo {
  // The number of consecutive failures.
  public final AtomicLong numConsecutiveFailures = new AtomicLong();
  // The time when the server started to become unresponsive
  // Once set, this would never be updated.
  public final long timeOfFirstFailureMilliSec;
  // The time when the client last tried to contact the server.
  // This is only updated by one client at a time
  public volatile long timeOfLatestAttemptMilliSec;
  // Used to keep track of concurrent attempts to contact the server.
  // In Fast fail mode, we want just one client thread to try to connect
  // the rest of the client threads will fail fast.
  public final AtomicBoolean exclusivelyRetringInspiteOfFastFail = new AtomicBoolean(
      false);

  @Override
  public String toString() {
    return "FailureInfo: numConsecutiveFailures = "
        + numConsecutiveFailures + " timeOfFirstFailureMilliSec = "
        + timeOfFirstFailureMilliSec + " timeOfLatestAttemptMilliSec = "
        + timeOfLatestAttemptMilliSec
        + " exclusivelyRetringInspiteOfFastFail  = "
        + exclusivelyRetringInspiteOfFastFail.get();
  }

  FailureInfo(long firstFailureTime) {
    this.timeOfFirstFailureMilliSec = firstFailureTime;
  }
}
