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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ServerName;

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

   /**
    * @param count
    * @param timeOfFirstFailureMilliSec
    * @param timeOfLatestAttemptMilliSec
    * @param serverName
    */
   public PreemptiveFastFailException(long count, long timeOfFirstFailureMilliSec,
       long timeOfLatestAttemptMilliSec, ServerName serverName) {
     super("Exception happened " + count + " times. to" + serverName);
     this.failureCount = count;
     this.timeOfFirstFailureMilliSec = timeOfFirstFailureMilliSec;
     this.timeOfLatestAttemptMilliSec = timeOfLatestAttemptMilliSec;
   }

   public long getFirstFailureAt() {
     return timeOfFirstFailureMilliSec;
   }

   public long getLastAttemptAt() {
     return timeOfLatestAttemptMilliSec;
   }

   public long getFailureCount() {
     return failureCount;
   }

   public boolean wasOperationAttemptedByServer() {
     return false;
   }
 }