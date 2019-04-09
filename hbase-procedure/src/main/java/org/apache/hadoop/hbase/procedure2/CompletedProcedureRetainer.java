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
package org.apache.hadoop.hbase.procedure2;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hold the reference to a completed root procedure. Will be cleaned up after expired.
 */
@InterfaceAudience.Private
class CompletedProcedureRetainer<TEnvironment> {
  private final Procedure<TEnvironment> procedure;
  private long clientAckTime;

  public CompletedProcedureRetainer(Procedure<TEnvironment> procedure) {
    this.procedure = procedure;
    clientAckTime = -1;
  }

  public Procedure<TEnvironment> getProcedure() {
    return procedure;
  }

  public boolean hasClientAckTime() {
    return clientAckTime != -1;
  }

  public long getClientAckTime() {
    return clientAckTime;
  }

  public void setClientAckTime(long clientAckTime) {
    this.clientAckTime = clientAckTime;
  }

  public boolean isExpired(long now, long evictTtl, long evictAckTtl) {
    return (hasClientAckTime() && (now - getClientAckTime()) >= evictAckTtl) ||
      (now - procedure.getLastUpdate()) >= evictTtl;
  }
}