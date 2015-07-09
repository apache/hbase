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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.NonceKey;

/**
 * Once a Procedure completes the ProcedureExecutor takes all the useful
 * information of the procedure (e.g. exception/result) and creates a ProcedureResult.
 * The user of the Procedure framework will get the procedure result with
 * procedureExecutor.getResult(procId)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureResult {
  private final NonceKey nonceKey;
  private final RemoteProcedureException exception;
  private final long lastUpdate;
  private final long startTime;
  private final byte[] result;

  private long clientAckTime = -1;

  public ProcedureResult(
      final NonceKey nonceKey,
      final long startTime,
      final long lastUpdate,
      final RemoteProcedureException exception) {
    this(nonceKey, exception, lastUpdate, startTime, null);
  }

  public ProcedureResult(
      final NonceKey nonceKey,
      final long startTime,
      final long lastUpdate,
      final byte[] result) {
    this(nonceKey, null, lastUpdate, startTime, result);
  }

  public ProcedureResult(
      final NonceKey nonceKey,
      final RemoteProcedureException exception,
      final long lastUpdate,
      final long startTime,
      final byte[] result) {
    this.nonceKey = nonceKey;
    this.exception = exception;
    this.lastUpdate = lastUpdate;
    this.startTime = startTime;
    this.result = result;
  }

  public NonceKey getNonceKey() {
    return nonceKey;
  }

  public boolean isFailed() {
    return exception != null;
  }

  public RemoteProcedureException getException() {
    return exception;
  }

  public boolean hasResultData() {
    return result != null;
  }

  public byte[] getResult() {
    return result;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public long executionTime() {
    return lastUpdate - startTime;
  }

  public boolean hasClientAckTime() {
    return clientAckTime > 0;
  }

  public long getClientAckTime() {
    return clientAckTime;
  }

  @InterfaceAudience.Private
  protected void setClientAckTime(final long timestamp) {
    this.clientAckTime = timestamp;
  }
}