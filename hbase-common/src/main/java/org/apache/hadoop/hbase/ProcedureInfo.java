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

package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.util.StringUtils;

/**
 * Procedure information
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProcedureInfo implements Cloneable {
  private final long procId;
  private final String procName;
  private final String procOwner;
  private final ProcedureState procState;
  private final long parentId;
  private final NonceKey nonceKey;
  private final ProcedureUtil.ForeignExceptionMsg exception;
  private final long lastUpdate;
  private final long startTime;
  private final byte[] result;

  private long clientAckTime = -1;

  @InterfaceAudience.Private
  public ProcedureInfo(
      final long procId,
      final String procName,
      final String procOwner,
      final ProcedureState procState,
      final long parentId,
      final NonceKey nonceKey,
      final ProcedureUtil.ForeignExceptionMsg exception,
      final long lastUpdate,
      final long startTime,
      final byte[] result) {
    this.procId = procId;
    this.procName = procName;
    this.procOwner = procOwner;
    this.procState = procState;
    this.parentId = parentId;
    this.nonceKey = nonceKey;
    this.lastUpdate = lastUpdate;
    this.startTime = startTime;

    // If the procedure is completed, we should treat exception and result differently
    this.exception = exception;
    this.result = result;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="CN_IDIOM_NO_SUPER_CALL",
      justification="Intentional; calling super class clone doesn't make sense here.")
  public ProcedureInfo clone() {
    return new ProcedureInfo(procId, procName, procOwner, procState, parentId, nonceKey,
      exception, lastUpdate, startTime, result);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Procedure=");
    sb.append(procName);
    sb.append(" (id=");
    sb.append(procId);
    if (hasParentId()) {
      sb.append(", parent=");
      sb.append(parentId);
    }
    if (hasOwner()) {
      sb.append(", owner=");
      sb.append(procOwner);
    }
    sb.append(", state=");
    sb.append(procState);

    long now = EnvironmentEdgeManager.currentTime();
    sb.append(", startTime=");
    sb.append(StringUtils.formatTime(now - startTime));
    sb.append(" ago, lastUpdate=");
    sb.append(StringUtils.formatTime(now - startTime));
    sb.append(" ago");

    if (isFailed()) {
      sb.append(", exception=\"");
      sb.append(getExceptionMessage());
      sb.append("\"");
    }
    sb.append(")");
    return sb.toString();
  }

  public long getProcId() {
    return procId;
  }

  public String getProcName() {
    return procName;
  }

  private boolean hasOwner() {
    return procOwner != null;
  }

  public String getProcOwner() {
    return procOwner;
  }

  public ProcedureState getProcState() {
    return procState;
  }

  public boolean hasParentId() {
    return (parentId != -1);
  }

  public long getParentId() {
    return parentId;
  }

  public NonceKey getNonceKey() {
    return nonceKey;
  }

  public boolean isFailed() {
    return exception != null;
  }

  public IOException getException() {
    if (isFailed()) {
      return ForeignExceptionUtil.toIOException(exception.getForeignExchangeMessage());
    }
    return null;
  }

  @InterfaceAudience.Private
  public ProcedureUtil.ForeignExceptionMsg getForeignExceptionMessage() {
    return exception;
  }

  public String getExceptionCause() {
    assert isFailed();
    return exception.getForeignExchangeMessage().getGenericException().getClassName();
  }

  public String getExceptionMessage() {
    assert isFailed();
    return exception.getForeignExchangeMessage().getGenericException().getMessage();
  }

  public String getExceptionFullMessage() {
    assert isFailed();
    return getExceptionCause() + " - " + getExceptionMessage();
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

  @InterfaceAudience.Private
  public boolean hasClientAckTime() {
    return clientAckTime != -1;
  }

  @InterfaceAudience.Private
  public long getClientAckTime() {
    return clientAckTime;
  }

  @InterfaceAudience.Private
  public void setClientAckTime(final long timestamp) {
    this.clientAckTime = timestamp;
  }

  /**
   * Check if the user is this procedure's owner
   * @param procInfo the procedure to check
   * @param user the user
   * @return true if the user is the owner of the procedure,
   *   false otherwise or the owner is unknown.
   */
  @InterfaceAudience.Private
  public static boolean isProcedureOwner(final ProcedureInfo procInfo, final User user) {
    if (user == null) {
      return false;
    }
    String procOwner = procInfo.getProcOwner();
    if (procOwner == null) {
      return false;
    }
    return procOwner.equals(user.getShortName());
  }

}