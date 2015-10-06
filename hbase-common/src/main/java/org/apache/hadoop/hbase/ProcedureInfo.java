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
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.ForeignExceptionMessage;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.NonceKey;

/**
 * Procedure information
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProcedureInfo {
  private final long procId;
  private final String procName;
  private final String procOwner;
  private final ProcedureState procState;
  private final long parentId;
  private final ForeignExceptionMessage exception;
  private final long lastUpdate;
  private final long startTime;
  private final byte[] result;

  private NonceKey nonceKey = null;
  private long clientAckTime = -1;

  public ProcedureInfo(
      final long procId,
      final String procName,
      final String procOwner,
      final ProcedureState procState,
      final long parentId,
      final ForeignExceptionMessage exception,
      final long lastUpdate,
      final long startTime,
      final byte[] result) {
    this.procId = procId;
    this.procName = procName;
    this.procOwner = procOwner;
    this.procState = procState;
    this.parentId = parentId;
    this.lastUpdate = lastUpdate;
    this.startTime = startTime;

    // If the procedure is completed, we should treat exception and result differently
    this.exception = exception;
    this.result = result;
  }

  public ProcedureInfo clone() {
    return new ProcedureInfo(
      procId, procName, procOwner, procState, parentId, exception, lastUpdate, startTime, result);
  }

  public long getProcId() {
    return procId;
  }

  public String getProcName() {
    return procName;
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

  public void setNonceKey(NonceKey nonceKey) {
    this.nonceKey = nonceKey;
  }

  public boolean isFailed() {
    return exception != null;
  }

  public IOException getException() {
    if (isFailed()) {
      return ForeignExceptionUtil.toIOException(exception);
    }
    return null;
  }

  @InterfaceAudience.Private
  public ForeignExceptionMessage getForeignExceptionMessage() {
    return exception;
  }

  public String getExceptionCause() {
    assert isFailed();
    return exception.getGenericException().getClassName();
  }

  public String getExceptionMessage() {
    assert isFailed();
    return exception.getGenericException().getMessage();
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
    return clientAckTime > 0;
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
   * @return Convert the current {@link ProcedureInfo} into a Protocol Buffers Procedure
   * instance.
   */
  @InterfaceAudience.Private
  public static ProcedureProtos.Procedure convertToProcedureProto(
      final ProcedureInfo procInfo) {
    ProcedureProtos.Procedure.Builder builder = ProcedureProtos.Procedure.newBuilder();

    builder.setClassName(procInfo.getProcName());
    builder.setProcId(procInfo.getProcId());
    builder.setStartTime(procInfo.getStartTime());
    builder.setState(procInfo.getProcState());
    builder.setLastUpdate(procInfo.getLastUpdate());

    if (procInfo.hasParentId()) {
      builder.setParentId(procInfo.getParentId());
    }

    if (procInfo.getProcOwner() != null) {
       builder.setOwner(procInfo.getProcOwner());
    }

    if (procInfo.isFailed()) {
        builder.setException(procInfo.getForeignExceptionMessage());
    }

    if (procInfo.hasResultData()) {
      builder.setResult(ByteStringer.wrap(procInfo.getResult()));
    }

    return builder.build();
  }

  /**
   * Helper to convert the protobuf object.
   * @return Convert the current Protocol Buffers Procedure to {@link ProcedureInfo}
   * instance.
   */
  @InterfaceAudience.Private
  public static ProcedureInfo convert(final ProcedureProtos.Procedure procProto) {
    return new ProcedureInfo(
      procProto.getProcId(),
      procProto.getClassName(),
      procProto.getOwner(),
      procProto.getState(),
      procProto.hasParentId() ? procProto.getParentId() : -1,
          procProto.getState() == ProcedureState.ROLLEDBACK ? procProto.getException() : null,
      procProto.getLastUpdate(),
      procProto.getStartTime(),
      procProto.getState() == ProcedureState.FINISHED ? procProto.getResult().toByteArray() : null);
  }

  /**
  * Check if the user is this procedure's owner
  * @param owner the owner field of the procedure
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