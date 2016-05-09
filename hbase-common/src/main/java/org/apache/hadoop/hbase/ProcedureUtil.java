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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.ForeignExceptionMessage;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.NonceKey;

@InterfaceAudience.Private

/**
 * Helper to convert to/from ProcedureProtos
 */
public class ProcedureUtil {

  private ProcedureUtil() {
  }
  /**
   * @return Convert the current {@link ProcedureInfo} into a Protocol Buffers Procedure
   * instance.
   */
  @InterfaceAudience.Private
  public static ProcedureProtos.Procedure convertToProcedureProto(final ProcedureInfo procInfo) {
    ProcedureProtos.Procedure.Builder builder = ProcedureProtos.Procedure.newBuilder();

    builder.setClassName(procInfo.getProcName());
    builder.setProcId(procInfo.getProcId());
    builder.setStartTime(procInfo.getStartTime());
    builder.setState(ProcedureProtos.ProcedureState.valueOf(procInfo.getProcState().name()));
    builder.setLastUpdate(procInfo.getLastUpdate());

    if (procInfo.hasParentId()) {
      builder.setParentId(procInfo.getParentId());
    }

    if (procInfo.getProcOwner() != null) {
      builder.setOwner(procInfo.getProcOwner());
    }

    if (procInfo.isFailed()) {
      builder.setException(procInfo.getForeignExceptionMessage().getForeignExchangeMessage());
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
    NonceKey nonceKey = null;
    if (procProto.getNonce() != HConstants.NO_NONCE) {
      nonceKey = new NonceKey(procProto.getNonceGroup(), procProto.getNonce());
    }

    return new ProcedureInfo(procProto.getProcId(), procProto.getClassName(), procProto.getOwner(),
        convertToProcedureState(procProto.getState()),
        procProto.hasParentId() ? procProto.getParentId() : -1, nonceKey,
        procProto.hasException() ? new ForeignExceptionMsg(procProto.getException()) : null,
        procProto.getLastUpdate(), procProto.getStartTime(),
        procProto.hasResult() ? procProto.getResult().toByteArray() : null);
  }

  public static ProcedureState convertToProcedureState(ProcedureProtos.ProcedureState state) {
    return ProcedureState.valueOf(state.name());
  }

  public static class ForeignExceptionMsg {
    private ForeignExceptionMessage exception;

    public ForeignExceptionMsg(ForeignExceptionMessage exception) {
      this.exception = exception;
    }

    public ForeignExceptionMessage getForeignExchangeMessage() {
      return this.exception;
    }
  }
}
