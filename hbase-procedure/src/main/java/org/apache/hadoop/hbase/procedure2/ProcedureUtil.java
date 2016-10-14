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

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ProcedureState;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.NonceKey;

/**
 * Helper to convert to/from ProcedureProtos
 */
@InterfaceAudience.Private
public final class ProcedureUtil {
  private ProcedureUtil() { }

  // ==========================================================================
  //  Reflection helpers to create/validate a Procedure object
  // ==========================================================================
  public static Procedure newProcedure(final String className) throws BadProcedureException {
    try {
      final Class<?> clazz = Class.forName(className);
      if (!Modifier.isPublic(clazz.getModifiers())) {
        throw new Exception("the " + clazz + " class is not public");
      }

      final Constructor<?> ctor = clazz.getConstructor();
      assert ctor != null : "no constructor found";
      if (!Modifier.isPublic(ctor.getModifiers())) {
        throw new Exception("the " + clazz + " constructor is not public");
      }
      return (Procedure)ctor.newInstance();
    } catch (Exception e) {
      throw new BadProcedureException("The procedure class " + className +
          " must be accessible and have an empty constructor", e);
    }
  }

  public static void validateClass(final Procedure proc) throws BadProcedureException {
    try {
      final Class<?> clazz = proc.getClass();
      if (!Modifier.isPublic(clazz.getModifiers())) {
        throw new Exception("the " + clazz + " class is not public");
      }

      final Constructor<?> ctor = clazz.getConstructor();
      assert ctor != null;
      if (!Modifier.isPublic(ctor.getModifiers())) {
        throw new Exception("the " + clazz + " constructor is not public");
      }
    } catch (Exception e) {
      throw new BadProcedureException("The procedure class " + proc.getClass().getName() +
          " must be accessible and have an empty constructor", e);
    }
  }

  // ==========================================================================
  //  convert to and from Procedure object
  // ==========================================================================

  /**
   * Helper to convert the procedure to protobuf.
   * Used by ProcedureStore implementations.
   */
  public static ProcedureProtos.Procedure convertToProtoProcedure(final Procedure proc)
      throws IOException {
    Preconditions.checkArgument(proc != null);
    validateClass(proc);

    final ProcedureProtos.Procedure.Builder builder = ProcedureProtos.Procedure.newBuilder()
      .setClassName(proc.getClass().getName())
      .setProcId(proc.getProcId())
      .setState(proc.getState())
      .setStartTime(proc.getStartTime())
      .setLastUpdate(proc.getLastUpdate());

    if (proc.hasParent()) {
      builder.setParentId(proc.getParentProcId());
    }

    if (proc.hasTimeout()) {
      builder.setTimeout(proc.getTimeout());
    }

    if (proc.hasOwner()) {
      builder.setOwner(proc.getOwner());
    }

    final int[] stackIds = proc.getStackIndexes();
    if (stackIds != null) {
      for (int i = 0; i < stackIds.length; ++i) {
        builder.addStackId(stackIds[i]);
      }
    }

    if (proc.hasException()) {
      RemoteProcedureException exception = proc.getException();
      builder.setException(
        RemoteProcedureException.toProto(exception.getSource(), exception.getCause()));
    }

    final byte[] result = proc.getResult();
    if (result != null) {
      builder.setResult(UnsafeByteOperations.unsafeWrap(result));
    }

    final ByteString.Output stateStream = ByteString.newOutput();
    try {
      proc.serializeStateData(stateStream);
      if (stateStream.size() > 0) {
        builder.setStateData(stateStream.toByteString());
      }
    } finally {
      stateStream.close();
    }

    if (proc.getNonceKey() != null) {
      builder.setNonceGroup(proc.getNonceKey().getNonceGroup());
      builder.setNonce(proc.getNonceKey().getNonce());
    }

    return builder.build();
  }

  /**
   * Helper to convert the protobuf procedure.
   * Used by ProcedureStore implementations.
   *
   * TODO: OPTIMIZATION: some of the field never change during the execution
   *                     (e.g. className, procId, parentId, ...).
   *                     We can split in 'data' and 'state', and the store
   *                     may take advantage of it by storing the data only on insert().
   */
  public static Procedure convertToProcedure(final ProcedureProtos.Procedure proto) throws IOException {
    // Procedure from class name
    final Procedure proc = newProcedure(proto.getClassName());

    // set fields
    proc.setProcId(proto.getProcId());
    proc.setState(proto.getState());
    proc.setStartTime(proto.getStartTime());
    proc.setLastUpdate(proto.getLastUpdate());

    if (proto.hasParentId()) {
      proc.setParentProcId(proto.getParentId());
    }

    if (proto.hasOwner()) {
      proc.setOwner(proto.getOwner());
    }

    if (proto.hasTimeout()) {
      proc.setTimeout(proto.getTimeout());
    }

    if (proto.getStackIdCount() > 0) {
      proc.setStackIndexes(proto.getStackIdList());
    }

    if (proto.hasException()) {
      assert proc.getState() == ProcedureProtos.ProcedureState.FINISHED ||
             proc.getState() == ProcedureProtos.ProcedureState.ROLLEDBACK :
             "The procedure must be failed (waiting to rollback) or rolledback";
      proc.setFailure(RemoteProcedureException.fromProto(proto.getException()));
    }

    if (proto.hasResult()) {
      proc.setResult(proto.getResult().toByteArray());
    }

    if (proto.getNonce() != HConstants.NO_NONCE) {
      proc.setNonceKey(new NonceKey(proto.getNonceGroup(), proto.getNonce()));
    }

    // we want to call deserialize even when the stream is empty, mainly for testing.
    proc.deserializeStateData(proto.getStateData().newInput());

    return proc;
  }

  // ==========================================================================
  //  convert to and from ProcedureInfo object
  // ==========================================================================

  /**
   * @return Convert the current {@link ProcedureInfo} into a Protocol Buffers Procedure
   * instance.
   */
  public static ProcedureProtos.Procedure convertToProtoProcedure(final ProcedureInfo procInfo) {
    final ProcedureProtos.Procedure.Builder builder = ProcedureProtos.Procedure.newBuilder();

    builder.setClassName(procInfo.getProcName());
    builder.setProcId(procInfo.getProcId());
    builder.setStartTime(procInfo.getStartTime());
    builder.setState(ProcedureProtos.ProcedureState.valueOf(procInfo.getProcState().name()));
    builder.setLastUpdate(procInfo.getLastUpdate());

    if (procInfo.hasParentId()) {
      builder.setParentId(procInfo.getParentId());
    }

    if (procInfo.hasOwner()) {
      builder.setOwner(procInfo.getProcOwner());
    }

    if (procInfo.isFailed()) {
      builder.setException(ForeignExceptionUtil.toProtoForeignException(procInfo.getException()));
    }

    if (procInfo.hasResultData()) {
      builder.setResult(UnsafeByteOperations.unsafeWrap(procInfo.getResult()));
    }

    return builder.build();
  }

  /**
   * Helper to convert the protobuf object.
   * @return Convert the current Protocol Buffers Procedure to {@link ProcedureInfo}
   * instance.
   */
  public static ProcedureInfo convertToProcedureInfo(final ProcedureProtos.Procedure procProto) {
    NonceKey nonceKey = null;
    if (procProto.getNonce() != HConstants.NO_NONCE) {
      nonceKey = new NonceKey(procProto.getNonceGroup(), procProto.getNonce());
    }

    return new ProcedureInfo(procProto.getProcId(), procProto.getClassName(),
        procProto.hasOwner() ? procProto.getOwner() : null,
        convertToProcedureState(procProto.getState()),
        procProto.hasParentId() ? procProto.getParentId() : -1, nonceKey,
        procProto.hasException() ?
          ForeignExceptionUtil.toIOException(procProto.getException()) : null,
        procProto.getLastUpdate(), procProto.getStartTime(),
        procProto.hasResult() ? procProto.getResult().toByteArray() : null);
  }

  public static ProcedureState convertToProcedureState(ProcedureProtos.ProcedureState state) {
    return ProcedureState.valueOf(state.name());
  }

  public static ProcedureInfo convertToProcedureInfo(final Procedure proc) {
    return convertToProcedureInfo(proc, null);
  }

  /**
   * Helper to create the ProcedureInfo from Procedure.
   */
  public static ProcedureInfo convertToProcedureInfo(final Procedure proc,
      final NonceKey nonceKey) {
    final RemoteProcedureException exception = proc.hasException() ? proc.getException() : null;
    return new ProcedureInfo(proc.getProcId(), proc.toStringClass(), proc.getOwner(),
        convertToProcedureState(proc.getState()),
        proc.hasParent() ? proc.getParentProcId() : -1, nonceKey,
        exception != null ? exception.unwrapRemoteIOException() : null,
        proc.getLastUpdate(), proc.getStartTime(), proc.getResult());
  }
}