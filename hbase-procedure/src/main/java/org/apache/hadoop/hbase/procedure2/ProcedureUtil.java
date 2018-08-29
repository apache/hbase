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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.Any;
import org.apache.hbase.thirdparty.com.google.protobuf.Internal;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.Parser;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
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
   * A serializer for our Procedures. Instead of the previous serializer, it
   * uses the stateMessage list to store the internal state of the Procedures.
   */
  private static class StateSerializer implements ProcedureStateSerializer {
    private final ProcedureProtos.Procedure.Builder builder;
    private int deserializeIndex;

    public StateSerializer(ProcedureProtos.Procedure.Builder builder) {
      this.builder = builder;
    }

    @Override
    public void serialize(Message message) throws IOException {
      Any packedMessage = Any.pack(message);
      builder.addStateMessage(packedMessage);
    }

    @Override
    public <M extends Message> M deserialize(Class<M> clazz)
        throws IOException {
      if (deserializeIndex >= builder.getStateMessageCount()) {
        throw new IOException("Invalid state message index: " + deserializeIndex);
      }

      try {
        Any packedMessage = builder.getStateMessage(deserializeIndex++);
        return packedMessage.unpack(clazz);
      } catch (InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      }
    }
  }

  /**
   * A serializer (deserializer) for those Procedures which were serialized
   * before this patch. It deserializes the old, binary stateData field.
   */
  private static class CompatStateSerializer implements ProcedureStateSerializer {
    private InputStream inputStream;

    public CompatStateSerializer(InputStream inputStream) {
      this.inputStream = inputStream;
    }

    @Override
    public void serialize(Message message) throws IOException {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends Message> M deserialize(Class<M> clazz)
        throws IOException {
      Parser<M> parser = (Parser<M>) Internal.getDefaultInstance(clazz).getParserForType();
      try {
        return parser.parseDelimitedFrom(inputStream);
      } catch (InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      }
    }
  }

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
      .setSubmittedTime(proc.getSubmittedTime())
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

    ProcedureStateSerializer serializer = new StateSerializer(builder);
    proc.serializeStateData(serializer);

    if (proc.getNonceKey() != null) {
      builder.setNonceGroup(proc.getNonceKey().getNonceGroup());
      builder.setNonce(proc.getNonceKey().getNonce());
    }

    if (proc.hasLock()) {
      builder.setLocked(true);
    }

    if (proc.isBypass()) {
      builder.setBypass(true);
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
    proc.setSubmittedTime(proto.getSubmittedTime());
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
      assert proc.getState() == ProcedureProtos.ProcedureState.FAILED ||
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

    if (proto.getLocked()) {
      proc.lockedWhenLoading();
    }

    if (proto.getBypass()) {
      proc.bypass();
    }

    ProcedureStateSerializer serializer = null;

    if (proto.getStateMessageCount() > 0) {
      serializer = new StateSerializer(proto.toBuilder());
    } else if (proto.hasStateData()) {
      InputStream inputStream = proto.getStateData().newInput();
      serializer = new CompatStateSerializer(inputStream);
    }

    if (serializer != null) {
      proc.deserializeStateData(serializer);
    }

    return proc;
  }

  // ==========================================================================
  //  convert from LockedResource object
  // ==========================================================================

  public static LockServiceProtos.LockedResourceType convertToProtoResourceType(
      LockedResourceType resourceType) {
    return LockServiceProtos.LockedResourceType.valueOf(resourceType.name());
  }

  public static LockServiceProtos.LockType convertToProtoLockType(LockType lockType) {
    return LockServiceProtos.LockType.valueOf(lockType.name());
  }

  public static LockServiceProtos.LockedResource convertToProtoLockedResource(
      LockedResource lockedResource) throws IOException
  {
    LockServiceProtos.LockedResource.Builder builder =
        LockServiceProtos.LockedResource.newBuilder();

    builder
        .setResourceType(convertToProtoResourceType(lockedResource.getResourceType()))
        .setResourceName(lockedResource.getResourceName())
        .setLockType(convertToProtoLockType(lockedResource.getLockType()));

    Procedure<?> exclusiveLockOwnerProcedure = lockedResource.getExclusiveLockOwnerProcedure();

    if (exclusiveLockOwnerProcedure != null) {
      ProcedureProtos.Procedure exclusiveLockOwnerProcedureProto =
          convertToProtoProcedure(exclusiveLockOwnerProcedure);
      builder.setExclusiveLockOwnerProcedure(exclusiveLockOwnerProcedureProto);
    }

    builder.setSharedLockCount(lockedResource.getSharedLockCount());

    for (Procedure<?> waitingProcedure : lockedResource.getWaitingProcedures()) {
      ProcedureProtos.Procedure waitingProcedureProto =
          convertToProtoProcedure(waitingProcedure);
      builder.addWaitingProcedures(waitingProcedureProto);
    }

    return builder.build();
  }
}
