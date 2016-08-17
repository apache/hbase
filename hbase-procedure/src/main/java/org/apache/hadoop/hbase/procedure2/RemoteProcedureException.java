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

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.ForeignExceptionMessage;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RemoteProcedureException is an exception from another thread or process.
 * <p>
 * RemoteProcedureExceptions are sent to 'remote' peers to signal an abort in the face of failures.
 * When serialized for transmission we encode using Protobufs to ensure version compatibility.
 * <p>
 * RemoteProcedureException exceptions contain a Throwable as its cause.
 * This can be a "regular" exception generated locally or a ProxyThrowable that is a representation
 * of the original exception created on original 'remote' source.  These ProxyThrowables have their
 * their stacks traces and messages overridden to reflect the original 'remote' exception.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class RemoteProcedureException extends ProcedureException {

  /**
   * Name of the throwable's source such as a host or thread name.  Must be non-null.
   */
  private final String source;

  /**
   * Create a new RemoteProcedureException that can be serialized.
   * It is assumed that this came form a local source.
   * @param source
   * @param cause
   */
  public RemoteProcedureException(String source, Throwable cause) {
    super(cause);
    assert source != null;
    assert cause != null;
    this.source = source;
  }

  public String getSource() {
    return source;
  }

  public IOException unwrapRemoteException() {
    if (getCause() instanceof RemoteException) {
      return ((RemoteException)getCause()).unwrapRemoteException();
    }
    if (getCause() instanceof IOException) {
      return (IOException)getCause();
    }
    return new IOException(getCause());
  }

  @Override
  public String toString() {
    String className = getCause().getClass().getName();
    return className + " via " + getSource() + ":" + getLocalizedMessage();
  }

  /**
   * Converts a RemoteProcedureException to an array of bytes.
   * @param source the name of the external exception source
   * @param t the "local" external exception (local)
   * @return protobuf serialized version of RemoteProcedureException
   */
  public static byte[] serialize(String source, Throwable t) {
    return toProto(source, t).toByteArray();
  }

  /**
   * Takes a series of bytes and tries to generate an RemoteProcedureException instance for it.
   * @param bytes
   * @return the ForeignExcpetion instance
   * @throws InvalidProtocolBufferException if there was deserialization problem this is thrown.
   */
  public static RemoteProcedureException deserialize(byte[] bytes)
      throws InvalidProtocolBufferException {
    return fromProto(ForeignExceptionMessage.parseFrom(bytes));
  }

  public ForeignExceptionMessage convert() {
    return ForeignExceptionUtil.toProtoForeignException(getSource(), getCause());
  }

  public static ForeignExceptionMessage toProto(String source, Throwable t) {
    return ForeignExceptionUtil.toProtoForeignException(source, t);
  }

  public static RemoteProcedureException fromProto(final ForeignExceptionMessage eem) {
    return new RemoteProcedureException(eem.getSource(), ForeignExceptionUtil.toException(eem));
  }
}
