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
package org.apache.hadoop.hbase.errorhandling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos.ForeignExceptionMessage;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos.GenericExceptionMessage;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos.StackTraceElementMessage;


/**
 * A ForeignException is an exception from another thread or process.
 * <p>
 * ForeignExceptions are sent to 'remote' peers to signal an abort in the face of failures.
 * When serialized for transmission we encode using Protobufs to ensure version compatibility.
 * <p>
 * Foreign exceptions contain a Throwable as its cause.  This can be a "regular" exception
 * generated locally or a ProxyThrowable that is a representation of the original exception
 * created on original 'remote' source.  These ProxyThrowables have their their stacks traces and
 * messages overridden to reflect the original 'remote' exception.  The only way these
 * ProxyThrowables are generated are by this class's {@link #deserialize(byte[])} method.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class ForeignException extends IOException {

  /**
   * Name of the throwable's source such as a host or thread name.  Must be non-null.
   */
  private final String source;

  /**
   * Create a new ForeignException that can be serialized.  It is assumed that this came form a
   * local source.
   * @param source
   * @param cause
   */
  public ForeignException(String source, Throwable cause) {
    super(cause);
    assert source != null;
    assert cause != null;
    this.source = source;
  }

  /**
   * Create a new ForeignException that can be serialized.  It is assumed that this is locally
   * generated.
   * @param source
   * @param msg
   */
  public ForeignException(String source, String msg) {
    super(new IllegalArgumentException(msg));
    this.source = source;
  }

  public String getSource() {
    return source;
  }

  /**
   * The cause of a ForeignException can be an exception that was generated on a local in process
   * thread, or a thread from a 'remote' separate process.
   *
   * If the cause is a ProxyThrowable, we know it came from deserialization which usually means
   * it came from not only another thread, but also from a remote thread.
   *
   * @return true if went through deserialization, false if locally generated
   */
  public boolean isRemote() {
    return getCause() instanceof ProxyThrowable;
  }

  @Override
  public String toString() {
    String className = getCause().getClass().getName()  ;
    return className + " via " + getSource() + ":" + getLocalizedMessage();
  }

  /**
   * Convert a stack trace to list of {@link StackTraceElement}.
   * @param trace the stack trace to convert to protobuf message
   * @return <tt>null</tt> if the passed stack is <tt>null</tt>.
   */
  private static List<StackTraceElementMessage> toStackTraceElementMessages(
      StackTraceElement[] trace) {
    // if there is no stack trace, ignore it and just return the message
    if (trace == null) return null;
    // build the stack trace for the message
    List<StackTraceElementMessage> pbTrace =
        new ArrayList<StackTraceElementMessage>(trace.length);
    for (StackTraceElement elem : trace) {
      StackTraceElementMessage.Builder stackBuilder = StackTraceElementMessage.newBuilder();
      stackBuilder.setDeclaringClass(elem.getClassName());
      stackBuilder.setFileName(elem.getFileName());
      stackBuilder.setLineNumber(elem.getLineNumber());
      stackBuilder.setMethodName(elem.getMethodName());
      pbTrace.add(stackBuilder.build());
    }
    return pbTrace;
  }

  /**
   * This is a Proxy Throwable that contains the information of the original remote exception
   */
  private static class ProxyThrowable extends Throwable {
    ProxyThrowable(String msg, StackTraceElement[] trace) {
      super(msg);
      this.setStackTrace(trace);
    }
  }

  /**
   * Converts a ForeignException to an array of bytes.
   * @param source the name of the external exception source
   * @param t the "local" external exception (local)
   * @return protobuf serialized version of ForeignException
   */
  public static byte[] serialize(String source, Throwable t) {
    GenericExceptionMessage.Builder gemBuilder = GenericExceptionMessage.newBuilder();
    gemBuilder.setClassName(t.getClass().getName());
    if (t.getMessage() != null) {
      gemBuilder.setMessage(t.getMessage());
    }
    // set the stack trace, if there is one
    List<StackTraceElementMessage> stack =
        ForeignException.toStackTraceElementMessages(t.getStackTrace());
    if (stack != null) {
      gemBuilder.addAllTrace(stack);
    }
    GenericExceptionMessage payload = gemBuilder.build();
    ForeignExceptionMessage.Builder exception = ForeignExceptionMessage.newBuilder();
    exception.setGenericException(payload).setSource(source);
    ForeignExceptionMessage eem = exception.build();
    return eem.toByteArray();
  }

  /**
   * Takes a series of bytes and tries to generate an ForeignException instance for it.
   * @param bytes
   * @return the ForeignExcpetion instance
   * @throws InvalidProtocolBufferException if there was deserialization problem this is thrown.
   * @throws org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException 
   */
  public static ForeignException deserialize(byte[] bytes)
  throws IOException {
    // figure out the data we need to pass
    ForeignExceptionMessage eem = ForeignExceptionMessage.parseFrom(bytes);
    GenericExceptionMessage gem = eem.getGenericException();
    StackTraceElement [] trace = ForeignException.toStackTrace(gem.getTraceList());
    ProxyThrowable dfe = new ProxyThrowable(gem.getMessage(), trace);
    ForeignException e = new ForeignException(eem.getSource(), dfe);
    return e;
  }

  /**
   * Unwind a serialized array of {@link StackTraceElementMessage}s to a
   * {@link StackTraceElement}s.
   * @param traceList list that was serialized
   * @return the deserialized list or <tt>null</tt> if it couldn't be unwound (e.g. wasn't set on
   *         the sender).
   */
  private static StackTraceElement[] toStackTrace(List<StackTraceElementMessage> traceList) {
    if (traceList == null || traceList.size() == 0) {
      return new StackTraceElement[0]; // empty array
    }
    StackTraceElement[] trace = new StackTraceElement[traceList.size()];
    for (int i = 0; i < traceList.size(); i++) {
      StackTraceElementMessage elem = traceList.get(i);
      trace[i] = new StackTraceElement(
          elem.getDeclaringClass(), elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
    }
    return trace;
  }
}
