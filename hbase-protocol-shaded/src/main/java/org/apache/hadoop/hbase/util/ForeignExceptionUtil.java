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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos.ForeignExceptionMessage;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos.GenericExceptionMessage;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos.StackTraceElementMessage;

/**
 * Helper to convert Exceptions and StackTraces from/to protobuf.
 * (see ErrorHandling.proto for the internal of the proto messages)
 */
@InterfaceAudience.Private
public final class ForeignExceptionUtil {
  private ForeignExceptionUtil() { }

  public static Exception toException(final ForeignExceptionMessage eem) {
    Exception re;
    try {
      re = createException(Exception.class, eem);
    } catch (Throwable e) {
      re = new Exception(eem.getGenericException().getMessage());
    }
    return setExceptionDetails(re, eem);
  }

  public static IOException toIOException(final ForeignExceptionMessage eem) {
    IOException re;
    try {
      re = createException(IOException.class, eem);
    } catch (Throwable e) {
      re = new IOException(eem.getGenericException().getMessage());
    }
    return setExceptionDetails(re, eem);
  }

  private static <T extends Exception> T createException(final Class<T> clazz,
      final ForeignExceptionMessage eem) throws ClassNotFoundException, NoSuchMethodException,
        InstantiationException, IllegalAccessException, InvocationTargetException {
    final GenericExceptionMessage gem = eem.getGenericException();
    final Class<?> realClass = Class.forName(gem.getClassName());
    final Class<? extends T> cls = realClass.asSubclass(clazz);
    final Constructor<? extends T> cn = cls.getConstructor(String.class);
    cn.setAccessible(true);
    return cn.newInstance(gem.getMessage());
  }

  private static <T extends Exception> T setExceptionDetails(final T exception,
      final ForeignExceptionMessage eem) {
    final GenericExceptionMessage gem = eem.getGenericException();
    final StackTraceElement[] trace = toStackTrace(gem.getTraceList());
    exception.setStackTrace(trace);
    return exception;
  }

  public static ForeignExceptionMessage toProtoForeignException(final Throwable t) {
    return toProtoForeignException(null, t);
  }

  public static ForeignExceptionMessage toProtoForeignException(String source, Throwable t) {
    GenericExceptionMessage.Builder gemBuilder = GenericExceptionMessage.newBuilder();
    gemBuilder.setClassName(t.getClass().getName());
    if (t.getMessage() != null) {
      gemBuilder.setMessage(t.getMessage());
    }
    // set the stack trace, if there is one
    List<StackTraceElementMessage> stack = toProtoStackTraceElement(t.getStackTrace());
    if (stack != null) {
      gemBuilder.addAllTrace(stack);
    }
    GenericExceptionMessage payload = gemBuilder.build();
    ForeignExceptionMessage.Builder exception = ForeignExceptionMessage.newBuilder();
    exception.setGenericException(payload);
    if (source != null) {
      exception.setSource(source);
    }

    return exception.build();
  }

  /**
   * Convert a stack trace to list of {@link StackTraceElement}.
   * @param trace the stack trace to convert to protobuf message
   * @return <tt>null</tt> if the passed stack is <tt>null</tt>.
   */
  public static List<StackTraceElementMessage> toProtoStackTraceElement(StackTraceElement[] trace) {
    // if there is no stack trace, ignore it and just return the message
    if (trace == null) {
      return null;
    }

    // build the stack trace for the message
    List<StackTraceElementMessage> pbTrace = new ArrayList<>(trace.length);
    for (StackTraceElement elem : trace) {
      StackTraceElementMessage.Builder stackBuilder = StackTraceElementMessage.newBuilder();
      stackBuilder.setDeclaringClass(elem.getClassName());
      if (elem.getFileName() != null) {
        stackBuilder.setFileName(elem.getFileName());
      }
      stackBuilder.setLineNumber(elem.getLineNumber());
      stackBuilder.setMethodName(elem.getMethodName());
      pbTrace.add(stackBuilder.build());
    }
    return pbTrace;
  }

  /**
   * Unwind a serialized array of {@link StackTraceElementMessage}s to a
   * {@link StackTraceElement}s.
   * @param traceList list that was serialized
   * @return the deserialized list or <tt>null</tt> if it couldn't be unwound (e.g. wasn't set on
   *         the sender).
   */
  public static StackTraceElement[] toStackTrace(List<StackTraceElementMessage> traceList) {
    if (traceList == null || traceList.isEmpty()) {
      return new StackTraceElement[0]; // empty array
    }
    StackTraceElement[] trace = new StackTraceElement[traceList.size()];
    for (int i = 0; i < traceList.size(); i++) {
      StackTraceElementMessage elem = traceList.get(i);
      trace[i] = new StackTraceElement(
          elem.getDeclaringClass(), elem.getMethodName(),
          elem.hasFileName() ? elem.getFileName() : null,
          elem.getLineNumber());
    }
    return trace;
  }
}
