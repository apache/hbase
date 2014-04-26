/**
 * Copyright 2014 The Apache Software Foundation
 *
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * Some utility methods for handling exception.
 */
public class ExceptionUtils {
  /**
   * If t is an RuntimeException throw it directly, or
   * If t is an InvocationTargetException, recursively call toIOException
   * with target exception, or
   * If t is an UndeclaredThrowableException, recursively call toIOException
   * with undeclared throwable, or
   * Otherwise convert it into an IOException. Wrap the exception with an
   * IOException if necessary.
   *
   * DO NOT use it if you want to wrap all exception including RuntimeException
   * into an IOException.
   */
  public static IOException toIOException(Throwable t) {
    if (t instanceof InvocationTargetException) {
      return toIOException(((InvocationTargetException) t)
          .getTargetException());
    }

    if (t instanceof UndeclaredThrowableException) {
      return toIOException(((UndeclaredThrowableException) t)
          .getUndeclaredThrowable());
    }

    if (t instanceof IOException) {
      return (IOException) t;
    }

    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }

    return new IOException(t);
  }

  /**
   * If t is an RuntimeException return it directly, or
   * If t is an InvocationTargetException, recursively call toRuntimeException
   * with target exception, or
   * If t is an UndeclaredThrowableException, recursively call
   * toRuntimeException with undeclared throwable, or
   * Otherwise wrap it into an toRuntimeException.
   */
  public static RuntimeException toRuntimeException(Throwable t) {
    if (t instanceof InvocationTargetException) {
      return toRuntimeException(((InvocationTargetException) t)
          .getTargetException());
    }

    if (t instanceof UndeclaredThrowableException) {
      return toRuntimeException(((UndeclaredThrowableException) t)
          .getUndeclaredThrowable());
    }

    if (t instanceof RuntimeException) {
      return (RuntimeException) t;
    }

    return new RuntimeException(t);
  }
}
