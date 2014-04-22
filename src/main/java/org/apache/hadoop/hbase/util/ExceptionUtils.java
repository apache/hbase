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

/**
 * Some utility methods for handling exception.
 */
public class ExceptionUtils {
  /**
   * Throws an arbitrary exception to an IOException. Wrap the exception with
   * an IOException if the exception cannot be thrown directly.
   */
  public static void throwIOExcetion(Throwable t)
      throws IOException {
    if (t instanceof IOException) {
      throw (IOException) t;
    }

    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }

    throw new IOException(t);
  }
}
