/**
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

import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This class handles the different interruption classes.
 * It can be:
 * - InterruptedException
 * - InterruptedIOException (inherits IOException); used in IO
 * - ClosedByInterruptException (inherits IOException)
 * , - SocketTimeoutException inherits InterruptedIOException but is not a real
 * interruption, so we have to distinguish the case. This pattern is unfortunately common.
 */
@InterfaceAudience.Private
public class ExceptionUtil {

  /**
   * @return true if the throwable comes an interruption, false otherwise.
   */
  public static boolean isInterrupt(Throwable t) {
    if (t instanceof InterruptedException) return true;
    if (t instanceof SocketTimeoutException) return false;
    return (t instanceof InterruptedIOException);
  }

  /**
   * @throws InterruptedIOException if t was an interruption. Does nothing otherwise.
   */
  public static void rethrowIfInterrupt(Throwable t) throws InterruptedIOException {
    InterruptedIOException iie = asInterrupt(t);
    if (iie != null) throw iie;
  }

  /**
   * @return an InterruptedIOException if t was an interruption, null otherwise
   */
  public static InterruptedIOException asInterrupt(Throwable t) {
    if (t instanceof SocketTimeoutException) return null;

    if (t instanceof InterruptedIOException) return (InterruptedIOException) t;

    if (t instanceof InterruptedException) {
      InterruptedIOException iie = new InterruptedIOException();
      iie.initCause(t);
      return iie;
    }

    return null;
  }

  private ExceptionUtil() {
  }
}
