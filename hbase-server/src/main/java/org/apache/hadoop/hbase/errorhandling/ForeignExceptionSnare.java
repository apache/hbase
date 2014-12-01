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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This is an interface for a cooperative exception throwing mechanism.  Implementations are
 * containers that holds an exception from a separate thread. This can be used to receive
 * exceptions from 'foreign' threads or from separate 'foreign' processes.
 * <p>
 * To use, one would pass an implementation of this object to a long running method and
 * periodically check by calling {@link #rethrowException()}.  If any foreign exceptions have
 * been received, the calling thread is then responsible for handling the rethrown exception.
 * <p>
 * One could use the boolean {@link #hasException()} to determine if there is an exceptoin as well.
 * <p>
 * NOTE: This is very similar to the InterruptedException/interrupt/interrupted pattern.  There,
 * the notification state is bound to a Thread.  Using this, applications receive Exceptions in
 * the snare.  The snare is referenced and checked by multiple threads which enables exception 
 * notification in all the involved threads/processes. 
 */
@InterfaceAudience.Private
public interface ForeignExceptionSnare {

  /**
   * Rethrow an exception currently held by the {@link ForeignExceptionSnare}. If there is
   * no exception this is a no-op
   *
   * @throws ForeignException
   *           all exceptions from remote sources are procedure exceptions
   */
  void rethrowException() throws ForeignException;

  /**
   * Non-exceptional form of {@link #rethrowException()}. Checks to see if any
   * process to which the exception checkers is bound has created an error that
   * would cause a failure.
   *
   * @return <tt>true</tt> if there has been an error,<tt>false</tt> otherwise
   */
  boolean hasException();

  /**
   * Get the value of the captured exception.
   *
   * @return the captured foreign exception or null if no exception captured.
   */
  ForeignException getException();
}
