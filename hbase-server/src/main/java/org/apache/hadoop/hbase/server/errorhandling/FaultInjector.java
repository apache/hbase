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
package org.apache.hadoop.hbase.server.errorhandling;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestratorFactory;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Inject faults when classes check to see if an error occurs.
 * <p>
 * Can be added to any monitoring via
 * {@link ExceptionOrchestratorFactory#addFaultInjector(FaultInjector)}
 * @see ExceptionListener
 * @see ExceptionCheckable
 * @param <E> Type of exception that the corresponding {@link ExceptionListener} is expecting
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FaultInjector<E extends Exception> {

  /**
   * Called by the specified class whenever checking for process errors. Care needs to be taken when
   * using fault injectors to pass the correct size array back or the received error in the listener
   * could not receive the correct number of argument and throw an error.
   * <p>
   * Note that every time the fault injector is called it does not necessarily need to inject a
   * fault, but only when the fault is desired.
   * @param trace full stack trace of the call to check for an error
   * @return the information about the fault that should be returned if there was a fault (expected
   *         exception to throw and generic error information) or <tt>null</tt> if no fault should
   *         be injected.
   */
  public Pair<E, Object[]> injectFault(StackTraceElement[] trace);
}