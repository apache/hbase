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
package org.apache.hadoop.hbase.server.errorhandling.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.FaultInjector;
import org.apache.hadoop.hbase.server.errorhandling.impl.delegate.DelegatingExceptionDispatcher;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.jasper.compiler.ErrorDispatcher;

/**
 * {@link ErrorDispatcher} that delegates calls for all methods, but wraps exception checking to
 * allow the fault injectors to have a chance to inject a fault into the running process
 * @param <D> {@link ExceptionOrchestrator} to wrap for fault checking
 * @param <T> type of generic error listener that should be notified
 * @param <E> exception to be thrown on checks of {@link #failOnError()}
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InjectingExceptionDispatcher<D extends ExceptionDispatcher<T, E>, T, E extends Exception> extends
    DelegatingExceptionDispatcher<D, T, E> {

  private final List<FaultInjector<E>> faults;

  /**
   * Wrap an exception handler with one that will inject faults on calls to {@link #checkForError()}
   * .
   * @param delegate base exception handler to wrap
   * @param faults injectors to run each time there is a check for an error
   */
  @SuppressWarnings("unchecked")
  public InjectingExceptionDispatcher(D delegate, List<FaultInjector<?>> faults) {
    super(delegate);
    // since we don't know the type of fault injector, we need to convert it.
    // this is only used in tests, so throwing a class-cast here isn't too bad.
    this.faults = new ArrayList<FaultInjector<E>>(faults.size());
    for (FaultInjector<?> fault : faults) {
      this.faults.add((FaultInjector<E>) fault);
    }
  }

  @Override
  public void failOnError() throws E {
    // first fail if there is already an error
    delegate.failOnError();
    // then check for an error via the update mechanism
    if (this.checkForError()) delegate.failOnError();
  }

  /**
   * Use the injectors to possibly inject an error into the delegate. Should call
   * {@link ExceptionCheckable#checkForError()} or {@link ExceptionCheckable#failOnError()} after calling
   * this method on return of <tt>true</tt>.
   * @return <tt>true</tt> if an error found via injector or in the delegate, <tt>false</tt>
   *         otherwise
   */
  @Override
  public boolean checkForError() {
    // if there are fault injectors, run them
    if (faults.size() > 0) {
      // get the caller of this method. Should be the direct calling class
      StackTraceElement[] trace = Thread.currentThread().getStackTrace();
      for (FaultInjector<E> injector : faults) {
        Pair<E, Object[]> info = injector.injectFault(trace);
        if (info != null) {
          delegate.receiveError("Injected fail", info.getFirst(), info.getSecond());
        }
      }
    }
    return delegate.checkForError();
  }
}