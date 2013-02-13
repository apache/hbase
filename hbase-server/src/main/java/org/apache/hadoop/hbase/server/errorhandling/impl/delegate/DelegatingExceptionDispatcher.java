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
package org.apache.hadoop.hbase.server.errorhandling.impl.delegate;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionDispatcher;

/**
 * Helper class for exception handler factories.
 * @param <D> Type of delegate to use
 * @param <T> type of generic error listener to update
 * @param <E> exception to expect for errors
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DelegatingExceptionDispatcher<D extends ExceptionDispatcher<T, E>, T, E extends Exception>
    extends ExceptionDispatcher<T, E> {

  protected final D delegate;
  public DelegatingExceptionDispatcher(D delegate) {
    super("delegate - " + delegate.getName(), delegate.getDefaultVisitor());
    this.delegate = delegate;
  }

  @Override
  public ExceptionVisitor<T> getDefaultVisitor() {
    return delegate.getDefaultVisitor();
  }

  @Override
  public void receiveError(String message, E e, Object... info) {
    delegate.receiveError(message, e, info);
  }

  @Override
  public <L> void addErrorListener(ExceptionVisitor<L> visitor, L errorable) {
    delegate.addErrorListener(visitor, errorable);
  }

  @Override
  public void failOnError() throws E {
    delegate.failOnError();
  }

  @Override
  public boolean checkForError() {
    return delegate.checkForError();
  }

  @Override
  public void addErrorListener(T errorable) {
    delegate.addErrorListener(errorable);
  }
}
