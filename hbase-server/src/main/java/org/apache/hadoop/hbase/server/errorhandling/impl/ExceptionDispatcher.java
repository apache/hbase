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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;

/**
 * The dispatcher acts as a central point of control of error handling. Any exceptions from the
 * dispatcher get passed directly to the listeners. Likewise, any errors from the listeners get
 * passed to the dispatcher and then back to any listeners.
 * <p>
 * This is useful, for instance, for informing multiple process in conjunction with an
 * {@link Abortable}
 * <p>
 * This is different than an {@link ExceptionOrchestrator} as it will only propagate an error
 * <i>once</i> to all listeners; its single use, just like an {@link ExceptionSnare}. For example,
 * if an error is passed to <tt>this</tt> then that error will be passed to all listeners, but a
 * second error passed to {@link #receiveError(String, Exception, Object...)} will be ignored. This
 * is particularly useful to help avoid accidentally having infinite loops when passing errors.
 * <p>
 * @param <T> generic exception listener type to update
 * @param <E> Type of {@link Exception} to throw when calling {@link #failOnError()}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ExceptionDispatcher<T, E extends Exception> extends ExceptionOrchestrator<E> implements
    ExceptionListener<E>, ExceptionCheckable<E> {
  private static final Log LOG = LogFactory.getLog(ExceptionDispatcher.class);
  protected final ExceptionVisitor<T> visitor;
  private final ExceptionSnare<E> snare = new ExceptionSnare<E>();

  public ExceptionDispatcher(String name, ExceptionVisitor<T> visitor) {
    super(name);
    this.visitor = visitor;
  }

  public ExceptionDispatcher(ExceptionVisitor<T> visitor) {
    this("single-error-dispatcher", visitor);
  }

  public ExceptionDispatcher() {
    this(null);
  }

  @Override
  public synchronized void receiveError(String message, E e, Object... info) {
    // if we already have an error, then ignore it
    if (snare.checkForError()) return;

    LOG.debug(name.getNamePrefixForLog() + "Accepting received error:" + message);
    // mark that we got the error
    snare.receiveError(message, e, info);

    // notify all the listeners
    super.receiveError(message, e, info);
  }

  @Override
  public void failOnError() throws E {
    snare.failOnError();
  }

  @Override
  public boolean checkForError() {
    return snare.checkForError();
  }

  public ExceptionVisitor<T> getDefaultVisitor() {
    return this.visitor;
  }

  /**
   * Add a typed error listener that will be visited by the {@link ExceptionVisitor}, passed in the
   * constructor, when receiving errors.
   * @param errorable listener for error notifications
   */
  public void addErrorListener(T errorable) {
    if (this.visitor == null) throw new UnsupportedOperationException("No error visitor for "
        + errorable + ", can't add it to the listeners");
    addErrorListener(this.visitor, errorable);
  }
}