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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.errorhandling.Name;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * The orchestrator acts as a central point of control of error handling. Any exceptions passed to
 * <tt>this</tt> get passed directly to the listeners.
 * <p>
 * Any exception listener added will only be <b>weakly referenced</b>, so you must keep a reference
 * to it if you want to use it other places. This allows minimal effort error monitoring, allowing
 * you to register an error listener and then not worry about having to unregister the listener.
 * <p>
 * A single {@link ExceptionOrchestrator} should be used for each set of operation attempts (e.g.
 * one parent operation with child operations, potentially multiple levels deep) to monitor. This
 * allows for a single source of truth for exception dispatch between all the interested operation
 * attempts.
 * @param <E> Type of {@link Exception} to expect when receiving errors
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ExceptionOrchestrator<E extends Exception> implements ExceptionListener<E> {

  private static final Log LOG = LogFactory.getLog(ExceptionOrchestrator.class);
  protected final Name name;

  protected final ListMultimap<ExceptionVisitor<?>, WeakReference<?>> listeners = ArrayListMultimap
      .create();

  /** Error visitor for framework listeners */
  final ForwardingErrorVisitor genericVisitor = new ForwardingErrorVisitor();

  public ExceptionOrchestrator() {
    this("generic-error-dispatcher");
  }

  public ExceptionOrchestrator(String name) {
    this.name = new Name(name);
  }

  public Name getName() {
    return this.name;
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public synchronized void receiveError(String message, E e, Object... info) {
    // update all the listeners with the passed error
    LOG.debug(name.getNamePrefixForLog() + " Recieved error, notifying listeners...");
    List<Pair<ExceptionVisitor<?>, WeakReference<?>>> toRemove = new ArrayList<Pair<ExceptionVisitor<?>, WeakReference<?>>>();
    for (Entry<ExceptionVisitor<?>, WeakReference<?>> entry : listeners.entries()) {
      Object o = entry.getValue().get();
      if (o == null) {
        // if the listener doesn't have a reference, then drop it from the list
        // need to copy this over b/c guava is finicky with the entries
        toRemove.add(new Pair<ExceptionVisitor<?>, WeakReference<?>>(entry.getKey(), entry
            .getValue()));
        continue;
      }
      // otherwise notify the listener that we had a failure
      ((ExceptionVisitor) entry.getKey()).visit(o, message, e, info);
    }

    // cleanup all visitors that aren't referenced anymore
    if (toRemove.size() > 0) LOG.debug(name.getNamePrefixForLog() + " Cleaning up entries.");
    for (Pair<ExceptionVisitor<?>, WeakReference<?>> entry : toRemove) {
      this.listeners.remove(entry.getFirst(), entry.getSecond());
    }
  }

  /**
   * Listen for failures to a given process
   * @param visitor pass error notifications onto the typed listener, possibly transforming or
   *          ignore the error notification
   * @param errorable listener for the errors
   */
  public synchronized <L> void addErrorListener(ExceptionVisitor<L> visitor, L errorable) {
    this.listeners.put(visitor, new WeakReference<L>(errorable));
  }

  /**
   * A simple error visitor that just forwards the received error to a generic listener.
   */
  private class ForwardingErrorVisitor implements ExceptionVisitor<ExceptionListener<E>> {

    @Override
    @SuppressWarnings("unchecked")
    public void visit(ExceptionListener<E> listener, String message, Exception e, Object... info) {
      listener.receiveError(message, (E) e, info);
    }
  }
}