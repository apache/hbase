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

import java.util.ArrayList;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatcher acts as the state holding entity for foreign error handling.  The first
 * exception received by the dispatcher get passed directly to the listeners.  Subsequent
 * exceptions are dropped.
 * <p>
 * If there are multiple dispatchers that are all in the same foreign exception monitoring group,
 * ideally all these monitors are "peers" -- any error on one dispatcher should get propagated to
 * all others (via rpc, or some other mechanism).  Due to racing error conditions the exact reason
 * for failure may be different on different peers, but the fact that they are in error state
 * should eventually hold on all.
 * <p>
 * This is thread-safe and must be because this is expected to be used to propagate exceptions
 * from foreign threads.
 */
@InterfaceAudience.Private
public class ForeignExceptionDispatcher implements ForeignExceptionListener, ForeignExceptionSnare {
  private static final Logger LOG = LoggerFactory.getLogger(ForeignExceptionDispatcher.class);
  protected final String name;
  protected final List<ForeignExceptionListener> listeners = new ArrayList<>();
  private ForeignException exception;

  public ForeignExceptionDispatcher(String name) {
    this.name = name;
  }

  public ForeignExceptionDispatcher() {
    this("");
  }

  public String getName() {
    return name;
  }

  @Override
  public synchronized void receive(ForeignException e) {
    // if we already have an exception, then ignore it
    if (exception != null) return;

    LOG.debug(name + " accepting received exception" , e);
    // mark that we got the error
    if (e != null) {
      exception = e;
    } else {
      exception = new ForeignException(name, "");
    }

    // notify all the listeners
    dispatch(e);
  }

  @Override
  public synchronized void rethrowException() throws ForeignException {
    if (exception != null) {
      // This gets the stack where this is caused, (instead of where it was deserialized).
      // This is much more useful for debugging
      throw new ForeignException(exception.getSource(), exception.getCause());
    }
  }

  @Override
  public synchronized boolean hasException() {
    return exception != null;
  }

  @Override
  synchronized public ForeignException getException() {
    return exception;
  }

  /**
   * Sends an exception to all listeners.
   * @param e {@link ForeignException} containing the cause.  Can be null.
   */
  private void dispatch(ForeignException e) {
    // update all the listeners with the passed error
    for (ForeignExceptionListener l: listeners) {
      l.receive(e);
    }
  }

  /**
   * Listen for failures to a given process.  This method should only be used during
   * initialization and not added to after exceptions are accepted.
   * @param errorable listener for the errors.  may be null.
   */
  public synchronized void addListener(ForeignExceptionListener errorable) {
    this.listeners.add(errorable);
  }
}
