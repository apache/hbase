/*
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
package org.apache.hadoop.hbase.coprocessor;

import java.util.Optional;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the only implementation of {@link ObserverContext}, which serves as the interface for
 * third-party Coprocessor developers.
 */
@InterfaceAudience.Private
public class ObserverContextImpl<E extends CoprocessorEnvironment> implements ObserverContext<E> {
  private E env;
  private boolean bypass;
  /**
   * Is this operation bypassable?
   */
  private final boolean bypassable;
  private final User caller;

  public ObserverContextImpl(User caller) {
    this(caller, false);
  }

  public ObserverContextImpl(User caller, boolean bypassable) {
    this.caller = caller;
    this.bypassable = bypassable;
  }

  @Override
  public E getEnvironment() {
    return env;
  }

  public void prepare(E env) {
    this.env = env;
  }

  public boolean isBypassable() {
    return this.bypassable;
  };

  @Override
  public void bypass() {
    if (!this.bypassable) {
      throw new UnsupportedOperationException("This method does not support 'bypass'.");
    }
    bypass = true;
  }

  /**
   * @return {@code true}, if {@link ObserverContext#bypass()} was called by one of the loaded
   * coprocessors, {@code false} otherwise.
   */
  public boolean shouldBypass() {
    if (!isBypassable()) {
      return false;
    }
    if (bypass) {
      bypass = false;
      return true;
    }
    return false;
  }

  @Override
  public Optional<User> getCaller() {
    return Optional.ofNullable(caller);
  }

  /**
   * Instantiates a new ObserverContext instance if the passed reference is <code>null</code> and
   * sets the environment in the new or existing instance. This allows deferring the instantiation
   * of a ObserverContext until it is actually needed.
   * @param <E> The environment type for the context
   * @param env The coprocessor environment to set
   * @return An instance of <code>ObserverContext</code> with the environment set
   */
  @Deprecated
  // TODO: Remove this method, ObserverContext should not depend on RpcServer
  public static <E extends CoprocessorEnvironment> ObserverContext<E> createAndPrepare(E env) {
    ObserverContextImpl<E> ctx = new ObserverContextImpl<>(RpcServer.getRequestUser().orElse(null));
    ctx.prepare(env);
    return ctx;
  }
}
