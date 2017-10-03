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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Carries the execution state for a given invocation of an Observer coprocessor
 * ({@link RegionObserver}, {@link MasterObserver}, or {@link WALObserver})
 * method.  The same ObserverContext instance is passed sequentially to all loaded
 * coprocessors for a given Observer method trigger, with the
 * <code>CoprocessorEnvironment</code> reference swapped out for each
 * coprocessor.
 * @param <E> The {@link CoprocessorEnvironment} subclass applicable to the
 *     revelant Observer interface.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class ObserverContext<E extends CoprocessorEnvironment> {
  private E env;
  private boolean bypass;
  private boolean complete;
  private final User caller;

  @InterfaceAudience.Private
  public ObserverContext(User caller) {
    this.caller = caller;
  }

  public E getEnvironment() {
    return env;
  }

  @InterfaceAudience.Private
  public void prepare(E env) {
    this.env = env;
  }

  /**
   * Call to indicate that the current coprocessor's return value should be
   * used in place of the normal HBase obtained value.
   */
  public void bypass() {
    bypass = true;
  }

  /**
   * Call to indicate that additional coprocessors further down the execution
   * chain do not need to be invoked.  Implies that this coprocessor's response
   * is definitive.
   */
  public void complete() {
    complete = true;
  }

  /**
   * For use by the coprocessor framework.
   * @return <code>true</code> if {@link ObserverContext#bypass()}
   *     was called by one of the loaded coprocessors, <code>false</code> otherwise.
   */
  public boolean shouldBypass() {
    boolean current = bypass;
    bypass = false;
    return current;
  }

  /**
   * For use by the coprocessor framework.
   * @return <code>true</code> if {@link ObserverContext#complete()}
   *     was called by one of the loaded coprocessors, <code>false</code> otherwise.
   */
  public boolean shouldComplete() {
    boolean current = complete;
    complete = false;
    return current;
  }

  /**
   * Returns the active user for the coprocessor call. If an explicit {@code User} instance was
   * provided to the constructor, that will be returned, otherwise if we are in the context of an
   * RPC call, the remote user is used. May not be present if the execution is outside of an RPC
   * context.
   */
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
  @InterfaceAudience.Private
  @VisibleForTesting
  // TODO: Remove this method, ObserverContext should not depend on RpcServer
  public static <E extends CoprocessorEnvironment> ObserverContext<E> createAndPrepare(E env) {
    ObserverContext<E> ctx = new ObserverContext<>(RpcServer.getRequestUser().orElse(null));
    ctx.prepare(env);
    return ctx;
  }
}
