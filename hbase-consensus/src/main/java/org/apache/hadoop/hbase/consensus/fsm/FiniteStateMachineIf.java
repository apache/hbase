package org.apache.hadoop.hbase.consensus.fsm;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.google.common.util.concurrent.ListenableFuture;

/**
 * An interface for FiniteStateMachine implementations.
 */
public interface FiniteStateMachineIf {
  /**
   * @return The name of the State Machine.
   */
  public String getName();

  /**
   * @return The current state of the state machine.
   */
  public State getCurrentState();

  /**
   * Set the start state of the state machine.
   * @param s The starting state.
   * @return Return false if we could not set the state. Otherwise return true.
   */
  public boolean setStartState(State s);

  /**
   * Add a transition t from the state s1, to the state s2.
   * @param s1
   * @param s2
   * @param t
   */
  public void addTransition(final State s1, final State s2, final Transition t);

  /**
   * Request the FSM to handle a particular Event.
   * @param e
   */
  public void handleEvent(final Event e);

  /**
   * @return Returns true, if the FSM is in an async state, that has not yet
   *         completed.
   */
  public boolean isInAnIncompleteAsyncState();

  /**
   * @return Returns true, if the FSM is in an async state, that has completed.
   */
  public boolean isInACompletedAsyncState();

  public ListenableFuture<?> getAsyncCompletion();

  /**
   * Finish the handling of an async state's onEntry() method completion.
   */
  public void finishAsyncStateCompletionHandling();
}
