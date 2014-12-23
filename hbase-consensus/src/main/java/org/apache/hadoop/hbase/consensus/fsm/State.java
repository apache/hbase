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
 * Represents a state in the state machine implementation.
 */
public abstract class State {
  protected StateType t;

  public State(final StateType t) {
    this.t = t;
  }

  public StateType getStateType() {
    return t;
  }

  @Override
  public String toString() {
    return t.toString();
  }

  abstract public void onEntry(final Event e);
  abstract public void onExit(final Event e);

  /**
   * @return Return true if the state is an async state. This means, that the
   * state machine would invoke the onEntry() method for this state, but it is
   * possible that the state might not be complete, because we spawned a
   * background operation, such as writing to disk.
   */
  public boolean isAsyncState() {
    return false;
  }

  /**
   * If this state is async, this method returns the future to be completed,
   * before we can declare an async state to be complete.
   * @return
   */
  public ListenableFuture<?> getAsyncCompletion() {
    return null;
  }

  /**
   * This method dictates whether the onEntry() method hasn't completed
   * logically, even if the call has actually returned.
   *
   * The FSM checks for this method before making any transition. If this method
   * returns false, then any arriving events will not be applied/aborted, even
   * if the call to onEntry() method has returned.
   *
   * This is a way to do FSM thread-blocking work in an async fashion in the
   * onEntry method, and make the FSM wait the state to complete before
   * transitioning off to another state.
   *
   * By default, the method returns true, which means the FSM will not wait for
   * any async ops that you would have issued. States which require waiting,
   * will need to override this method, and make this method return true when
   * they are done.
   * @return
   */
  public boolean isComplete() {
    return true;
  }
}
