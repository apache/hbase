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


import java.util.concurrent.TimeUnit;

public interface FiniteStateMachineService {
  /**
   * @return true if this service is shutdown, false otherwise
   */
  public boolean isShutdown();

  /**
   * @return true if the service has completed processing events,
   * false otherwise.
   */
  public boolean isTerminated();

  /**
   * @return the name of the state machine
   */
  public String getName();

  /**
   * Return the current state of the state machine. Note: this is not thread
   * safe, so unless this is called from within the state machine this will
   * return a stale result.
   *
   * @return the current state of the state machine
   */
  public State getCurrentState();

  /**
   * Submits an event to the state machine for execution.
   *
   * @param e the event to be executed
   * @return true if the event was added to the event queue, false otherwise
   */
  public boolean offer(final Event e);

  /**
   * Initiates an orderly shutdown in which no new events will be accepted and
   * the service will shutdown as soon as the currently executing event was
   * completed.
   */
  public void shutdown();

  /**
   * Attempts to interrupt the actively executing event and halts the process of
   * waiting for events.
   */
  public void shutdownNow();

  /**
   * Blocks until the current event has completed execution, or the timeout
   * occurs, or the current thread is interrupted, whichever happens first.
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return true if this service terminated and false if the timeout elapsed before
   * termination
   */
  public boolean awaitTermination(final long timeout, TimeUnit unit)
          throws InterruptedException;

  /**
   * @return Return the number of pending events in the FSM's event queue.
   */
  public int getNumPendingEvents();
}
