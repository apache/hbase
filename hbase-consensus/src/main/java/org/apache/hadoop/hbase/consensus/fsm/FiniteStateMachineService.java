package org.apache.hadoop.hbase.consensus.fsm;

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
