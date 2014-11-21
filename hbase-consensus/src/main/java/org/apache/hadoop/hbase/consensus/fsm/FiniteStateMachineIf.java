package org.apache.hadoop.hbase.consensus.fsm;

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
