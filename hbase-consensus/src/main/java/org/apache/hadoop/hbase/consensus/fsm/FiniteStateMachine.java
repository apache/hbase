package org.apache.hadoop.hbase.consensus.fsm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * This is an implementation of a finite state machine. The state machine also
 * handles async states.
 */
public class FiniteStateMachine implements FiniteStateMachineIf {
  protected static Logger LOG = LoggerFactory.getLogger(
          FiniteStateMachine.class);

  private String name;
  private State currentState;
  private HashMap<State, HashMap<Transition, State>> stateTransitionMap;
  private boolean needsHandlingOnAsyncTaskCompletion = false;

  public FiniteStateMachine(final String name) {
    this.name = name;
    this.stateTransitionMap = new HashMap<>();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public State getCurrentState() {
    return currentState;
  }

  @Override
  public String toString() {
    return "[" + this.getClass().getSimpleName() + ": " + name + ", state: " +
            currentState + "]";
  }

  @Override
  public void addTransition(final State s1, final State s2,
                            final Transition t) {
    HashMap<Transition, State> transitionMap = getTransitionMap(s1);
    if (transitionMap == null) {
      transitionMap = new HashMap<>();
    }
    transitionMap.put(t, s2);
    stateTransitionMap.put(s1, transitionMap);
  }

  @Override
  public boolean setStartState(State s) {
    if (currentState == null) {
      currentState = s;
      return true;
    }
    return false;
  }

  @Override
  public boolean isInAnIncompleteAsyncState() {
    return currentState.isAsyncState() && !currentState.isComplete();
  }

  @Override
  public boolean isInACompletedAsyncState() {
    return currentState.isAsyncState() && currentState.isComplete();
  }

  @Override
  public void finishAsyncStateCompletionHandling() {
    if (isInACompletedAsyncState()) {
      applyValidConditionalTransitions();
    }
  }

  /**
   * Check if we can apply any conditional transitions. This is usually
   * used when an async state finishes, and any valid conditional transitions
   * that were not applicable earlier, can be applied now.
   */
  private void applyValidConditionalTransitions() {
    if (isInACompletedAsyncState() && needsHandlingOnAsyncTaskCompletion) {
      needsHandlingOnAsyncTaskCompletion = false;
      State nextState = getNextState(null);
      if (nextState != null) {
        currentState = nextState;
      }
    }
  }

  @Override
  public void handleEvent(final Event e) {
    try {
      State nextState = getNextState(e);
      if (nextState != null) {
        currentState = nextState;
      } else {
        e.abort(String.format("No transition found from state %s:%s", getName(),
          currentState));
      }
    } catch (Exception ex) {
      LOG.warn(String.format("%s ran into an error while processing event " +
        "%s. Current State : %s", this.getName(), e, currentState), ex);
    }
  }

  @Override
  public ListenableFuture<?> getAsyncCompletion() {
    if (currentState.isAsyncState()) {
      Assert.assertTrue(needsHandlingOnAsyncTaskCompletion);
      return currentState.getAsyncCompletion();
    }
    return null;
  }

  public boolean waitForAsyncToComplete(long timeout, TimeUnit unit) {
    if (isInAnIncompleteAsyncState()) {
      Future<?> future = getAsyncCompletion();
      if (future == null) {
        return true;
      }
      try {
        future.get(timeout, unit);
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      } catch (TimeoutException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Timed out while waiting for the async " +
            "state %s to complete.", getCurrentState()));
        }
        return false;
      } catch (ExecutionException e) {
        return true;
      }
    }
    return true;
  }

  protected HashMap<Transition, State> getTransitionMap(final State s) {
    return stateTransitionMap.get(s);
  }

  protected Transition getNextTransition(final State s, final Event e) {
    if (needsHandlingOnAsyncTaskCompletion) {
      return null;
    }

    Transition nextTransition = null;
    HashMap<Transition, State> transitionMap = getTransitionMap(s);
    if (transitionMap != null) {
      Iterator<Entry<Transition, State>> it = transitionMap.entrySet().iterator();
      while (nextTransition == null && it.hasNext()) {
        Entry<Transition, State> entry = it.next();
        Transition t = entry.getKey();
        if (t.getCondition().isMet(e)) {
          nextTransition = t;
        }
      }
    }
    return nextTransition;
  }

  protected State getNextState(Event e) {
    State nextState = null;

    Transition nextTransition = getNextTransition(currentState, e);
    while (nextTransition != null) {
      nextState = stateTransitionMap.get(currentState).get(nextTransition);

      if (nextState != null && nextState != currentState) {
        currentState.onExit(e);

        currentState = nextState;
        if (nextState.isAsyncState()) {
          needsHandlingOnAsyncTaskCompletion = true;
        } else {
          needsHandlingOnAsyncTaskCompletion = false;
        }
        nextState.onEntry(e);
      }
      e = null; // Only pass the event on the first loop.
      nextTransition = getNextTransition(nextState, e);
    }
    return nextState;
  }
}
