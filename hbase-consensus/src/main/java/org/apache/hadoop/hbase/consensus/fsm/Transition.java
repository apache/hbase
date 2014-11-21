package org.apache.hadoop.hbase.consensus.fsm;

/**
 * Represents a transition in the state machine.
 */
public class Transition {
  protected TransitionType t;
  protected Conditional c;

  /**
   * Create a transition
   * @param t type of transition
   * @param c the Conditional object, whose isMet() should return true, for
   *          this transition to happen.
   */
  public Transition(final TransitionType t, final Conditional c) {
    this.t = t;
    this.c = c;
  }

  public TransitionType getTransitionType() {
    return t;
  }

  public Conditional getCondition() {
    return c;
  }

  @Override
  public String toString() {
    return t.toString();
  }
}
