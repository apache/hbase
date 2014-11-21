package org.apache.hadoop.hbase.consensus.fsm;

/**
 * A Conditional is used in the State Machine to check if an event meets a
 * particular condition before making a transition.
 */
public interface Conditional {
  boolean isMet(final Event e);
}
