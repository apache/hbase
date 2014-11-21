package org.apache.hadoop.hbase.consensus.fsm;

public class OnEvent implements Conditional {
  protected EventType t;

  public OnEvent(final EventType t) {
    this.t = t;
  }

  public boolean isMet(final Event e) {
    return e != null && e.getEventType() == t;
  }
}
