package org.apache.hadoop.hbase.consensus.fsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents an event within a state machine.
 */
public class Event {
  protected static Logger LOG = LoggerFactory.getLogger(Event.class);

  protected EventType t;

  public Event(final EventType t) {
    this.t = t;
  }

  public EventType getEventType() {
    return t;
  }

  /**
   * This method is called when the event is aborted. If your event has state,
   * such as a future, make sure to override this method to handle that state
   * appropriately on an abort.
   * @param message
   */
  public void abort(final String message) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Aborted %s event: %s", this, message));
    }
  }

  @Override
  public String toString() {
    return t.toString();
  }

  @Override
  public boolean equals(Object o) {
    boolean equals = false;
    if (this == o) {
      equals = true;
    } else {
      if (o instanceof Event) {
        Event that = (Event)o;
        equals = this.t.equals(that.t);
      }
    }
    return equals;
  }
}
