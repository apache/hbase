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
