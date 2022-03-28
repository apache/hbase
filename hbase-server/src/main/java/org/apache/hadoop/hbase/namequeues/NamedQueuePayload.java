/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.namequeues;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base payload to be prepared by client to send various namedQueue events for in-memory
 * ring buffer storage in either HMaster or RegionServer.
 * e.g slowLog responses
 */
@InterfaceAudience.Private
public class NamedQueuePayload {

  public enum NamedQueueEvent {
    SLOW_LOG(0),
    BALANCE_DECISION(1),
    BALANCE_REJECTION(2);

    private final int value;

    NamedQueueEvent(int i) {
      this.value = i;
    }

    public static NamedQueueEvent getEventByOrdinal(int value){
      switch (value) {
        case 0: {
          return SLOW_LOG;
        }
        case 1: {
          return BALANCE_DECISION;
        }
        case 2: {
          return BALANCE_REJECTION;
        }
        default: {
          throw new IllegalArgumentException(
            "NamedQueue event with ordinal " + value + " not defined");
        }
      }
    }

    public int getValue() {
      return value;
    }
  }

  private final NamedQueueEvent namedQueueEvent;

  public NamedQueuePayload(int eventOrdinal) {
    this.namedQueueEvent = NamedQueueEvent.getEventByOrdinal(eventOrdinal);
  }

  public NamedQueueEvent getNamedQueueEvent() {
    return namedQueueEvent;
  }

}
