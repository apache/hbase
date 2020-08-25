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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Base payload to be prepared by client to send various namedQueue events for in-memory
 * ring buffer storage in either HMaster or RegionServer.
 * e.g slowLog responses
 */
@InterfaceAudience.Private
public class NamedQueuePayload {

  public enum NamedQueueEvent {
    SLOW_LOG
  }

  private final NamedQueueEvent namedQueueEvent;

  public NamedQueuePayload(NamedQueueEvent namedQueueEvent) {
    if (namedQueueEvent == null) {
      throw new RuntimeException("NamedQueuePayload with null namedQueueEvent");
    }
    this.namedQueueEvent = namedQueueEvent;
  }

  public NamedQueueEvent getNamedQueueEvent() {
    return namedQueueEvent;
  }

}
