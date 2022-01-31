/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * <p>There are two modes with catalog replica support. </p>
 *
 * <ol>
 *   <li>HEDGED_READ  - Client sends requests to the primary region first, within a
 *                 configured amount of time, if there is no response coming back,
 *                 client sends requests to all replica regions and takes the first
 *                 response. </li>
 *
 *   <li>LOAD_BALANCE - Client sends requests to replica regions in a round-robin mode,
 *                 if results from replica regions are stale, next time, client sends requests for
 *                 these stale locations to the primary region. In this mode, scan
 *                 requests are load balanced across all replica regions.</li>
 * </ol>
 */
@InterfaceAudience.Private
enum CatalogReplicaMode {
  NONE {
    @Override
    public String toString() {
      return "None";
    }
  },
  HEDGED_READ {
    @Override
    public String toString() {
      return "HedgedRead";
    }
  },
  LOAD_BALANCE {
    @Override
    public String toString() {
      return "LoadBalance";
    }
  };

  public static CatalogReplicaMode fromString(final String value) {
    for(CatalogReplicaMode mode : values()) {
      if (mode.toString().equalsIgnoreCase(value)) {
        return mode;
      }
    }
    throw new IllegalArgumentException();
  }
}
