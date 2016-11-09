/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.htrace.Span;

/**
 * A 'truck' to carry a payload across the ring buffer from Handler to WAL. Has EITHER a
 * {@link FSWALEntry} for making an append OR it has a {@link SyncFuture} to represent a 'sync'
 * invocation. Truck instances are reused by the disruptor when it gets around to it so their
 * payload references must be discarded on consumption to release them to GC.
 */
@InterfaceAudience.Private
final class RingBufferTruck {

  public enum Type {
    APPEND, SYNC, EMPTY
  }

  private Type type = Type.EMPTY;

  /**
   * Either this syncFuture is set or entry is set, but not both.
   */
  private SyncFuture sync;
  private FSWALEntry entry;

  /**
   * Load the truck with a {@link FSWALEntry} and associated {@link Span}.
   */
  void load(FSWALEntry entry, Span span) {
    entry.attachSpan(span);
    this.entry = entry;
    this.type = Type.APPEND;
  }

  /**
   * Load the truck with a {@link SyncFuture}.
   */
  void load(final SyncFuture syncFuture) {
    this.sync = syncFuture;
    this.type = Type.SYNC;
  }

  /**
   * @return the type of this truck's payload.
   */
  Type type() {
    return type;
  }

  /**
   * Unload the truck of its {@link FSWALEntry} payload. The internal reference is released.
   */
  FSWALEntry unloadAppend() {
    FSWALEntry entry = this.entry;
    this.entry = null;
    this.type = Type.EMPTY;
    return entry;
  }

  /**
   * Unload the truck of its {@link SyncFuture} payload. The internal reference is released.
   */
  SyncFuture unloadSync() {
    SyncFuture sync = this.sync;
    this.sync = null;
    this.type = Type.EMPTY;
    return sync;
  }
}
