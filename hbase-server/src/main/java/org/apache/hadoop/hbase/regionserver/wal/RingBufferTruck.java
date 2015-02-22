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

import com.lmax.disruptor.EventFactory;

/**
 * A 'truck' to carry a payload across the {@link FSHLog} ring buffer from Handler to WAL.
 * Has EITHER a {@link FSWALEntry} for making an append OR it has a {@link SyncFuture} to
 * represent a 'sync' invocation. Truck instances are reused by the disruptor when it gets
 * around to it so their payload references must be discarded on consumption to release them
 * to GC.
 */
@InterfaceAudience.Private
class RingBufferTruck {
  /**
   * Either this syncFuture is set or entry is set, but not both.
   */
  private SyncFuture syncFuture;
  private FSWALEntry entry;

  /**
   * The tracing span for this entry.  Can be null.
   * TODO: Fix up tracing.
   */
  private Span span;

  /**
   * Load the truck with a {@link FSWALEntry} and associated {@link Span}.
   */
  void loadPayload(final FSWALEntry entry, final Span span) {
    this.entry = entry;
    this.span = span;
    this.syncFuture = null;
  }

  /**
   * Load the truck with a {@link SyncFuture}.
   */
  void loadPayload(final SyncFuture syncFuture) {
    this.syncFuture = syncFuture;
    this.entry = null;
    this.span = null;
  }

  /**
   * return {@code true} when this truck is carrying a {@link FSWALEntry},
   * {@code false} otherwise.
   */
  boolean hasFSWALEntryPayload() {
    return this.entry != null;
  }

  /**
   * return {@code true} when this truck is carrying a {@link SyncFuture},
   * {@code false} otherwise.
   */
  boolean hasSyncFuturePayload() {
    return this.syncFuture != null;
  }

  /**
   * Unload the truck of its {@link FSWALEntry} payload. The internal refernce is released.
   */
  FSWALEntry unloadFSWALEntryPayload() {
    FSWALEntry ret = this.entry;
    this.entry = null;
    return ret;
  }

  /**
   * Unload the truck of its {@link SyncFuture} payload. The internal refernce is released.
   */
  SyncFuture unloadSyncFuturePayload() {
    SyncFuture ret = this.syncFuture;
    this.syncFuture = null;
    return ret;
  }

  /**
   * Unload the truck of its {@link Span} payload. The internal reference is released.
   */
  Span unloadSpanPayload() {
    Span ret = this.span;
    this.span = null;
    return ret;
  }

  /**
   * Factory for making a bunch of these.  Needed by the ringbuffer/disruptor.
   */
  final static EventFactory<RingBufferTruck> EVENT_FACTORY = new EventFactory<RingBufferTruck>() {
    public RingBufferTruck newInstance() {
      return new RingBufferTruck();
    }
  };
}
