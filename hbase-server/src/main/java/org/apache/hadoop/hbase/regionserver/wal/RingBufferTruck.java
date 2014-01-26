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

import org.apache.hadoop.classification.InterfaceAudience;
import org.cloudera.htrace.Span;

import com.lmax.disruptor.EventFactory;

/**
 * A 'truck' to carry a payload across the {@link FSHLog} ring buffer from Handler to WAL.
 * Has EITHER a {@link FSWALEntry} for making an append OR it has a {@link SyncFuture} to
 * represent a 'sync' invocation. Immutable but instances get recycled on the ringbuffer.
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

  void loadPayload(final FSWALEntry entry, final Span span) {
    this.entry = entry;
    this.span = span;
    this.syncFuture = null;
  }

  void loadPayload(final SyncFuture syncFuture) {
    this.syncFuture = syncFuture;
    this.entry = null;
    this.span = null;
  }

  FSWALEntry getFSWALEntryPayload() {
    return this.entry;
  }

  SyncFuture getSyncFuturePayload() {
    return this.syncFuture;
  }

  Span getSpanPayload() {
    return this.span;
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