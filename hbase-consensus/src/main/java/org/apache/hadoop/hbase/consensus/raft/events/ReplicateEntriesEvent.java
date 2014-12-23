package org.apache.hadoop.hbase.consensus.raft.events;

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


import com.google.common.util.concurrent.SettableFuture;

import org.apache.hadoop.hbase.consensus.protocol.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

import java.nio.ByteBuffer;

public class ReplicateEntriesEvent extends Event {
  private static final Logger LOG = LoggerFactory.getLogger(
          ReplicateEntriesEvent.class);
  private final boolean isHeartBeat;

  private final Payload payload;

  public ReplicateEntriesEvent(final boolean isHeartBeat, final ByteBuffer entries) {
    this (isHeartBeat, entries, null);
  }

  public ReplicateEntriesEvent(final boolean isHeartBeat,
                               final ByteBuffer entries,
                               final SettableFuture<Long> result) {
    super(RaftEventType.REPLICATE_ENTRIES);
    this.isHeartBeat = isHeartBeat;
    this.payload = new Payload(entries, result);
  }

  public Payload getPayload() {
    return payload;
  }

  public ByteBuffer getEntries() {
    return payload.getEntries();
  }

  public boolean isHeartBeat() {
    return this.isHeartBeat;
  }

  public SettableFuture<Long> getFutureResult() {
    // won't create the future result for the heart beat msg
    assert !isHeartBeat;
    return payload.getResult();
  }

  public void setReplicationSucceeded(long commitIndex) {
    assert !isHeartBeat;
    if (!isHeartBeat() && payload.getResult() != null) {
      payload.getResult().set(commitIndex);
    }
  }

  public void setReplicationFailed(Throwable reason) {
    assert !isHeartBeat;
    if (!isHeartBeat() && payload.getResult() != null) {
      payload.getResult().setException(reason);
    }
  }

  @Override
  public void abort(final String message) {
    LOG.error(String.format("Aborted %s event: %s", this, message));

    if (!isHeartBeat() && payload.getResult() != null) {
      payload.getResult().setException(new ThriftHBaseException(new Exception(
        String.format("Cannot complete the replication request. Reason %s",
          message))));
    }
  }

  @Override
  public boolean equals(Object o) {
    boolean equals = false;
    if (this == o) {
      equals = true;
    } else {
      if (o instanceof ReplicateEntriesEvent) {
        ReplicateEntriesEvent that = (ReplicateEntriesEvent)o;
        equals = super.equals(that) &&
                isHeartBeat == that.isHeartBeat &&
                payload.getEntries().equals(that.payload.getEntries());
      }
    }
    return equals;
  }
}
