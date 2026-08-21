/*
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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Coordinates replicated bulk load events across all region servers in the sink cluster.
 */
@InterfaceAudience.Private
public interface ReplicationBulkLoadEventTracker {

  Event newEvent(String replicationClusterId, TableName table, byte[] encodedRegionName,
    long bulkLoadSeqNum, long writeTime);

  ClaimResult claim(Event event) throws IOException;

  void markDone(Event event) throws IOException;

  void release(Event event) throws IOException;

  boolean isInProgress(Event event) throws IOException;

  boolean isDone(Event event) throws IOException;

  int cleanDoneMarkers(long ttlMs) throws IOException;

  enum ClaimResult {
    CLAIMED(true),
    COMPLETED(false);

    private final boolean claimed;

    ClaimResult(boolean claimed) {
      this.claimed = claimed;
    }

    public boolean isClaimed() {
      return claimed;
    }
  }

  final class Event {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final String bucket;
    private final String eventId;
    private final String eventKey;

    Event(String bucket, String eventId, String eventKey) {
      this.bucket = bucket;
      this.eventId = eventId;
      this.eventKey = eventKey;
    }

    String getBucket() {
      return bucket;
    }

    String getEventId() {
      return eventId;
    }

    byte[] getData() {
      return eventKey == null ? EMPTY_BYTES : eventKey.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Event)) {
        return false;
      }
      Event event = (Event) o;
      return Objects.equals(bucket, event.bucket) && Objects.equals(eventId, event.eventId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bucket, eventId);
    }

    @Override
    public String toString() {
      return "Event{bucket='" + bucket + "', eventId='" + eventId + "'}";
    }
  }
}
