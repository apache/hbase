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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for staged WAL flush behavior in ReplicationSourceShipper. These tests validate that
 * beforePersistingReplicationOffset() is invoked only when there is staged WAL data, and is not
 * invoked for empty batches.
 */
public class TestReplicationSourceShipperBufferedFlush {

  /**
   * ReplicationEndpoint implementation used for testing. Counts how many times
   * beforePersistingReplicationOffset() is called.
   */
  static class CountingReplicationEndpoint extends BaseReplicationEndpoint {

    private final AtomicInteger beforePersistCalls = new AtomicInteger();

    @Override
    protected void doStart() {
      notifyStarted();
    }

    @Override
    protected void doStop() {
      notifyStopped();
    }

    @Override
    public UUID getPeerUUID() {
      return null;
    }

    @Override
    public boolean replicate(ReplicateContext ctx) {
      return true;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void beforePersistingReplicationOffset() {
      beforePersistCalls.incrementAndGet();
    }

    int getBeforePersistCalls() {
      return beforePersistCalls.get();
    }

    @Override
    public long getMaxBufferSize() {
      // Force size-based flush after any non-empty batch
      return 1L;
    }

    @Override
    public long maxFlushInterval() {
      return Long.MAX_VALUE;
    }
  }

  @Test
  public void testBeforePersistNotCalledForEmptyBatch() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    CountingReplicationEndpoint endpoint = new CountingReplicationEndpoint();

    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    ReplicationSourceWALReader walReader = Mockito.mock(ReplicationSourceWALReader.class);

    WALEntryBatch emptyBatch = Mockito.mock(WALEntryBatch.class);
    Mockito.when(emptyBatch.getWalEntries()).thenReturn(Collections.emptyList());

    Mockito.when(walReader.take()).thenReturn(emptyBatch).thenReturn(null);

    Mockito.when(source.isPeerEnabled()).thenReturn(true);
    Mockito.when(source.getReplicationEndpoint()).thenReturn(endpoint);
    Mockito.when(source.getPeerId()).thenReturn("1");

    ReplicationSourceShipper shipper =
      new ReplicationSourceShipper(conf, "wal-group", source, walReader);

    shipper.start();

    // Give the shipper thread time to process the empty batch
    Waiter.waitFor(conf, 3000, () -> true);

    shipper.interrupt();
    shipper.join();

    assertEquals("beforePersistingReplicationOffset should not be called for empty batch", 0,
      endpoint.getBeforePersistCalls());
  }

  @Test
  public void testBeforePersistCalledForNonEmptyBatch() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    CountingReplicationEndpoint endpoint = new CountingReplicationEndpoint();

    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    ReplicationSourceWALReader walReader = Mockito.mock(ReplicationSourceWALReader.class);

    WALEntryBatch batch = Mockito.mock(WALEntryBatch.class);
    WAL.Entry entry = Mockito.mock(WAL.Entry.class);

    Mockito.when(batch.getWalEntries()).thenReturn(Collections.singletonList(entry));
    Mockito.when(batch.getHeapSize()).thenReturn(10L);
    Mockito.when(batch.isEndOfFile()).thenReturn(false);
    Mockito.when(walReader.take()).thenReturn(batch).thenReturn(null);

    Mockito.when(source.isPeerEnabled()).thenReturn(true);
    Mockito.when(source.getReplicationEndpoint()).thenReturn(endpoint);
    Mockito.when(source.getPeerId()).thenReturn("1");

    ReplicationSourceShipper shipper =
      new ReplicationSourceShipper(conf, "wal-group", source, walReader);

    shipper.start();

    // Wait until beforePersistingReplicationOffset() is invoked once
    Waiter.waitFor(conf, 5000, () -> endpoint.getBeforePersistCalls() == 1);

    shipper.interrupt();
    shipper.join();

    assertEquals("beforePersistingReplicationOffset should be called exactly once", 1,
      endpoint.getBeforePersistCalls());
  }
}
