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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Tests staged WAL flush behavior in ReplicationSourceShipper.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationSourceShipperBufferedFlush {

  static class CountingReplicationEndpoint extends BaseReplicationEndpoint {

    private final AtomicInteger beforePersistCalls = new AtomicInteger();

    @Override
    public void start() {
      startAsync().awaitRunning();
    }

    @Override
    public void stop() {
      stopAsync().awaitTerminated();
    }

    @Override
    protected void doStart() {
      notifyStarted();
    }

    @Override
    protected void doStop() {
      notifyStopped();
    }

    @Override
    public boolean replicate(ReplicateContext ctx) {
      return true;
    }

    @Override
    public void beforePersistingReplicationOffset() {
      beforePersistCalls.incrementAndGet();
    }

    @Override
    public long getMaxBufferSize() {
      return 1L; // force immediate flush
    }

    @Override
    public long maxFlushInterval() {
      return Long.MAX_VALUE;
    }

    @Override
    public UUID getPeerUUID() {
      return null;
    }

    int getBeforePersistCalls() {
      return beforePersistCalls.get();
    }
  }

  @Test
  public void testBeforePersistNotCalledForEmptyBatch() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    CountingReplicationEndpoint endpoint = new CountingReplicationEndpoint();
    endpoint.start();

    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    ReplicationSourceWALReader walReader = Mockito.mock(ReplicationSourceWALReader.class);

    Mockito.when(source.isPeerEnabled()).thenReturn(true);
    Mockito.when(source.isSourceActive()).thenReturn(true);
    Mockito.when(source.getReplicationEndpoint()).thenReturn(endpoint);
    Mockito.when(source.getPeerId()).thenReturn("1");
    Mockito.when(source.getSourceMetrics()).thenReturn(Mockito.mock(MetricsSource.class));

    WALEntryBatch batch = new WALEntryBatch(1, null);
    batch.setLastWalPath(new Path("wal"));
    batch.setLastWalPosition(1L);
    // no entries, no heap size

    Mockito.when(walReader.take()).thenReturn(batch).thenReturn(null);

    ReplicationSourceShipper shipper =
      new ReplicationSourceShipper(conf, "wal-group", source, walReader);

    shipper.start();

    // Allow loop to run
    Waiter.waitFor(conf, 3000, () -> true);

    shipper.interrupt();
    shipper.join();

    assertEquals(0, endpoint.getBeforePersistCalls());
  }
}
