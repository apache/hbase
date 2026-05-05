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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class TestReplicationSourceShipperRestart {

  private ReplicationSource source;
  private ReplicationSourceWALReader reader;
  private MetricsSource metrics;
  private ReplicationEndpoint endpoint;

  private static final Path PATH = new Path("/test");

  @Before
  public void setup() {
    source = mock(ReplicationSource.class);
    reader = mock(ReplicationSourceWALReader.class);
    metrics = mock(MetricsSource.class);
    endpoint = mock(ReplicationEndpoint.class);

    when(source.isPeerEnabled()).thenReturn(true);
    when(source.isSourceActive()).thenReturn(true);
    when(source.getSourceMetrics()).thenReturn(metrics);
    when(source.getReplicationEndpoint()).thenReturn(endpoint);

    ReplicationQueueId qid = mock(ReplicationQueueId.class);
    when(source.getQueueId()).thenReturn(qid);

    when(endpoint.replicate(any())).thenReturn(true);

    // 🔥 IMPORTANT: isolate heavy HBase logic
    doNothing().when(source).logPositionAndCleanOldLogs(any());
  }

  // ------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------

  private WALEntryBatch emptyBatch() {
    WALEntryBatch b = mock(WALEntryBatch.class);
    when(b.getWalEntries()).thenReturn(Collections.emptyList());
    when(b.getHeapSize()).thenReturn(100L);
    when(b.getLastWalPosition()).thenReturn(1L);
    when(b.getLastWalPath()).thenReturn(PATH);
    return b;
  }

  private WALEntryBatch batch(long size) {
    WALEntryBatch b = mock(WALEntryBatch.class);

    Entry e = mock(Entry.class);
    WALEdit edit = mock(WALEdit.class);
    WALKeyImpl key = mock(WALKeyImpl.class);

    when(key.getWriteTime()).thenReturn(1L);
    when(key.getTableName()).thenReturn(TableName.valueOf("t"));

    when(e.getKey()).thenReturn(key);
    when(e.getEdit()).thenReturn(edit);

    when(b.getWalEntries()).thenReturn(Collections.singletonList(e));
    when(b.getHeapSize()).thenReturn(size);
    when(b.getLastWalPosition()).thenReturn(1L);
    when(b.getLastWalPath()).thenReturn(PATH);

    return b;
  }

  /**
   * Light shipper → skips persist logic
   */
  private ReplicationSourceShipper lightShipper(Configuration conf) {
    return new ReplicationSourceShipper(conf, "group", source, reader) {
      int loops = 0;

      @Override
      protected boolean isActive() {
        return loops++ < 2;
      }

      @Override
      protected void cleanUpHFileRefs(WALEdit edit) {
      }

      @Override
      void persistLogPosition() {
        // skip heavy logic
      }
    };
  }

  /**
   * Real shipper → executes persist logic
   */
  private ReplicationSourceShipper realShipper(Configuration conf) {
    return new ReplicationSourceShipper(conf, "group", source, reader) {
      int loops = 0;

      @Override
      protected boolean isActive() {
        return loops++ < 2;
      }

      @Override
      protected void cleanUpHFileRefs(WALEdit edit) {
      }
    };
  }

  // ------------------------------------------------------------------------
  // Restart
  // ------------------------------------------------------------------------

  @Test
  public void testAbortAndRestart() {
    ReplicationSourceShipper s =
      new ReplicationSourceShipper(new Configuration(), "group", source, reader);

    s.abortAndRestart(new IOException());

    verify(source).restartShipper("group", s);
  }

  // ------------------------------------------------------------------------
  // Empty batch
  // ------------------------------------------------------------------------

  @Test
  public void testEmptyBatchTriggersPersist() throws Exception {
    ReplicationSourceShipper s = spy(lightShipper(new Configuration()));

    s.shipEdits(emptyBatch());

    verify(s).persistLogPosition();
  }

  // ------------------------------------------------------------------------
  // Default persist
  // ------------------------------------------------------------------------

  @Test
  public void testDefaultImmediatePersist() throws Exception {
    ReplicationSourceShipper s = spy(lightShipper(new Configuration()));

    s.shipEdits(batch(50));

    verify(s).persistLogPosition();
  }

  // ------------------------------------------------------------------------
  // Size threshold
  // ------------------------------------------------------------------------

  @Test
  public void testSizeThreshold() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("hbase.replication.shipper.offset.update.size.threshold", 100);

    ReplicationSourceShipper s = spy(lightShipper(conf));

    s.shipEdits(batch(40));
    verify(s, never()).persistLogPosition();

    s.shipEdits(batch(70));
    verify(s, times(1)).persistLogPosition();
  }

  // ------------------------------------------------------------------------
  // Time threshold
  // ------------------------------------------------------------------------

  @Test
  public void testTimeThreshold() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("hbase.replication.shipper.offset.update.interval.ms", 1);

    ReplicationSourceShipper s = spy(lightShipper(conf));

    s.shipEdits(batch(10));
    Thread.sleep(2);
    s.shipEdits(batch(10));

    verify(s, atLeastOnce()).persistLogPosition();
  }

  // ------------------------------------------------------------------------
  // Hook test (FIXED)
  // ------------------------------------------------------------------------

  @Test
  public void testBeforePersistHookCalled() throws Exception {
    ReplicationSourceShipper s = realShipper(new Configuration());

    s.shipEdits(emptyBatch());

    verify(endpoint, atLeastOnce()).beforePersistingReplicationOffset();
  }

  // ------------------------------------------------------------------------
  // Ordering test (FIXED)
  // ------------------------------------------------------------------------

  @Test
  public void testBeforePersistCalledBeforeUpdate() throws Exception {
    ReplicationSourceShipper s = realShipper(new Configuration());

    InOrder order = inOrder(endpoint, source);

    s.shipEdits(batch(100));

    order.verify(endpoint).beforePersistingReplicationOffset();
    order.verify(source).logPositionAndCleanOldLogs(any());
  }

  // ------------------------------------------------------------------------
  // Persist failure
  // ------------------------------------------------------------------------

  @Test(expected = IOException.class)
  public void testPersistFailureThrows() throws Exception {
    doThrow(new IOException()).when(endpoint).beforePersistingReplicationOffset();

    new ReplicationSourceShipper(new Configuration(), "group", source, reader)
      .shipEdits(emptyBatch());
  }

  // ------------------------------------------------------------------------
  // Restart via run
  // ------------------------------------------------------------------------

  @Test
  public void testPersistFailureTriggersRestart() throws Exception {

    WALEntryBatch batch = emptyBatch();

    when(reader.poll(anyLong())).thenReturn(batch).thenThrow(new InterruptedException());

    doThrow(new IOException()).when(endpoint).beforePersistingReplicationOffset();

    ReplicationSourceShipper s =
      new ReplicationSourceShipper(new Configuration(), "group", source, reader);

    ReplicationSourceShipper spy = spy(s);
    doReturn(true, true, false).when(spy).isActive();

    spy.run();

    verify(source, atLeastOnce()).restartShipper(eq("group"), eq(spy));
  }

  // ------------------------------------------------------------------------
  // Normal run
  // ------------------------------------------------------------------------

  @Test
  public void testRunNormalFlow() throws Exception {

    WALEntryBatch batch = emptyBatch();

    when(reader.poll(anyLong())).thenReturn(batch).thenThrow(new InterruptedException());

    ReplicationSourceShipper s = lightShipper(new Configuration());

    ReplicationSourceShipper spy = spy(s);
    doReturn(true, true, false).when(spy).isActive();

    spy.run();

    verify(source, never()).restartShipper(any(), any());
  }
}
