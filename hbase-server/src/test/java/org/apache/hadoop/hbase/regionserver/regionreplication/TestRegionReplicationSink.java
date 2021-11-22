/**
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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicationSink {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicationSink.class);

  private Configuration conf;

  private TableDescriptor td;

  private RegionInfo primary;

  private Runnable flushRequester;

  private AsyncClusterConnection conn;

  private RegionReplicationBufferManager manager;

  private RegionReplicationSink sink;

  @Rule
  public final TableNameTestRule name = new TableNameTestRule();

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    td = TableDescriptorBuilder.newBuilder(name.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).setRegionReplication(3).build();
    primary = RegionInfoBuilder.newBuilder(name.getTableName()).build();
    flushRequester = mock(Runnable.class);
    conn = mock(AsyncClusterConnection.class);
    manager = mock(RegionReplicationBufferManager.class);
    sink = new RegionReplicationSink(conf, primary, td, manager, flushRequester, conn);
  }

  @After
  public void tearDown() throws InterruptedException {
    sink.stop();
    sink.waitUntilStopped();
  }

  @Test
  public void testNormal() {
    MutableInt next = new MutableInt(0);
    List<CompletableFuture<Void>> futures =
      Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());
    when(conn.replicate(any(), anyList(), anyInt(), anyLong(), anyLong()))
      .then(i -> futures.get(next.getAndIncrement()));
    ServerCall<?> rpcCall = mock(ServerCall.class);
    WALKeyImpl key = mock(WALKeyImpl.class);
    when(key.estimatedSerializedSizeOf()).thenReturn(100L);
    WALEdit edit = mock(WALEdit.class);
    when(edit.estimatedSerializedSizeOf()).thenReturn(1000L);
    when(manager.increase(anyLong())).thenReturn(true);

    sink.add(key, edit, rpcCall);
    // should call increase on manager
    verify(manager, times(1)).increase(anyLong());
    // should have been retained
    verify(rpcCall, times(1)).retainByWAL();
    assertEquals(1100, sink.pendingSize());

    futures.get(0).complete(null);
    // should not call decrease yet
    verify(manager, never()).decrease(anyLong());
    // should not call release yet
    verify(rpcCall, never()).releaseByWAL();
    assertEquals(1100, sink.pendingSize());

    futures.get(1).complete(null);
    // should call decrease
    verify(manager, times(1)).decrease(anyLong());
    // should call release
    verify(rpcCall, times(1)).releaseByWAL();
    assertEquals(0, sink.pendingSize());
  }

  @Test
  public void testDropEdits() {
    MutableInt next = new MutableInt(0);
    List<CompletableFuture<Void>> futures =
      Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());
    when(conn.replicate(any(), anyList(), anyInt(), anyLong(), anyLong()))
      .then(i -> futures.get(next.getAndIncrement()));
    ServerCall<?> rpcCall1 = mock(ServerCall.class);
    WALKeyImpl key1 = mock(WALKeyImpl.class);
    when(key1.estimatedSerializedSizeOf()).thenReturn(100L);
    WALEdit edit1 = mock(WALEdit.class);
    when(edit1.estimatedSerializedSizeOf()).thenReturn(1000L);
    when(manager.increase(anyLong())).thenReturn(true);

    sink.add(key1, edit1, rpcCall1);
    verify(manager, times(1)).increase(anyLong());
    verify(manager, never()).decrease(anyLong());
    verify(rpcCall1, times(1)).retainByWAL();
    assertEquals(1100, sink.pendingSize());

    ServerCall<?> rpcCall2 = mock(ServerCall.class);
    WALKeyImpl key2 = mock(WALKeyImpl.class);
    when(key2.estimatedSerializedSizeOf()).thenReturn(200L);
    WALEdit edit2 = mock(WALEdit.class);
    when(edit2.estimatedSerializedSizeOf()).thenReturn(2000L);

    sink.add(key2, edit2, rpcCall2);
    verify(manager, times(2)).increase(anyLong());
    verify(manager, never()).decrease(anyLong());
    verify(rpcCall2, times(1)).retainByWAL();
    assertEquals(3300, sink.pendingSize());

    ServerCall<?> rpcCall3 = mock(ServerCall.class);
    WALKeyImpl key3 = mock(WALKeyImpl.class);
    when(key3.estimatedSerializedSizeOf()).thenReturn(200L);
    WALEdit edit3 = mock(WALEdit.class);
    when(edit3.estimatedSerializedSizeOf()).thenReturn(3000L);
    when(manager.increase(anyLong())).thenReturn(false);

    // should not buffer this edit
    sink.add(key3, edit3, rpcCall3);
    verify(manager, times(3)).increase(anyLong());
    verify(manager, times(1)).decrease(anyLong());
    // should retain and then release immediately
    verify(rpcCall3, times(1)).retainByWAL();
    verify(rpcCall3, times(1)).releaseByWAL();
    // should also clear the pending edit
    verify(rpcCall2, times(1)).releaseByWAL();
    assertEquals(1100, sink.pendingSize());
    // should have request flush
    verify(flushRequester, times(1)).run();

    // finish the replication for first edit, we should decrease the size, release the rpc call,and
    // the pendingSize should be 0 as there are no pending entries
    futures.forEach(f -> f.complete(null));
    verify(manager, times(2)).decrease(anyLong());
    verify(rpcCall1, times(1)).releaseByWAL();
    assertEquals(0, sink.pendingSize());

    // should only call replicate 2 times for replicating the first edit, as we have 2 secondary
    // replicas
    verify(conn, times(2)).replicate(any(), anyList(), anyInt(), anyLong(), anyLong());
  }

  @Test
  public void testNotAddToFailedReplicas() {
    MutableInt next = new MutableInt(0);
    List<CompletableFuture<Void>> futures =
      Stream.generate(() -> new CompletableFuture<Void>()).limit(4).collect(Collectors.toList());
    when(conn.replicate(any(), anyList(), anyInt(), anyLong(), anyLong()))
      .then(i -> futures.get(next.getAndIncrement()));

    ServerCall<?> rpcCall1 = mock(ServerCall.class);
    WALKeyImpl key1 = mock(WALKeyImpl.class);
    when(key1.estimatedSerializedSizeOf()).thenReturn(100L);
    when(key1.getSequenceId()).thenReturn(1L);
    WALEdit edit1 = mock(WALEdit.class);
    when(edit1.estimatedSerializedSizeOf()).thenReturn(1000L);
    when(manager.increase(anyLong())).thenReturn(true);
    sink.add(key1, edit1, rpcCall1);

    ServerCall<?> rpcCall2 = mock(ServerCall.class);
    WALKeyImpl key2 = mock(WALKeyImpl.class);
    when(key2.estimatedSerializedSizeOf()).thenReturn(200L);
    when(key2.getSequenceId()).thenReturn(3L);

    Map<byte[], List<Path>> committedFiles = td.getColumnFamilyNames().stream()
      .collect(Collectors.toMap(Function.identity(), k -> Collections.emptyList(), (u, v) -> {
        throw new IllegalStateException();
      }, () -> new TreeMap<>(Bytes.BYTES_COMPARATOR)));
    FlushDescriptor fd =
      ProtobufUtil.toFlushDescriptor(FlushAction.START_FLUSH, primary, 2L, committedFiles);
    WALEdit edit2 = WALEdit.createFlushWALEdit(primary, fd);
    sink.add(key2, edit2, rpcCall2);

    // fail the call to replica 2
    futures.get(0).complete(null);
    futures.get(1).completeExceptionally(new IOException("inject error"));

    // the failure should not cause replica 2 to be added to failedReplicas, as we have already
    // trigger a flush after it.
    verify(conn, times(4)).replicate(any(), anyList(), anyInt(), anyLong(), anyLong());

    futures.get(2).complete(null);
    futures.get(3).complete(null);

    // should have send out all so no pending entries.
    assertEquals(0, sink.pendingSize());
  }

  @Test
  public void testAddToFailedReplica() {
    MutableInt next = new MutableInt(0);
    List<CompletableFuture<Void>> futures =
      Stream.generate(() -> new CompletableFuture<Void>()).limit(5).collect(Collectors.toList());
    when(conn.replicate(any(), anyList(), anyInt(), anyLong(), anyLong()))
      .then(i -> futures.get(next.getAndIncrement()));

    ServerCall<?> rpcCall1 = mock(ServerCall.class);
    WALKeyImpl key1 = mock(WALKeyImpl.class);
    when(key1.estimatedSerializedSizeOf()).thenReturn(100L);
    when(key1.getSequenceId()).thenReturn(1L);
    WALEdit edit1 = mock(WALEdit.class);
    when(edit1.estimatedSerializedSizeOf()).thenReturn(1000L);
    when(manager.increase(anyLong())).thenReturn(true);
    sink.add(key1, edit1, rpcCall1);

    ServerCall<?> rpcCall2 = mock(ServerCall.class);
    WALKeyImpl key2 = mock(WALKeyImpl.class);
    when(key2.estimatedSerializedSizeOf()).thenReturn(200L);
    when(key2.getSequenceId()).thenReturn(1L);
    WALEdit edit2 = mock(WALEdit.class);
    when(edit2.estimatedSerializedSizeOf()).thenReturn(2000L);
    when(manager.increase(anyLong())).thenReturn(true);
    sink.add(key2, edit2, rpcCall2);

    // fail the call to replica 2
    futures.get(0).complete(null);
    futures.get(1).completeExceptionally(new IOException("inject error"));

    // we should only call replicate once for edit2, since replica 2 is marked as failed
    verify(conn, times(3)).replicate(any(), anyList(), anyInt(), anyLong(), anyLong());
    futures.get(2).complete(null);
    // should have send out all so no pending entries.
    assertEquals(0, sink.pendingSize());

    ServerCall<?> rpcCall3 = mock(ServerCall.class);
    WALKeyImpl key3 = mock(WALKeyImpl.class);
    when(key3.estimatedSerializedSizeOf()).thenReturn(200L);
    when(key3.getSequenceId()).thenReturn(3L);
    Map<byte[], List<Path>> committedFiles = td.getColumnFamilyNames().stream()
      .collect(Collectors.toMap(Function.identity(), k -> Collections.emptyList(), (u, v) -> {
        throw new IllegalStateException();
      }, () -> new TreeMap<>(Bytes.BYTES_COMPARATOR)));
    FlushDescriptor fd =
      ProtobufUtil.toFlushDescriptor(FlushAction.START_FLUSH, primary, 2L, committedFiles);
    WALEdit edit3 = WALEdit.createFlushWALEdit(primary, fd);
    sink.add(key3, edit3, rpcCall3);

    // the flush marker should have cleared the failedReplicas, so we will send the edit to 2
    // replicas again
    verify(conn, times(5)).replicate(any(), anyList(), anyInt(), anyLong(), anyLong());
    futures.get(3).complete(null);
    futures.get(4).complete(null);

    // should have send out all so no pending entries.
    assertEquals(0, sink.pendingSize());
  }
}
