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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;

/**
 * Unit tests for HBASE-30234: several size-tracking variables in the replication source pipeline
 * used {@code int} instead of {@code long}, causing integer overflow (and negative metrics / broken
 * throttling) once a batch exceeds {@link Integer#MAX_VALUE} (~2GB). These tests exercise each
 * fixed site with a value that overflows {@code int}.
 */
@Tag(ReplicationTests.TAG)
@Tag(SmallTests.TAG)
public class TestReplicationSizeOverflow {

  private static final RegionInfo RI =
    RegionInfoBuilder.newBuilder(TableName.valueOf("testReplicationSizeOverflow")).build();

  /** A byte count that is larger than Integer.MAX_VALUE and would wrap negative as an int. */
  private static final long OVER_2GB = 3_500_000_000L;

  /**
   * The sum of the bulk load store file sizes must not overflow when it exceeds ~2GB.
   * {@link ReplicationSourceWALReader#sizeOfStoreFilesIncludeBulkLoad} previously accumulated the
   * sizes into an int and cast on every step, wrapping the total to a negative value.
   */
  @Test
  public void testSizeOfStoreFilesIncludeBulkLoadDoesNotOverflow() {
    long size1 = 2_000_000_000L;
    long size2 = 1_500_000_000L;
    long expected = size1 + size2;
    // sanity: the total genuinely overflows a signed int
    assertTrue(expected > Integer.MAX_VALUE);

    Map<byte[], List<Path>> storeFiles = new HashMap<>();
    Map<String, Long> storeFilesSize = new HashMap<>();
    Path hfile1 = new Path("f1");
    storeFiles.put(Bytes.toBytes("f1"), Collections.singletonList(hfile1));
    storeFilesSize.put(hfile1.getName(), size1);
    Path hfile2 = new Path("f2");
    storeFiles.put(Bytes.toBytes("f2"), Collections.singletonList(hfile2));
    storeFilesSize.put(hfile2.getName(), size2);

    BulkLoadDescriptor desc = ProtobufUtil.toBulkLoadDescriptor(RI.getTable(),
      UnsafeByteOperations.unsafeWrap(RI.getEncodedNameAsBytes()), storeFiles, storeFilesSize, 1);
    WALEdit edit = WALEdit.createBulkLoadEvent(RI, desc);

    assertEquals(expected, ReplicationSourceWALReader.sizeOfStoreFilesIncludeBulkLoad(edit));
  }

  /**
   * The shipped-bytes metric must receive the full batch size. Previously
   * {@link MetricsSource#shipBatch} took an int {@code sizeInBytes}, so a &gt;2GB batch was
   * incremented into the counter as a negative value.
   */
  @Test
  public void testShipBatchDoesNotTruncateShippedBytes() {
    MetricsReplicationSourceSource single = mock(MetricsReplicationSourceSource.class);
    MetricsReplicationGlobalSourceSource global = mock(MetricsReplicationGlobalSourceSource.class);
    MetricsSource metrics = new MetricsSource("test-source", single, global, new HashMap<>());

    metrics.shipBatch(10L, OVER_2GB, 2L);

    // the full long size must reach both counters unchanged, not a truncated int
    verify(single).incrShippedBytes(OVER_2GB);
    verify(global).incrShippedBytes(OVER_2GB);
    verify(single).incrHFilesShipped(2L);
    verify(global).incrHFilesShipped(2L);
  }

  /**
   * The replicate context must carry the full batch size to the endpoint. Previously the
   * {@code size} field and its accessors were int, truncating a &gt;2GB batch.
   */
  @Test
  public void testReplicateContextSizeDoesNotOverflow() {
    ReplicationEndpoint.ReplicateContext ctx = new ReplicationEndpoint.ReplicateContext();
    ctx.setSize(OVER_2GB);
    assertEquals(OVER_2GB, ctx.getSize());
  }
}
