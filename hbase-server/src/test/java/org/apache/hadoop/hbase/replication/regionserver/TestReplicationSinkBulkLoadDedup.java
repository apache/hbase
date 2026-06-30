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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALEditInternalHelper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.UUID;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALKey;

/**
 * Unit tests for bulkload deduplication in {@link ReplicationSink}. Verifies that concurrent RPC
 * retries for the same bulkload event do not result in duplicate processing.
 */
@Tag(ReplicationTests.TAG)
@Tag(MediumTests.TAG)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestReplicationSinkBulkLoadDedup {

  private static final String CLUSTER_ID_A = "cluster-A";
  private static final String CLUSTER_ID_B = "cluster-B";
  private static final byte[] REGION_NAME = Bytes.toBytes("regionXYZ");
  private static final long SEQ_NUM = 100L;
  private static final TableName TABLE = TableName.valueOf("testDedup");
  private static final byte[] FAMILY = Bytes.toBytes("cf");

  private final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private ReplicationSink sink;
  private ZKWatcher zkw;

  @BeforeAll
  public void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set("hbase.replication.source.fs.conf.provider",
      TestSourceFSConfigurationProvider.class.getCanonicalName());
    TEST_UTIL.startMiniZKCluster();
    zkw = new ZKWatcher(conf, "replication-sink-bulkload-dedup", null);
    sink = new ReplicationSink(conf, null);
  }

  @AfterAll
  public void tearDownAfterClass() throws Exception {
    if (zkw != null) {
      zkw.close();
    }
    TEST_UTIL.shutdownMiniZKCluster();
  }

  /**
   * Verify that manually adding a key to inProgressBulkLoads blocks a second add for the same key,
   * and that remove restores it — core Set semantics that the dedup logic relies on.
   */
  @Test
  public void testInProgressSetAddAndRemove() {
    String key = CLUSTER_ID_A + "#" + Bytes.toString(REGION_NAME) + "#" + SEQ_NUM;

    assertTrue(sink.getInProgressBulkLoads().add(key), "First add should succeed");
    assertFalse(sink.getInProgressBulkLoads().add(key),
      "Second add should fail (already in progress)");

    sink.getInProgressBulkLoads().remove(key);
    assertTrue(sink.getInProgressBulkLoads().add(key),
      "Add after remove should succeed (retry allowed)");
    sink.getInProgressBulkLoads().remove(key); // cleanup
  }

  /**
   * Verify that keys from different source clusters with the same region/seqNum are treated as
   * distinct entries and do not block each other.
   */
  @Test
  public void testDifferentClustersDontConflict() {
    String keyA = CLUSTER_ID_A + "#" + Bytes.toString(REGION_NAME) + "#" + SEQ_NUM;
    String keyB = CLUSTER_ID_B + "#" + Bytes.toString(REGION_NAME) + "#" + SEQ_NUM;

    assertTrue(sink.getInProgressBulkLoads().add(keyA));
    assertTrue(sink.getInProgressBulkLoads().add(keyB),
      "Same region/seqNum from different cluster should not conflict");

    sink.getInProgressBulkLoads().remove(keyA);
    sink.getInProgressBulkLoads().remove(keyB);
    assertEquals(0, sink.getInProgressBulkLoads().size());
  }

  /**
   * End-to-end: replicateEntries() with a bulkload WAL cell that has replicate=false should not
   * register any key in inProgressBulkLoads.
   */
  @Test
  public void testNonReplicateBulkLoadNotTracked() throws Exception {
    WALProtos.BulkLoadDescriptor bld = buildBulkLoadDescriptor(REGION_NAME, SEQ_NUM, false);
    WALEdit edit = buildWALEdit(bld);
    List<WALEntry> entries = buildWALEntries(edit);

    int before = sink.getInProgressBulkLoads().size();
    sink.replicateEntries(entries,
      PrivateCellUtil
        .createExtendedCellScanner(WALEditInternalHelper.getExtendedCells(edit).iterator()),
      CLUSTER_ID_A, "/dummy/namespace", "/dummy/archive");

    assertEquals(before, sink.getInProgressBulkLoads().size(),
      "Non-replicate bulkload should not add key to inProgressBulkLoads");
  }

  /**
   * Simulate a concurrent retry: manually pre-populate the key to mimic a first call still in
   * progress, then verify replicateEntries() skips the bulkload cell without throwing.
   */
  @Test
  public void testConcurrentRetryIsSkipped() throws Exception {
    WALProtos.BulkLoadDescriptor bld = buildBulkLoadDescriptor(REGION_NAME, SEQ_NUM + 1, true);
    String key = CLUSTER_ID_A + "#" + Bytes.toString(REGION_NAME) + "#" + (SEQ_NUM + 1);

    // Simulate first call still in progress
    sink.getInProgressBulkLoads().add(key);

    WALEdit edit = buildWALEdit(bld);
    List<WALEntry> entries = buildWALEntries(edit);

    // Second call (retry) should skip without exception
    sink.replicateEntries(entries,
      PrivateCellUtil
        .createExtendedCellScanner(WALEditInternalHelper.getExtendedCells(edit).iterator()),
      CLUSTER_ID_A, "/dummy/namespace", "/dummy/archive");

    // Key still held by the "first call"
    assertTrue(sink.getInProgressBulkLoads().contains(key));
    sink.getInProgressBulkLoads().remove(key); // cleanup
  }

  @Test
  public void testCompletedZkBulkLoadEventIsSkippedBySink() throws Exception {
    WALProtos.BulkLoadDescriptor bld = buildBulkLoadDescriptor(REGION_NAME, SEQ_NUM + 2, true);
    WALEdit edit = buildWALEdit(bld);
    List<WALEntry> entries = buildWALEntries(edit);

    ReplicationBulkLoadEventTracker tracker =
      new ReplicationBulkLoadEventTracker(TEST_UTIL.getConfiguration(), zkw);
    ReplicationBulkLoadEventTracker.Event event = tracker.newEvent(CLUSTER_ID_A, TABLE, REGION_NAME,
      SEQ_NUM + 2, entries.get(0).getKey().getWriteTime());
    tracker.markDone(event);

    ReplicationSink zkSink = new ReplicationSink(TEST_UTIL.getConfiguration(), null, zkw);
    zkSink.replicateEntries(entries,
      PrivateCellUtil
        .createExtendedCellScanner(WALEditInternalHelper.getExtendedCells(edit).iterator()),
      CLUSTER_ID_A, "/missing/namespace", "/missing/archive");

    assertTrue(tracker.isDone(event));
    assertFalse(tracker.isInProgress(event));
  }

  // ---- helpers ----

  private WALProtos.BulkLoadDescriptor buildBulkLoadDescriptor(byte[] regionName, long seqNum,
    boolean replicate) {
    WALProtos.StoreDescriptor store =
      WALProtos.StoreDescriptor.newBuilder().setFamilyName(UnsafeByteOperations.unsafeWrap(FAMILY))
        .setStoreHomeDir(Bytes.toString(FAMILY)).addStoreFile("hfile-0").setStoreFileSizeBytes(1024)
        .build();
    return WALProtos.BulkLoadDescriptor.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(TABLE))
      .setEncodedRegionName(UnsafeByteOperations.unsafeWrap(regionName)).addStores(store)
      .setBulkloadSeqNum(seqNum).setReplicate(replicate).build();
  }

  private WALEdit buildWALEdit(WALProtos.BulkLoadDescriptor bld) {
    // RegionInfo is only used to construct the WAL cell row key; dedup logic reads
    // encodedRegionName from BulkLoadDescriptor directly, so any RegionInfo works here.
    org.apache.hadoop.hbase.client.RegionInfo ri =
      org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(TABLE).build();
    return WALEdit.createBulkLoadEvent(ri, bld);
  }

  private List<WALEntry> buildWALEntries(WALEdit edit) {
    WALEntry.Builder builder = WALEntry.newBuilder();
    builder.setAssociatedCellCount(edit.getCells().size());
    WALKey.Builder keyBuilder = WALKey.newBuilder();
    UUID.Builder uuidBuilder = UUID.newBuilder();
    uuidBuilder.setLeastSigBits(HConstants.DEFAULT_CLUSTER_ID.getLeastSignificantBits());
    uuidBuilder.setMostSigBits(HConstants.DEFAULT_CLUSTER_ID.getMostSignificantBits());
    keyBuilder.setClusterId(uuidBuilder.build());
    keyBuilder.setTableName(UnsafeByteOperations.unsafeWrap(TABLE.getName()));
    keyBuilder.setWriteTime(System.currentTimeMillis());
    keyBuilder.setEncodedRegionName(UnsafeByteOperations.unsafeWrap(REGION_NAME));
    keyBuilder.setLogSequenceNumber(-1);
    builder.setKey(keyBuilder.build());
    return Collections.singletonList(builder.build());
  }
}
