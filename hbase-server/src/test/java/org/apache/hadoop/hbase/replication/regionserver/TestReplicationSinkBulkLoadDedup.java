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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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
    sink = new ReplicationSink(conf, null, zkw);
  }

  @AfterAll
  public void tearDownAfterClass() throws Exception {
    if (zkw != null) {
      zkw.close();
    }
    TEST_UTIL.shutdownMiniZKCluster();
  }

  /**
   * End-to-end: replicateEntries() with a bulkload WAL cell that has replicate=false should not
   * create any replicated bulk load event marker.
   */
  @Test
  public void testNonReplicateBulkLoadNotTracked() throws Exception {
    WALProtos.BulkLoadDescriptor bld = buildBulkLoadDescriptor(REGION_NAME, SEQ_NUM, false);
    WALEdit edit = buildWALEdit(bld);
    long writeTime = System.currentTimeMillis();
    List<WALEntry> entries = buildWALEntries(TABLE, REGION_NAME, edit, writeTime);
    ReplicationBulkLoadEventTracker tracker =
      new ZKReplicationBulkLoadEventTracker(TEST_UTIL.getConfiguration(), zkw);
    ReplicationBulkLoadEventTracker.Event event =
      tracker.newEvent(CLUSTER_ID_A, TABLE, REGION_NAME, SEQ_NUM, writeTime);

    assertFalse(tracker.isInProgress(event));
    assertFalse(tracker.isDone(event));
    sink.replicateEntries(entries,
      PrivateCellUtil
        .createExtendedCellScanner(WALEditInternalHelper.getExtendedCells(edit).iterator()),
      CLUSTER_ID_A, "/dummy/namespace", "/dummy/archive");

    assertFalse(tracker.isInProgress(event));
    assertFalse(tracker.isDone(event));
  }

  @Test
  public void testCompletedZkBulkLoadEventIsSkippedBySink() throws Exception {
    WALProtos.BulkLoadDescriptor bld = buildBulkLoadDescriptor(REGION_NAME, SEQ_NUM + 2, true);
    WALEdit edit = buildWALEdit(bld);
    long writeTime = System.currentTimeMillis();
    List<WALEntry> entries = buildWALEntries(TABLE, REGION_NAME, edit, writeTime);

    ReplicationBulkLoadEventTracker tracker =
      new ZKReplicationBulkLoadEventTracker(TEST_UTIL.getConfiguration(), zkw);
    ReplicationBulkLoadEventTracker.Event event =
      tracker.newEvent(CLUSTER_ID_A, TABLE, REGION_NAME, SEQ_NUM + 2, writeTime);
    tracker.markDone(event);

    ReplicationSink zkSink = new ReplicationSink(TEST_UTIL.getConfiguration(), null, zkw);
    zkSink.replicateEntries(entries,
      PrivateCellUtil
        .createExtendedCellScanner(WALEditInternalHelper.getExtendedCells(edit).iterator()),
      CLUSTER_ID_A, "/missing/namespace", "/missing/archive");

    assertTrue(tracker.isDone(event));
    assertFalse(tracker.isInProgress(event));
  }

  @Test
  public void testLoadedEventIsMarkedDoneButFailedEventStaysInProgress() throws Exception {
    TableName loadedTable = TableName.valueOf("testLoadedEvent");
    TableName failedTable = TableName.valueOf("testFailedEvent");
    long loadedSeqNum = SEQ_NUM + 3;
    long failedSeqNum = SEQ_NUM + 4;
    long writeTime = System.currentTimeMillis();
    RecordingBulkLoadEventTracker tracker = new RecordingBulkLoadEventTracker();
    HFileReplicator hFileReplicator = mock(HFileReplicator.class);
    doAnswer(invocation -> {
      HFileReplicator.BulkLoadTableLoadListener listener = invocation.getArgument(0);
      listener.tableLoaded(loadedTable.getNameWithNamespaceInclAsString());
      listener.tableLoadFailed(failedTable.getNameWithNamespaceInclAsString());
      throw new IOException("failed after loading started");
    }).when(hFileReplicator).replicate(any(HFileReplicator.BulkLoadTableLoadListener.class));
    ReplicationSink testSink =
      new TestableReplicationSink(TEST_UTIL.getConfiguration(), tracker, hFileReplicator);

    WALProtos.BulkLoadDescriptor loadedBld =
      buildBulkLoadDescriptor(loadedTable, REGION_NAME, loadedSeqNum, true);
    WALProtos.BulkLoadDescriptor failedBld =
      buildBulkLoadDescriptor(failedTable, REGION_NAME, failedSeqNum, true);
    WALEdit loadedEdit = buildWALEdit(loadedTable, loadedBld);
    WALEdit failedEdit = buildWALEdit(failedTable, failedBld);
    List<WALEntry> entries = new ArrayList<>();
    entries.add(buildWALEntry(loadedTable, REGION_NAME, loadedEdit, writeTime));
    entries.add(buildWALEntry(failedTable, REGION_NAME, failedEdit, writeTime));
    List<ExtendedCell> cells = new ArrayList<>();
    cells.addAll(WALEditInternalHelper.getExtendedCells(loadedEdit));
    cells.addAll(WALEditInternalHelper.getExtendedCells(failedEdit));

    IOException error = assertThrows(IOException.class,
      () -> testSink.replicateEntries(entries,
        PrivateCellUtil.createExtendedCellScanner(cells.iterator()), CLUSTER_ID_A,
        "/dummy/namespace", "/dummy/archive"));

    assertEquals("failed after loading started", error.getMessage());
    ReplicationBulkLoadEventTracker.Event loadedEvent =
      tracker.getEvent(loadedTable, REGION_NAME, loadedSeqNum);
    ReplicationBulkLoadEventTracker.Event failedEvent =
      tracker.getEvent(failedTable, REGION_NAME, failedSeqNum);
    assertTrue(tracker.doneEvents.contains(loadedEvent));
    assertTrue(tracker.releasedEvents.contains(loadedEvent));
    assertFalse(tracker.doneEvents.contains(failedEvent));
    assertFalse(tracker.releasedEvents.contains(failedEvent));
  }

  // ---- helpers ----

  private WALProtos.BulkLoadDescriptor buildBulkLoadDescriptor(byte[] regionName, long seqNum,
    boolean replicate) {
    return buildBulkLoadDescriptor(TABLE, regionName, seqNum, replicate);
  }

  private WALProtos.BulkLoadDescriptor buildBulkLoadDescriptor(TableName table, byte[] regionName,
    long seqNum, boolean replicate) {
    WALProtos.StoreDescriptor store =
      WALProtos.StoreDescriptor.newBuilder().setFamilyName(UnsafeByteOperations.unsafeWrap(FAMILY))
        .setStoreHomeDir(Bytes.toString(FAMILY)).addStoreFile("hfile-0").setStoreFileSizeBytes(1024)
        .build();
    return WALProtos.BulkLoadDescriptor.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(table))
      .setEncodedRegionName(UnsafeByteOperations.unsafeWrap(regionName)).addStores(store)
      .setBulkloadSeqNum(seqNum).setReplicate(replicate).build();
  }

  private WALEdit buildWALEdit(WALProtos.BulkLoadDescriptor bld) {
    return buildWALEdit(TABLE, bld);
  }

  private WALEdit buildWALEdit(TableName table, WALProtos.BulkLoadDescriptor bld) {
    // RegionInfo is only used to construct the WAL cell row key; dedup logic reads
    // encodedRegionName from BulkLoadDescriptor directly, so any RegionInfo works here.
    org.apache.hadoop.hbase.client.RegionInfo ri =
      org.apache.hadoop.hbase.client.RegionInfoBuilder.newBuilder(table).build();
    return WALEdit.createBulkLoadEvent(ri, bld);
  }

  private List<WALEntry> buildWALEntries(TableName table, byte[] regionName, WALEdit edit,
    long writeTime) {
    return Collections.singletonList(buildWALEntry(table, regionName, edit, writeTime));
  }

  private WALEntry buildWALEntry(TableName table, byte[] regionName, WALEdit edit, long writeTime) {
    WALEntry.Builder builder = WALEntry.newBuilder();
    builder.setAssociatedCellCount(edit.getCells().size());
    WALKey.Builder keyBuilder = WALKey.newBuilder();
    UUID.Builder uuidBuilder = UUID.newBuilder();
    uuidBuilder.setLeastSigBits(HConstants.DEFAULT_CLUSTER_ID.getLeastSignificantBits());
    uuidBuilder.setMostSigBits(HConstants.DEFAULT_CLUSTER_ID.getMostSignificantBits());
    keyBuilder.setClusterId(uuidBuilder.build());
    keyBuilder.setTableName(UnsafeByteOperations.unsafeWrap(table.getName()));
    keyBuilder.setWriteTime(writeTime);
    keyBuilder.setEncodedRegionName(UnsafeByteOperations.unsafeWrap(regionName));
    keyBuilder.setLogSequenceNumber(-1);
    builder.setKey(keyBuilder.build());
    return builder.build();
  }

  private static final class TestableReplicationSink extends ReplicationSink {
    private final HFileReplicator hFileReplicator;

    TestableReplicationSink(Configuration conf, ReplicationBulkLoadEventTracker tracker,
      HFileReplicator hFileReplicator) throws IOException {
      super(conf, null, tracker);
      this.hFileReplicator = hFileReplicator;
    }

    @Override
    HFileReplicator createHFileReplicator(Configuration providerConf,
      String sourceBaseNamespaceDirPath, String sourceHFileArchiveDirPath,
      Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap, List<String> sourceClusterIds)
      throws IOException {
      return hFileReplicator;
    }
  }

  private static final class RecordingBulkLoadEventTracker
    implements ReplicationBulkLoadEventTracker {
    private final Map<String, ReplicationBulkLoadEventTracker.Event> events = new HashMap<>();
    private final List<ReplicationBulkLoadEventTracker.Event> doneEvents = new ArrayList<>();
    private final List<ReplicationBulkLoadEventTracker.Event> releasedEvents = new ArrayList<>();

    @Override
    public ReplicationBulkLoadEventTracker.Event newEvent(String replicationClusterId,
      TableName table, byte[] encodedRegionName, long bulkLoadSeqNum, long writeTime) {
      ReplicationBulkLoadEventTracker.Event event = new ReplicationBulkLoadEventTracker.Event("0",
        table.getNameWithNamespaceInclAsString() + '#' + bulkLoadSeqNum, null);
      events.put(key(table, encodedRegionName, bulkLoadSeqNum), event);
      return event;
    }

    @Override
    public ReplicationBulkLoadEventTracker.ClaimResult
      claim(ReplicationBulkLoadEventTracker.Event event) {
      return ReplicationBulkLoadEventTracker.ClaimResult.CLAIMED;
    }

    @Override
    public void markDone(ReplicationBulkLoadEventTracker.Event event) {
      doneEvents.add(event);
    }

    @Override
    public void release(ReplicationBulkLoadEventTracker.Event event) {
      releasedEvents.add(event);
    }

    @Override
    public boolean isInProgress(ReplicationBulkLoadEventTracker.Event event) {
      return false;
    }

    @Override
    public boolean isDone(ReplicationBulkLoadEventTracker.Event event) {
      return doneEvents.contains(event);
    }

    @Override
    public int cleanDoneMarkers(long ttlMs) {
      return 0;
    }

    ReplicationBulkLoadEventTracker.Event getEvent(TableName table, byte[] encodedRegionName,
      long bulkLoadSeqNum) {
      return events.get(key(table, encodedRegionName, bulkLoadSeqNum));
    }

    private String key(TableName table, byte[] encodedRegionName, long bulkLoadSeqNum) {
      return table.getNameWithNamespaceInclAsString() + '#'
        + Bytes.toStringBinary(encodedRegionName) + '#' + bulkLoadSeqNum;
    }
  }
}
