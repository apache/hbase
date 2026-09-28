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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;

/**
 * Trivial test to confirm that we can get last flushed sequence id by encodedRegionName. See
 * HBASE-12715.
 */
@Tag(MediumTests.TAG)
public class TestGetLastFlushedSequenceId {

  private final HBaseTestingUtil testUtil = new HBaseTestingUtil();

  private final TableName tableName = TableName.valueOf(getClass().getSimpleName(), "test");

  private final byte[] family = Bytes.toBytes("f1");

  private final byte[][] families = new byte[][] { family };

  @BeforeEach
  public void setUp() throws Exception {
    testUtil.getConfiguration().setInt("hbase.regionserver.msginterval", 1000);
    testUtil.startMiniCluster();
  }

  @AfterEach
  public void tearDown() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    testUtil.getAdmin()
      .createNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString()).build());
    Table table = testUtil.createTable(tableName, families);
    table
      .put(new Put(Bytes.toBytes("k")).addColumn(family, Bytes.toBytes("q"), Bytes.toBytes("v")));
    SingleProcessHBaseCluster cluster = testUtil.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    Region region = null;
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region r : hrs.getRegions(tableName)) {
        region = r;
        break;
      }
    }
    assertNotNull(region);
    Thread.sleep(2000);
    RegionStoreSequenceIds ids = testUtil.getHBaseCluster().getMaster().getServerManager()
      .getLastFlushedSequenceId(region.getRegionInfo().getEncodedNameAsBytes());
    // This will be the sequenceid just before that of the earliest edit in memstore.
    long storeSequenceId = ids.getStoreSequenceId(0).getSequenceId();
    assertTrue(storeSequenceId > 0);
    // HBASE-30335: openSeqNum is now seeded on region OPEN, so lastFlushedSequenceId is no
    // longer NO_SEQNUM before the first flush.
    assertNotEquals(HConstants.NO_SEQNUM, ids.getLastFlushedSequenceId());
    testUtil.getAdmin().flush(tableName);
    Thread.sleep(2000);
    ids = testUtil.getHBaseCluster().getMaster().getServerManager()
      .getLastFlushedSequenceId(region.getRegionInfo().getEncodedNameAsBytes());
    assertTrue(ids.getLastFlushedSequenceId() > storeSequenceId,
      ids.getLastFlushedSequenceId() + " > " + storeSequenceId);
    assertEquals(ids.getLastFlushedSequenceId(), ids.getStoreSequenceId(0).getSequenceId());
    table.close();
  }

  /**
   * HBASE-30335: after a region is opened - and before any user write or flush - the master's
   * flushedSequenceIdByRegion must already contain the region's openSeqNum. Otherwise a subsequent
   * WAL split (e.g. the hosting RS crashes before its first flush heartbeat) would treat
   * already-durable edits as unflushed and produce orphaned recovered.edits.
   */
  @Test
  public void testFlushedSequenceIdSeededOnRegionOpen() throws IOException, InterruptedException {
    TableName freshTable = TableName.valueOf(getClass().getSimpleName(), "openseed");
    testUtil.getAdmin()
      .createNamespace(NamespaceDescriptor.create(freshTable.getNamespaceAsString()).build());
    Table table = testUtil.createTable(freshTable, families);
    try {
      SingleProcessHBaseCluster cluster = testUtil.getMiniHBaseCluster();
      HRegion region = null;
      for (JVMClusterUtil.RegionServerThread rst : cluster.getRegionServerThreads()) {
        for (HRegion r : rst.getRegionServer().getRegions(freshTable)) {
          region = r;
          break;
        }
        if (region != null) {
          break;
        }
      }
      assertNotNull(region);
      long openSeqNum = region.getOpenSeqNum();
      RegionStoreSequenceIds ids = testUtil.getHBaseCluster().getMaster().getServerManager()
        .getLastFlushedSequenceId(region.getRegionInfo().getEncodedNameAsBytes());
      assertNotEquals(HConstants.NO_SEQNUM, ids.getLastFlushedSequenceId(),
        "flushedSequenceIdByRegion should be seeded on region OPEN (HBASE-30335)");
      assertEquals(openSeqNum, ids.getLastFlushedSequenceId(),
        "seeded value must equal the region's openSeqNum");
    } finally {
      table.close();
    }
  }

  /**
   * HBASE-30335 follow-up: on graceful region CLOSE the RS reports the region's final durable
   * flushed seqid, and the master must lift its flushedSequenceIdByRegion watermark to that value.
   * This complements the OPEN-time seed and covers the drain-move / disable window: if the source
   * RS crashes after the close, a subsequent WAL split must see the already-durable edits as
   * flushed and not resurrect them as orphaned recovered.edits. Writes and flushes so the region's
   * maxFlushedSeqId advances well past its openSeqNum, then disables the table (a graceful close
   * that keeps the region - unlike delete/split/merge, disable does not call
   * ServerManager#removeRegion) and asserts the watermark reflects the post-close flushed seqid.
   */
  @Test
  public void testFlushedSequenceIdSeededOnRegionClose() throws IOException, InterruptedException {
    TableName freshTable = TableName.valueOf(getClass().getSimpleName(), "closeseed");
    testUtil.getAdmin()
      .createNamespace(NamespaceDescriptor.create(freshTable.getNamespaceAsString()).build());
    Table table = testUtil.createTable(freshTable, families);
    try {
      SingleProcessHBaseCluster cluster = testUtil.getMiniHBaseCluster();
      HRegion region = null;
      for (JVMClusterUtil.RegionServerThread rst : cluster.getRegionServerThreads()) {
        for (HRegion r : rst.getRegionServer().getRegions(freshTable)) {
          region = r;
          break;
        }
        if (region != null) {
          break;
        }
      }
      assertNotNull(region);
      long openSeqNum = region.getOpenSeqNum();
      // Write and flush a few times so the region's durable flushed seqid advances past openSeqNum;
      // this makes the CLOSE-time seed distinguishable from the OPEN-time seed.
      for (int i = 0; i < 3; i++) {
        table.put(new Put(Bytes.toBytes("k" + i)).addColumn(family, Bytes.toBytes("q"),
          Bytes.toBytes("v" + i)));
        testUtil.getAdmin().flush(freshTable);
      }
      long flushedSeqId = region.getMaxFlushedSeqId();
      assertTrue(flushedSeqId > openSeqNum,
        "test setup: flushed seqid " + flushedSeqId + " must exceed openSeqNum " + openSeqNum);
      byte[] encodedName = region.getRegionInfo().getEncodedNameAsBytes();
      // Gracefully close the region (disable keeps the region entry - it is not removed like a
      // delete/split/merge would), driving a CLOSED transition that carries the flushed seqid.
      testUtil.getAdmin().disableTable(freshTable);
      RegionStoreSequenceIds ids = testUtil.getHBaseCluster().getMaster().getServerManager()
        .getLastFlushedSequenceId(encodedName);
      assertNotEquals(HConstants.NO_SEQNUM, ids.getLastFlushedSequenceId(),
        "flushedSequenceIdByRegion should be seeded on region CLOSE");
      assertTrue(ids.getLastFlushedSequenceId() >= flushedSeqId,
        "CLOSE seed " + ids.getLastFlushedSequenceId() + " must be >= the region's flushed seqid "
          + flushedSeqId);
    } finally {
      table.close();
    }
  }
}
