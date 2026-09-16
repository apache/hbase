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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionMetricsBuilder;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestServerManager {

  private static final class DummyMasterServices extends MockNoopMasterServices {
    private final AssignmentManager am;

    DummyMasterServices(Configuration conf) {
      super(conf);
      am = mock(AssignmentManager.class);
      RegionStates rss = mock(RegionStates.class);
      when(am.getRegionStates()).thenReturn(rss);
    }

    @Override
    public AssignmentManager getAssignmentManager() {
      return am;
    }
  }

  private ServerManager sm;
  private RegionInfo region;

  @BeforeEach
  public void setUp() {
    Configuration conf = HBaseConfiguration.create();
    sm = new ServerManager(new DummyMasterServices(conf), new DummyRegionServerList());
    region = RegionInfoBuilder.newBuilder(TableName.valueOf("t")).build();
  }

  private long lastFlushed(RegionInfo ri) {
    return sm.getLastFlushedSequenceId(ri.getEncodedNameAsBytes()).getLastFlushedSequenceId();
  }

  @Test
  public void testReportRegionOpenSeedsFlushedSequenceId() {
    assertEquals(HConstants.NO_SEQNUM, lastFlushed(region));
    sm.reportRegionOpen(region, 42L);
    assertEquals(42L, lastFlushed(region));
  }

  @Test
  public void testReportRegionOpenDoesNotRegressExistingValue() {
    sm.reportRegionOpen(region, 100L);
    // A later OPEN carrying a smaller openSeqNum (e.g. after a restart replayed less) must not
    // clobber a higher watermark already seeded here or supplied by a heartbeat.
    sm.reportRegionOpen(region, 50L);
    assertEquals(100L, lastFlushed(region));
  }

  @Test
  public void testReportRegionOpenIgnoresNoSeqNum() {
    sm.reportRegionOpen(region, HConstants.NO_SEQNUM);
    assertEquals(HConstants.NO_SEQNUM, lastFlushed(region));
  }

  @Test
  public void testReportRegionOpenIgnoresNegativeSeqNum() {
    sm.reportRegionOpen(region, -5L);
    assertEquals(HConstants.NO_SEQNUM, lastFlushed(region));
  }

  /**
   * HBASE-30335: the OPEN-time seed ({@link ServerManager#reportRegionOpen}, which uses
   * {@code merge(Math::max)}) and the heartbeat handler ({@link ServerManager#regionServerReport})
   * both write {@code flushedSequenceIdByRegion}. A stale in-flight heartbeat from the
   * soon-to-be-dead source RS carries a lower {@code completedSequenceId}. Whatever the
   * interleaving, the watermark must never regress below the seed: if the heartbeat lands first the
   * seed lifts it to {@code openSeqNum}; if it lands after, the heartbeat's read-modify-write must
   * refuse to lower it. Only a non-atomic check-then-put in the heartbeat path (the pre-fix bug)
   * could let the stale value clobber the seed. This drives both writers concurrently over many
   * rounds to catch that race.
   */
  @Test
  public void testConcurrentStaleHeartbeatDoesNotClobberOpenSeed() throws Exception {
    final long seedSeqId = 200L;
    final long staleSeqId = 100L;
    // Register the server so regionServerReport takes the heartbeat (updateLastFlushedSequenceIds)
    // path instead of the new-server-registration path.
    ServerName sn = ServerName.valueOf("rs.example.org", 16020, 1L);
    sm.recordNewServerWithLock(sn, ServerMetricsBuilder.of(sn));

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < 500; i++) {
        // Fresh region per round so no round is masked by a prior round's watermark.
        final RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.valueOf("concurrentSeed"))
          .setStartKey(Bytes.toBytes(i)).setEndKey(Bytes.toBytes(i + 1)).build();
        final ServerMetrics staleReport = ServerMetricsBuilder.newBuilder(sn)
          .setRegionMetrics(Collections.singletonList(RegionMetricsBuilder
            .newBuilder(ri.getRegionName()).setCompletedSequenceId(staleSeqId).build()))
          .build();
        final CyclicBarrier barrier = new CyclicBarrier(2);
        Future<?> seedTask = pool.submit(() -> {
          await(barrier);
          sm.reportRegionOpen(ri, seedSeqId);
        });
        Future<?> heartbeatTask = pool.submit(() -> {
          await(barrier);
          try {
            sm.regionServerReport(sn, staleReport);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
        seedTask.get(30, TimeUnit.SECONDS);
        heartbeatTask.get(30, TimeUnit.SECONDS);
        assertTrue(lastFlushed(ri) >= seedSeqId, "round " + i + ": watermark regressed to "
          + lastFlushed(ri) + ", stale heartbeat clobbered the openSeqNum seed");
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private static void await(CyclicBarrier barrier) {
    try {
      barrier.await(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
