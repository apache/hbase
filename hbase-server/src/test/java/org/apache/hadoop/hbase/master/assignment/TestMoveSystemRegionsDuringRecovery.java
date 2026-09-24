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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestMoveSystemRegionsDuringRecovery {

  private static final class AssignmentManagerForTest extends AssignmentManager {
    private volatile ServerName blockedServer;
    private final CountDownLatch metaOpening = new CountDownLatch(1);
    private final CountDownLatch resumeReport = new CountDownLatch(1);

    AssignmentManagerForTest(MasterServices master, MasterRegion masterRegion) {
      super(master, masterRegion);
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
      ReportRegionStateTransitionRequest request) throws PleaseHoldException {
      if (ProtobufUtil.toServerName(request.getServer()).equals(blockedServer)) {
        for (RegionStateTransition transition : request.getTransitionList()) {
          if (
            transition.getTransitionCode() == TransitionCode.OPENED
              && ProtobufUtil.toRegionInfo(transition.getRegionInfo(0)).isMetaRegion()
          ) {
            metaOpening.countDown();
            try {
              if (!resumeReport.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to resume the meta OPENED report");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new AssertionError(e);
            }
          }
        }
      }
      return super.reportRegionStateTransition(request);
    }
  }

  public static final class MasterForTest extends HMaster {
    private volatile ServerName newestServer;

    public MasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master,
      MasterRegion masterRegion) {
      return new AssignmentManagerForTest(master, masterRegion);
    }

    @Override
    public String getRegionServerVersion(ServerName server) {
      return server.equals(newestServer) ? "4.0.0" : "2.6.4";
    }
  }

  @Test
  public void testMetaRecoveryAfterCompatibilityCheck() throws Exception {
    HBaseTestingUtil util = new HBaseTestingUtil();
    util.getConfiguration().setClass(HConstants.MASTER_IMPL, MasterForTest.class, HMaster.class);
    AssignmentManagerForTest am = null;
    try {
      util.startMiniCluster(3);
      util.getAdmin().balancerSwitch(false, true);
      TableName tableName = TableName.valueOf("testMetaRecoveryAfterCompatibilityCheck");
      util.createTable(tableName, Bytes.toBytes("cf")).close();
      util.waitUntilNoRegionsInTransition();
      MasterForTest master = (MasterForTest) util.getMiniHBaseCluster().getMaster();
      am = (AssignmentManagerForTest) master.getAssignmentManager();
      RegionInfo meta = RegionInfoBuilder.FIRST_META_REGIONINFO;
      RegionStateNode node = am.getRegionStates().getRegionStateNode(meta);
      ServerName source = node.getRegionLocation();
      ServerName destination = master.getServerManager().getOnlineServersList().stream()
        .filter(s -> !s.equals(source)).findFirst().get();

      // Keep the existing TRSP in OPENING without holding the RegionStateNode lock.
      am.blockedServer = source;
      Future<byte[]> move = am.moveAsync(new RegionPlan(meta, source, source));
      assertTrue(am.metaOpening.await(30, TimeUnit.SECONDS));
      assertEquals(State.OPENING, node.getState());
      TransitRegionStateProcedure original = node.getProcedure();
      assertNotNull(original);

      // A newer RS becomes available while meta is still opening on the old RS.
      master.newestServer = destination;
      AssignmentManager checker = spy(am);
      CompletableFuture<Thread> checkThread = new CompletableFuture<>();
      doAnswer(invocation -> {
        checkThread.complete(Thread.currentThread());
        return invocation.callRealMethod();
      }).when(checker).getExcludedServersForSystemTable();
      checker.checkIfShouldMoveSystemRegionAsync();
      Thread thread = checkThread.get(10, TimeUnit.SECONDS);
      thread.join(TimeUnit.SECONDS.toMillis(10));
      assertFalse(thread.isAlive());
      assertSame(original, node.getProcedure());
      verify(checker, never()).moveAsync(any());

      util.getMiniHBaseCluster().killRegionServer(source);
      util.waitFor(30000, () -> !master.getServerManager().isServerOnline(source));
      am.resumeReport.countDown();
      move.get(60, TimeUnit.SECONDS);
      util.waitUntilNoRegionsInTransition();
      util.waitFor(60000,
        () -> master.getProcedures().stream().filter(p -> p instanceof ServerCrashProcedure)
          .map(p -> (ServerCrashProcedure) p)
          .anyMatch(p -> p.getServerName().equals(source) && p.isSuccess()));
      assertEquals(State.OPEN, node.getState());
      assertEquals(destination, node.getRegionLocation());
      assertTrue(master.getServerManager().isServerOnline(node.getRegionLocation()));
      // A client lookup must read meta successfully after recovery.
      try (org.apache.hadoop.hbase.client.RegionLocator locator =
        util.getConnection().getRegionLocator(tableName)) {
        assertNotNull(locator.getRegionLocation(HConstants.EMPTY_START_ROW, true));
      }
      assertEquals(1, util.getAdmin().getRegions(tableName).size());
    } finally {
      if (am != null) {
        am.resumeReport.countDown();
      }
      util.shutdownMiniCluster();
    }
  }
}
