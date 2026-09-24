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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestMasterAbortWhileSplittingRegion {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestMasterAbortWhileSplittingRegion.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("testSplit");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] SPLIT_KEY = Bytes.toBytes("row5");

  private static final CountDownLatch SPLIT_META_UPDATED = new CountDownLatch(1);

  @BeforeAll
  public static void setupCluster() throws Exception {
    UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      SplitRegionObserver.class.getName());
    UTIL.startMiniCluster(3);
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterAll
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
  public void test() throws Exception {
    try (Admin admin = UTIL.getAdmin();
      Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < 10; i++) {
        table.put(new Put(Bytes.toBytes("row" + i)).addColumn(CF, CF, CF));
      }
      List<RegionInfo> regionInfos = admin.getRegions(TABLE_NAME);
      SplitTableRegionProcedure splitProcedure = new SplitTableRegionProcedure(
        UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment(),
        regionInfos.get(0), SPLIT_KEY);
      long procId = UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
        .submitProcedure(splitProcedure);
      SPLIT_META_UPDATED.await();
      UTIL.getMiniHBaseCluster().stopMaster(0);
      UTIL.getMiniHBaseCluster().startMaster();
      UTIL.waitFor(30000,
        () -> UTIL.getMiniHBaseCluster().getMaster() != null
          && UTIL.getMiniHBaseCluster().getMaster().isInitialized());
      UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
        .isFinished(procId));
      assertTrue(UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
        .getRegionsInTransition().isEmpty(), "Found region RIT, that's impossible! "
          + UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionsInTransition());
      assertEquals(10, UTIL.countRows(TABLE_NAME),
        "Split should keep all rows after master failover");
      assertEquals(2, admin.getRegions(TABLE_NAME).size(),
        "Split should leave two daughter regions online");
    }
  }

  public static class SplitRegionObserver implements MasterCoprocessor, MasterObserver {

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preSplitRegionAfterMETAAction(
      ObserverContext<MasterCoprocessorEnvironment> ctx) {
      SPLIT_META_UPDATED.countDown();
    }
  }
}
