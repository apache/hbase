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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

@Category({ MasterTests.class, MediumTests.class })
public class TestCloseAnOpeningRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCloseAnOpeningRegion.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("race");

  private static byte[] CF = Bytes.toBytes("cf");

  private static volatile CountDownLatch ARRIVE;

  private static volatile CountDownLatch RESUME;

  public static final class MockHMaster extends HMaster {

    public MockHMaster(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master) {
      return new AssignmentManager(master) {

        @Override
        public ReportRegionStateTransitionResponse reportRegionStateTransition(
            ReportRegionStateTransitionRequest req) throws PleaseHoldException {
          ReportRegionStateTransitionResponse resp = super.reportRegionStateTransition(req);
          TransitionCode code = req.getTransition(0).getTransitionCode();
          if (code == TransitionCode.OPENED && ARRIVE != null) {
            ARRIVE.countDown();
            try {
              RESUME.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
          return resp;
        }
      };
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY, 60000);
    UTIL.startMiniCluster(
      StartMiniClusterOption.builder().numRegionServers(2).masterClass(MockHMaster.class).build());
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    ARRIVE = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    RegionInfo region = UTIL.getAdmin().getRegions(TABLE_NAME).get(0);
    HRegionServer src = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    HRegionServer dst = UTIL.getOtherRegionServer(src);
    Thread move0 = new Thread(() -> {
      try {
        UTIL.getAdmin().move(region.getEncodedNameAsBytes(), dst.getServerName());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    move0.start();
    ARRIVE.await();
    Thread move1 = new Thread(() -> {
      try {
        UTIL.getAdmin().move(region.getEncodedNameAsBytes(), src.getServerName());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    move1.start();
    // No simple way to determine when it is safe to go on and produce the race so let's sleep for a
    // well...
    Thread.sleep(10000);
    RESUME.countDown();
    move0.join();
    move1.join();
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      // make sure that we can write to the table, which means the region is online
      table.put(new Put(Bytes.toBytes(0)).addColumn(CF, Bytes.toBytes("cq"), Bytes.toBytes(0)));
    }
  }
}
