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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;

/**
 * UT for HBASE-25032.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestAssignRegionToUninitializedRegionServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAssignRegionToUninitializedRegionServer.class);

  private static CountDownLatch ARRIVE;

  private static CountDownLatch RESUME;

  private static AtomicBoolean ASSIGN_CALLED = new AtomicBoolean(false);

  public static final class RSRpcServicesForTest extends RSRpcServices {

    public RSRpcServicesForTest(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ExecuteProceduresResponse executeProcedures(RpcController controller,
      ExecuteProceduresRequest request) throws ServiceException {
      if (request.getOpenRegionCount() > 0) {
        ASSIGN_CALLED.set(true);
      }
      return super.executeProcedures(controller, request);
    }
  }

  public static final class RegionServerForTest extends HRegionServer {

    public RegionServerForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected void tryRegionServerReport(long reportStartTime, long reportEndTime)
      throws IOException {
      if (ARRIVE != null) {
        ARRIVE.countDown();
        ARRIVE = null;
        try {
          RESUME.await();
        } catch (InterruptedException e) {
        }
      }
      super.tryRegionServerReport(reportStartTime, reportEndTime);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new RSRpcServicesForTest(this);
    }
  }

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("test");

  private static byte[] FAMILY = Bytes.toBytes("family");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    UTIL.createTable(NAME, FAMILY);
    UTIL.waitTableAvailable(NAME);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMove() throws Exception {
    UTIL.getMiniHBaseCluster().getConfiguration().setClass(HConstants.REGION_SERVER_IMPL,
      RegionServerForTest.class, HRegionServer.class);
    CountDownLatch arrive = new CountDownLatch(1);
    ARRIVE = arrive;
    RESUME = new CountDownLatch(1);
    // restart a new region server, and wait until it finish initialization and want to call
    // regionServerReport, so it will load the peer state to peer cache.
    Future<HRegionServer> regionServerFuture = ForkJoinPool.commonPool()
      .submit(() -> UTIL.getMiniHBaseCluster().startRegionServer().getRegionServer());
    ARRIVE.await();
    // try move region to the new region server, it will fail, but we need to make sure that we do
    // not try to assign it to the new server.
    HRegionServer src = UTIL.getRSForFirstRegionInTable(NAME);
    HRegionServer dst = UTIL.getOtherRegionServer(src);
    try {
      UTIL.getAdmin().move(UTIL.getAdmin().getRegions(NAME).get(0).getEncodedNameAsBytes(),
        dst.getServerName());
      // assert the region should still on the original region server, and we didn't call assign to
      // the new server
      assertSame(src, UTIL.getRSForFirstRegionInTable(NAME));
      assertFalse(ASSIGN_CALLED.get());
    } finally {
      // let the region server go
      RESUME.countDown();
    }
    // wait the new region server online
    assertSame(dst, regionServerFuture.get());
    // try move again
    UTIL.getAdmin().move(UTIL.getAdmin().getRegions(NAME).get(0).getEncodedNameAsBytes(),
      dst.getServerName());
    // this time the region should be on the new region server
    assertSame(dst, UTIL.getRSForFirstRegionInTable(NAME));
    assertTrue(ASSIGN_CALLED.get());
  }
}
