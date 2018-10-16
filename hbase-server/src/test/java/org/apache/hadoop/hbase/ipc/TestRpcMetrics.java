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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RPCTests.class, SmallTests.class})
public class TestRpcMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRpcMetrics.class);

  public MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testFactory() {
    MetricsHBaseServer masterMetrics = new MetricsHBaseServer("HMaster", new MetricsHBaseServerWrapperStub());
    MetricsHBaseServerSource masterSource = masterMetrics.getMetricsSource();

    MetricsHBaseServer rsMetrics = new MetricsHBaseServer("HRegionServer", new MetricsHBaseServerWrapperStub());
    MetricsHBaseServerSource rsSource = rsMetrics.getMetricsSource();

    assertEquals("master", masterSource.getMetricsContext());
    assertEquals("regionserver", rsSource.getMetricsContext());

    assertEquals("Master,sub=IPC", masterSource.getMetricsJmxContext());
    assertEquals("RegionServer,sub=IPC", rsSource.getMetricsJmxContext());

    assertEquals("Master", masterSource.getMetricsName());
    assertEquals("RegionServer", rsSource.getMetricsName());
  }

  /**
   * This test makes sure that the numbers from a MetricsHBaseServerWrapper are correctly exported
   * to hadoop metrics 2 system.
   */
  @Test
  public void testWrapperSource() {
    MetricsHBaseServer mrpc = new MetricsHBaseServer("HMaster", new MetricsHBaseServerWrapperStub());
    MetricsHBaseServerSource serverSource = mrpc.getMetricsSource();
    HELPER.assertGauge("queueSize", 101, serverSource);
    HELPER.assertGauge("numCallsInGeneralQueue", 102, serverSource);
    HELPER.assertGauge("numCallsInReplicationQueue", 103, serverSource);
    HELPER.assertGauge("numCallsInPriorityQueue", 104, serverSource);
    HELPER.assertGauge("numOpenConnections", 105, serverSource);
    HELPER.assertGauge("numActiveHandler", 106, serverSource);
    HELPER.assertGauge("numActiveGeneralHandler", 201, serverSource);
    HELPER.assertGauge("numActivePriorityHandler", 202, serverSource);
    HELPER.assertGauge("numActiveReplicationHandler", 203, serverSource);
    HELPER.assertGauge("numActiveWriteHandler", 50, serverSource);
    HELPER.assertGauge("numActiveReadHandler", 50, serverSource);
    HELPER.assertGauge("numActiveScanHandler", 6, serverSource);
    HELPER.assertGauge("numCallsInWriteQueue", 50, serverSource);
    HELPER.assertGauge("numCallsInReadQueue", 50, serverSource);
    HELPER.assertGauge("numCallsInScanQueue", 2, serverSource);
  }

  /**
   * Test to make sure that all the actively called method on MetricsHBaseServer work.
   */
  @Test
  public void testSourceMethods() {
    MetricsHBaseServer mrpc = new MetricsHBaseServer("HMaster", new MetricsHBaseServerWrapperStub());
    MetricsHBaseServerSource serverSource = mrpc.getMetricsSource();

    for (int i=0; i < 12; i++) {
      mrpc.authenticationFailure();
    }
    for (int i=0; i < 13; i++) {
      mrpc.authenticationSuccess();
    }
    HELPER.assertCounter("authenticationFailures", 12, serverSource);
    HELPER.assertCounter("authenticationSuccesses", 13, serverSource);



    for (int i=0; i < 14; i++) {
      mrpc.authorizationSuccess();
    }
    for (int i=0; i < 15; i++) {
      mrpc.authorizationFailure();
    }
    HELPER.assertCounter("authorizationSuccesses", 14, serverSource);
    HELPER.assertCounter("authorizationFailures", 15, serverSource);


    mrpc.dequeuedCall(100);
    mrpc.processedCall(101);
    mrpc.totalCall(102);
    HELPER.assertCounter("queueCallTime_NumOps", 1, serverSource);
    HELPER.assertCounter("processCallTime_NumOps", 1, serverSource);
    HELPER.assertCounter("totalCallTime_NumOps", 1, serverSource);

    mrpc.sentBytes(103);
    mrpc.sentBytes(103);
    mrpc.sentBytes(103);

    mrpc.receivedBytes(104);
    mrpc.receivedBytes(104);

    HELPER.assertCounter("sentBytes", 309, serverSource);
    HELPER.assertCounter("receivedBytes", 208, serverSource);

    mrpc.receivedRequest(105);
    mrpc.sentResponse(106);
    HELPER.assertCounter("requestSize_NumOps", 1, serverSource);
    HELPER.assertCounter("responseSize_NumOps", 1, serverSource);

    mrpc.exception(null);
    HELPER.assertCounter("exceptions", 1, serverSource);

    mrpc.exception(new RegionMovedException(ServerName.parseServerName("localhost:60020"), 100));
    mrpc.exception(new RegionTooBusyException("Some region"));
    mrpc.exception(new OutOfOrderScannerNextException());
    mrpc.exception(new NotServingRegionException());
    HELPER.assertCounter("exceptions.RegionMovedException", 1, serverSource);
    HELPER.assertCounter("exceptions.RegionTooBusyException", 1, serverSource);
    HELPER.assertCounter("exceptions.OutOfOrderScannerNextException", 1, serverSource);
    HELPER.assertCounter("exceptions.NotServingRegionException", 1, serverSource);
    HELPER.assertCounter("exceptions", 5, serverSource);
  }

  @Test
  public void testServerContextNameWithHostName() {
    String[] masterServerNames = { "master/node-xyz/10.19.250.253:16020",
        "master/node-regionserver-xyz/10.19.250.253:16020", "HMaster/node-xyz/10.19.250.253:16020",
        "HMaster/node-regionserver-xyz/10.19.250.253:16020" };

    String[] regionServerNames = { "regionserver/node-xyz/10.19.250.253:16020",
        "regionserver/node-master1-xyz/10.19.250.253:16020",
        "HRegionserver/node-xyz/10.19.250.253:16020",
        "HRegionserver/node-master1-xyz/10.19.250.253:16020" };

    MetricsHBaseServerSource masterSource = null;
    for (String serverName : masterServerNames) {
      masterSource = new MetricsHBaseServer(serverName, new MetricsHBaseServerWrapperStub())
          .getMetricsSource();
      assertEquals("master", masterSource.getMetricsContext());
      assertEquals("Master,sub=IPC", masterSource.getMetricsJmxContext());
      assertEquals("Master", masterSource.getMetricsName());
    }

    MetricsHBaseServerSource rsSource = null;
    for (String serverName : regionServerNames) {
      rsSource = new MetricsHBaseServer(serverName, new MetricsHBaseServerWrapperStub())
          .getMetricsSource();
      assertEquals("regionserver", rsSource.getMetricsContext());
      assertEquals("RegionServer,sub=IPC", rsSource.getMetricsJmxContext());
      assertEquals("RegionServer", rsSource.getMetricsName());
    }
  }
}

