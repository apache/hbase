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
package org.apache.hadoop.hbase.namequeues;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.regionHistorian.RegionHistorianTableAccessor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionHist;
import org.apache.hadoop.hbase.slowlog.SlowLogTableAccessor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Uninterruptibles;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog;

@Category({ MasterTests.class, MediumTests.class })
public class TestRegionHistorianAccessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionHistorianAccessor.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestNamedQueueRecorder.class);
  private static final HBaseTestingUtility HBASE_TESTING_UTILITY = new HBaseTestingUtility();
  private NamedQueueRecorder namedQueueRecorder;

  @BeforeClass
  public static void setup() throws Exception {
    try {
      HBASE_TESTING_UTILITY.shutdownMiniHBaseCluster();
    } catch (IOException e) {
      LOG.debug("No worries.");
    }
    Configuration conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.setBoolean(HConstants.REGION_HISTORIAN_BUFFER_ENABLED_KEY, true);
    conf.setBoolean(HConstants.REGION_HISTORIAN_SYS_TABLE_ENABLED_KEY, true);
    conf.setInt("hbase.master.regionHistorian.systable.chore.duration", 900);
    conf.setInt("hbase.master.regionHistorian.ringbuffer.size", 50000);
    conf.setInt("hbase.master.regionHistorian.systable.queue.size", 5000);
    HBASE_TESTING_UTILITY.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    HBASE_TESTING_UTILITY.shutdownMiniHBaseCluster();
  }

  @Before
  public void setUp() throws Exception {
    HMaster hMaster = HBASE_TESTING_UTILITY.getMiniHBaseCluster().getMaster();
    Field regionHistorianRecorder = HMaster.class.getDeclaredField("namedQueueRecorder");
    regionHistorianRecorder.setAccessible(true);
    this.namedQueueRecorder = (NamedQueueRecorder) regionHistorianRecorder.get(hMaster);
  }

  private List<RegionHist.RegionHistorianPayload> getRegionHistorianPayloads(
    AdminProtos.RegionHistorianResponseRequest request) {
    NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
    namedQueueGetRequest.setNamedQueueEvent(RegionHistorianPayload.REGION_HISTORIAN_EVENT);
    namedQueueGetRequest.setRegionHistorianResponseRequest(request);
    NamedQueueGetResponse namedQueueGetResponse =
      namedQueueRecorder.getNamedQueueRecords(namedQueueGetRequest);
    return namedQueueGetResponse == null ?
      Collections.emptyList() :
      namedQueueGetResponse.getRegionHistorianPayloads();
  }

  private int getTableCount(Connection connection) {
    try (Table table = connection.getTable(RegionHistorianTableAccessor.REGION_HISTORIAN_TABLE_NAME)) {
      ResultScanner resultScanner = table.getScanner(new Scan().setReadType(Scan.ReadType.STREAM));
      int count = 0;
      for (Result result : resultScanner) {
        ++count;
      }
      return count;
    } catch (Exception e) {
      return 0;
    }
  }

  private Connection waitForRegionHistorianTableCreation() throws IOException {
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(2000, () -> {
      try {
        return HBASE_TESTING_UTILITY.getAdmin()
          .tableExists(RegionHistorianTableAccessor.REGION_HISTORIAN_TABLE_NAME);
      } catch (IOException e) {
        return false;
      }
    }));
    return HBASE_TESTING_UTILITY.getConnection();
  }

  @Test
  public void testRegionHistorianRecords() throws Exception {

    AdminProtos.RegionHistorianResponseRequest request =
      AdminProtos.RegionHistorianResponseRequest.newBuilder().setLimit(15).build();

    namedQueueRecorder.clearNamedQueue(NamedQueuePayload.NamedQueueEvent.REGION_HISTORIAN);
    Assert.assertEquals(getRegionHistorianPayloads(request).size(),0);

    int i = 0;
    Connection connection = waitForRegionHistorianTableCreation();
    // add 5 records initially
    for (; i < 5; i++) {
      RegionHistorianPayload regionHistorianPayload = TestNamedQueueRecorder.getRegionHistorianPayload("host_" + (i + 1), "region_" + (i + 1), "table_" + (i + 1),
        "event_" + (i + 1), System.currentTimeMillis(), i + 1, i + 1);
      namedQueueRecorder.addRecord(regionHistorianPayload);
    }

    // add 2 records initially
    for (; i < 7; i++) {
      RegionHistorianPayload regionHistorianPayload = TestNamedQueueRecorder.getRegionHistorianPayload("host_" + (i + 1), "region_" + (i + 1), "table_" + (i + 1),
        "event_" + (i + 1), System.currentTimeMillis(), i + 1, i + 1);
      namedQueueRecorder.addRecord(regionHistorianPayload);
    }

    // add 3 records initially
    for (; i < 10; i++) {
      RegionHistorianPayload regionHistorianPayload = TestNamedQueueRecorder.getRegionHistorianPayload("host_" + (i + 1), "region_" + (i + 1), "table_" + (i + 1),
        "event_" + (i + 1), System.currentTimeMillis(), i + 1, i + 1);
      namedQueueRecorder.addRecord(regionHistorianPayload);
    }

    // add 4 records initially
    for (; i < 14; i++) {
      RegionHistorianPayload regionHistorianPayload = TestNamedQueueRecorder.getRegionHistorianPayload("host_" + (i + 1), "region_" + (i + 1), "table_" + (i + 1),
        "event_" + (i + 1), System.currentTimeMillis(), i + 1, i + 1);
      namedQueueRecorder.addRecord(regionHistorianPayload);
    }

    Assert.assertNotEquals(-1,
      HBASE_TESTING_UTILITY.waitFor(3000, () -> getRegionHistorianPayloads(request).size() == 14));
    HBASE_TESTING_UTILITY.wait(1000);
    Assert.assertNotEquals(-1,
      HBASE_TESTING_UTILITY.waitFor(3000, () -> getTableCount(connection) == 18));
  }
}
