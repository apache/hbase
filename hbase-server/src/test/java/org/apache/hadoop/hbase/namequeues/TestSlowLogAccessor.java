/*
 *
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
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
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

/**
 * Tests for SlowLog System Table
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestSlowLogAccessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSlowLogAccessor.class);

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
    conf.setBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY, true);
    conf.setBoolean(HConstants.SLOW_LOG_SYS_TABLE_ENABLED_KEY, true);
    conf.setInt("hbase.slowlog.systable.chore.duration", 900);
    conf.setInt("hbase.regionserver.slowlog.ringbuffer.size", 50000);
    HBASE_TESTING_UTILITY.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    HBASE_TESTING_UTILITY.shutdownMiniHBaseCluster();
  }

  @Before
  public void setUp() throws Exception {
    HRegionServer hRegionServer = HBASE_TESTING_UTILITY.getMiniHBaseCluster().getRegionServer(0);
    Field slowLogRecorder = HRegionServer.class.getDeclaredField("namedQueueRecorder");
    slowLogRecorder.setAccessible(true);
    this.namedQueueRecorder = (NamedQueueRecorder) slowLogRecorder.get(hRegionServer);
  }

  private List<TooSlowLog.SlowLogPayload> getSlowLogPayloads(
      AdminProtos.SlowLogResponseRequest request) {
    NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
    namedQueueGetRequest.setNamedQueueEvent(RpcLogDetails.SLOW_LOG_EVENT);
    namedQueueGetRequest.setSlowLogResponseRequest(request);
    NamedQueueGetResponse namedQueueGetResponse =
      namedQueueRecorder.getNamedQueueRecords(namedQueueGetRequest);
    return namedQueueGetResponse.getSlowLogPayloads();
  }

  @Test
  public void testSlowLogRecords() throws Exception {

    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().setLimit(15).build();

    namedQueueRecorder.clearNamedQueue(NamedQueuePayload.NamedQueueEvent.SLOW_LOG);
    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);

    int i = 0;

    Connection connection = waitForSlowLogTableCreation();
    // add 5 records initially
    for (; i < 5; i++) {
      RpcLogDetails rpcLogDetails = TestNamedQueueRecorder
        .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    // add 2 more records
    for (; i < 7; i++) {
      RpcLogDetails rpcLogDetails = TestNamedQueueRecorder
        .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    // add 3 more records
    for (; i < 10; i++) {
      RpcLogDetails rpcLogDetails = TestNamedQueueRecorder
        .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    // add 4 more records
    for (; i < 14; i++) {
      RpcLogDetails rpcLogDetails = TestNamedQueueRecorder
        .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY
      .waitFor(3000, () -> getSlowLogPayloads(request).size() == 14));

    Assert.assertNotEquals(-1,
      HBASE_TESTING_UTILITY.waitFor(3000, () -> getTableCount(connection) == 14));
  }

  private int getTableCount(Connection connection) {
    try (Table table = connection.getTable(SlowLogTableAccessor.SLOW_LOG_TABLE_NAME)) {
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

  private Connection waitForSlowLogTableCreation() throws IOException {
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(2000, () -> {
      try {
        return HBASE_TESTING_UTILITY.getAdmin()
          .tableExists(SlowLogTableAccessor.SLOW_LOG_TABLE_NAME);
      } catch (IOException e) {
        return false;
      }
    }));
    return HBASE_TESTING_UTILITY.getConnection();
  }

  @Test
  public void testHigherSlowLogs() throws Exception {
    Connection connection = waitForSlowLogTableCreation();

    namedQueueRecorder.clearNamedQueue(NamedQueuePayload.NamedQueueEvent.SLOW_LOG);
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().setLimit(500000).build();
    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);

    for (int j = 0; j < 100; j++) {
      CompletableFuture.runAsync(() -> {
        for (int i = 0; i < 350; i++) {
          if (i == 300) {
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
          }
          RpcLogDetails rpcLogDetails = TestNamedQueueRecorder
            .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
          namedQueueRecorder.addRecord(rpcLogDetails);
        }
      });
    }

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(7000, () -> {
      int count = getSlowLogPayloads(request).size();
      LOG.debug("RingBuffer records count: {}", count);
      return count > 2000;
    }));

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(7000, () -> {
      int count = getTableCount(connection);
      LOG.debug("SlowLog Table records count: {}", count);
      return count > 2000;
    }));
  }

}
