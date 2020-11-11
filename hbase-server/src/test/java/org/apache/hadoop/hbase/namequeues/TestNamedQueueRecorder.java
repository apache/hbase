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
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcCallback;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog.SlowLogPayload;

/**
 * Tests for Online SlowLog Provider Service
 */
@Category({MasterTests.class, MediumTests.class})
public class TestNamedQueueRecorder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNamedQueueRecorder.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestNamedQueueRecorder.class);

  private static final HBaseTestingUtility HBASE_TESTING_UTILITY = new HBaseTestingUtility();

  private NamedQueueRecorder namedQueueRecorder;

  private static int i = 0;

  private static Configuration applySlowLogRecorderConf(int eventSize) {
    Configuration conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.setBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY, true);
    conf.setInt("hbase.regionserver.slowlog.ringbuffer.size", eventSize);
    return conf;
  }

  /**
   * confirm that for a ringbuffer of slow logs, payload on given index of buffer
   * has expected elements
   *
   * @param i index of ringbuffer logs
   * @param j data value that was put on index i
   * @param slowLogPayloads list of payload retrieved from {@link NamedQueueRecorder}
   * @return if actual values are as per expectations
   */
  private boolean confirmPayloadParams(int i, int j, List<SlowLogPayload> slowLogPayloads) {
    boolean isClientExpected = slowLogPayloads.get(i).getClientAddress().equals("client_" + j);
    boolean isUserExpected = slowLogPayloads.get(i).getUserName().equals("userName_" + j);
    boolean isClassExpected = slowLogPayloads.get(i).getServerClass().equals("class_" + j);
    return isClassExpected && isClientExpected && isUserExpected;
  }

  @Test
  public void testOnlieSlowLogConsumption() throws Exception{

    Configuration conf = applySlowLogRecorderConf(8);
    Constructor<NamedQueueRecorder> constructor =
      NamedQueueRecorder.class.getDeclaredConstructor(Configuration.class);
    constructor.setAccessible(true);
    namedQueueRecorder = constructor.newInstance(conf);
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().setLimit(15).build();

    namedQueueRecorder.clearNamedQueue(NamedQueuePayload.NamedQueueEvent.SLOW_LOG);
    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);
    LOG.debug("Initially ringbuffer of Slow Log records is empty");

    int i = 0;

    // add 5 records initially
    for (; i < 5; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    Assert.assertNotEquals(-1,
      HBASE_TESTING_UTILITY.waitFor(3000, () -> getSlowLogPayloads(request).size() == 5));
    List<SlowLogPayload> slowLogPayloads = getSlowLogPayloads(request);
    Assert.assertTrue(confirmPayloadParams(0, 5, slowLogPayloads));
    Assert.assertTrue(confirmPayloadParams(1, 4, slowLogPayloads));
    Assert.assertTrue(confirmPayloadParams(2, 3, slowLogPayloads));
    Assert.assertTrue(confirmPayloadParams(3, 2, slowLogPayloads));
    Assert.assertTrue(confirmPayloadParams(4, 1, slowLogPayloads));

    // add 2 more records
    for (; i < 7; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(request).size() == 7));

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> slowLogPayloadsList = getSlowLogPayloads(request);
        return slowLogPayloadsList.size() == 7
          && confirmPayloadParams(0, 7, slowLogPayloadsList)
          && confirmPayloadParams(5, 2, slowLogPayloadsList)
          && confirmPayloadParams(6, 1, slowLogPayloadsList);
      })
    );

    // add 3 more records
    for (; i < 10; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(request).size() == 8));

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> slowLogPayloadsList = getSlowLogPayloads(request);
        // confirm ringbuffer is full
        return slowLogPayloadsList.size() == 8
          && confirmPayloadParams(7, 3, slowLogPayloadsList)
          && confirmPayloadParams(0, 10, slowLogPayloadsList)
          && confirmPayloadParams(1, 9, slowLogPayloadsList);
      })
    );

    // add 4 more records
    for (; i < 14; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(request).size() == 8));

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> slowLogPayloadsList = getSlowLogPayloads(request);
        // confirm ringbuffer is full
        // and ordered events
        return slowLogPayloadsList.size() == 8
          && confirmPayloadParams(0, 14, slowLogPayloadsList)
          && confirmPayloadParams(1, 13, slowLogPayloadsList)
          && confirmPayloadParams(2, 12, slowLogPayloadsList)
          && confirmPayloadParams(3, 11, slowLogPayloadsList);
      })
    );

    AdminProtos.SlowLogResponseRequest largeLogRequest =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .setLogType(AdminProtos.SlowLogResponseRequest.LogType.LARGE_LOG)
        .build();
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> slowLogPayloadsList = getSlowLogPayloads(largeLogRequest);
        // confirm ringbuffer is full
        // and ordered events
        return slowLogPayloadsList.size() == 8
          && confirmPayloadParams(0, 14, slowLogPayloadsList)
          && confirmPayloadParams(1, 13, slowLogPayloadsList)
          && confirmPayloadParams(2, 12, slowLogPayloadsList)
          && confirmPayloadParams(3, 11, slowLogPayloadsList);
      })
    );

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        boolean isRingBufferCleaned = namedQueueRecorder.clearNamedQueue(
          NamedQueuePayload.NamedQueueEvent.SLOW_LOG);

        LOG.debug("cleared the ringbuffer of Online Slow Log records");

        List<SlowLogPayload> slowLogPayloadsList = getSlowLogPayloads(request);
        // confirm ringbuffer is empty
        return slowLogPayloadsList.size() == 0 && isRingBufferCleaned;
      })
    );

  }

  private List<SlowLogPayload> getSlowLogPayloads(AdminProtos.SlowLogResponseRequest request) {
    NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
    namedQueueGetRequest.setNamedQueueEvent(RpcLogDetails.SLOW_LOG_EVENT);
    namedQueueGetRequest.setSlowLogResponseRequest(request);
    NamedQueueGetResponse namedQueueGetResponse =
      namedQueueRecorder.getNamedQueueRecords(namedQueueGetRequest);
    return namedQueueGetResponse == null ?
      Collections.emptyList() : namedQueueGetResponse.getSlowLogPayloads();
  }

  @Test
  public void testOnlineSlowLogWithHighRecords() throws Exception {

    Configuration conf = applySlowLogRecorderConf(14);
    Constructor<NamedQueueRecorder> constructor =
      NamedQueueRecorder.class.getDeclaredConstructor(Configuration.class);
    constructor.setAccessible(true);
    namedQueueRecorder = constructor.newInstance(conf);
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().setLimit(14 * 11).build();

    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);
    LOG.debug("Initially ringbuffer of Slow Log records is empty");

    for (int i = 0; i < 14 * 11; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }
    LOG.debug("Added 14 * 11 records, ringbuffer should only provide latest 14 records");

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(request).size() == 14));

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> slowLogPayloads = getSlowLogPayloads(request);

        // confirm strict order of slow log payloads
        return slowLogPayloads.size() == 14
          && confirmPayloadParams(0, 154, slowLogPayloads)
          && confirmPayloadParams(1, 153, slowLogPayloads)
          && confirmPayloadParams(2, 152, slowLogPayloads)
          && confirmPayloadParams(3, 151, slowLogPayloads)
          && confirmPayloadParams(4, 150, slowLogPayloads)
          && confirmPayloadParams(5, 149, slowLogPayloads)
          && confirmPayloadParams(6, 148, slowLogPayloads)
          && confirmPayloadParams(7, 147, slowLogPayloads)
          && confirmPayloadParams(8, 146, slowLogPayloads)
          && confirmPayloadParams(9, 145, slowLogPayloads)
          && confirmPayloadParams(10, 144, slowLogPayloads)
          && confirmPayloadParams(11, 143, slowLogPayloads)
          && confirmPayloadParams(12, 142, slowLogPayloads)
          && confirmPayloadParams(13, 141, slowLogPayloads);
      })
    );

    boolean isRingBufferCleaned = namedQueueRecorder.clearNamedQueue(
      NamedQueuePayload.NamedQueueEvent.SLOW_LOG);
    Assert.assertTrue(isRingBufferCleaned);
    LOG.debug("cleared the ringbuffer of Online Slow Log records");
    List<SlowLogPayload> slowLogPayloads = getSlowLogPayloads(request);

    // confirm ringbuffer is empty
    Assert.assertEquals(slowLogPayloads.size(), 0);
  }

  @Test
  public void testOnlineSlowLogWithDefaultDisableConfig() throws Exception {
    Configuration conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.unset(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY);

    Constructor<NamedQueueRecorder> constructor =
      NamedQueueRecorder.class.getDeclaredConstructor(Configuration.class);
    constructor.setAccessible(true);
    namedQueueRecorder = constructor.newInstance(conf);
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().build();
    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);
    LOG.debug("Initially ringbuffer of Slow Log records is empty");
    for (int i = 0; i < 300; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> slowLogPayloads = getSlowLogPayloads(request);
        return slowLogPayloads.size() == 0;
      })
    );

  }

  @Test
  public void testOnlineSlowLogWithDisableConfig() throws Exception {
    Configuration conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.setBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY, false);
    Constructor<NamedQueueRecorder> constructor =
      NamedQueueRecorder.class.getDeclaredConstructor(Configuration.class);
    constructor.setAccessible(true);
    namedQueueRecorder = constructor.newInstance(conf);

    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().build();
    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);
    LOG.debug("Initially ringbuffer of Slow Log records is empty");
    for (int i = 0; i < 300; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> slowLogPayloads = getSlowLogPayloads(request);
        return slowLogPayloads.size() == 0;
      })
    );
    conf.setBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY, true);
  }

  @Test
  public void testSlowLogFilters() throws Exception {

    Configuration conf = applySlowLogRecorderConf(30);
    Constructor<NamedQueueRecorder> constructor =
      NamedQueueRecorder.class.getDeclaredConstructor(Configuration.class);
    constructor.setAccessible(true);
    namedQueueRecorder = constructor.newInstance(conf);
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .setUserName("userName_87")
        .build();

    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);

    LOG.debug("Initially ringbuffer of Slow Log records is empty");

    for (int i = 0; i < 100; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }
    LOG.debug("Added 100 records, ringbuffer should only 1 record with matching filter");

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(request).size() == 1));

    AdminProtos.SlowLogResponseRequest requestClient =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .setClientAddress("client_85")
        .build();
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(requestClient).size() == 1));

    AdminProtos.SlowLogResponseRequest requestSlowLog =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .build();
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(requestSlowLog).size() == 15));
  }

  @Test
  public void testConcurrentSlowLogEvents() throws Exception {

    Configuration conf = applySlowLogRecorderConf(50000);
    Constructor<NamedQueueRecorder> constructor =
      NamedQueueRecorder.class.getDeclaredConstructor(Configuration.class);
    constructor.setAccessible(true);
    namedQueueRecorder = constructor.newInstance(conf);
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().setLimit(500000).build();
    AdminProtos.SlowLogResponseRequest largeLogRequest =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(500000)
        .setLogType(AdminProtos.SlowLogResponseRequest.LogType.LARGE_LOG)
        .build();
    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);
    LOG.debug("Initially ringbuffer of Slow Log records is empty");

    for (int j = 0; j < 1000; j++) {

      CompletableFuture.runAsync(() -> {
        for (int i = 0; i < 3500; i++) {
          RpcLogDetails rpcLogDetails =
            getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
          namedQueueRecorder.addRecord(rpcLogDetails);
        }
      });

    }

    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(
      5000, () -> getSlowLogPayloads(request).size() > 10000));
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(
      5000, () -> getSlowLogPayloads(largeLogRequest).size() > 10000));
  }

  @Test
  public void testSlowLargeLogEvents() throws Exception {
    Configuration conf = applySlowLogRecorderConf(28);
    Constructor<NamedQueueRecorder> constructor =
      NamedQueueRecorder.class.getDeclaredConstructor(Configuration.class);
    constructor.setAccessible(true);
    namedQueueRecorder = constructor.newInstance(conf);

    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().setLimit(14 * 11).build();

    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);
    LOG.debug("Initially ringbuffer of Slow Log records is empty");

    boolean isSlowLog;
    boolean isLargeLog;
    for (int i = 0; i < 14 * 11; i++) {
      if (i % 2 == 0) {
        isSlowLog = true;
        isLargeLog = false;
      } else {
        isSlowLog = false;
        isLargeLog = true;
      }
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1),
          isSlowLog, isLargeLog);
      namedQueueRecorder.addRecord(rpcLogDetails);
    }
    LOG.debug("Added 14 * 11 records, ringbuffer should only provide latest 14 records");

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(request).size() == 14));

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> slowLogPayloads = getSlowLogPayloads(request);

        // confirm strict order of slow log payloads
        return slowLogPayloads.size() == 14
          && confirmPayloadParams(0, 153, slowLogPayloads)
          && confirmPayloadParams(1, 151, slowLogPayloads)
          && confirmPayloadParams(2, 149, slowLogPayloads)
          && confirmPayloadParams(3, 147, slowLogPayloads)
          && confirmPayloadParams(4, 145, slowLogPayloads)
          && confirmPayloadParams(5, 143, slowLogPayloads)
          && confirmPayloadParams(6, 141, slowLogPayloads)
          && confirmPayloadParams(7, 139, slowLogPayloads)
          && confirmPayloadParams(8, 137, slowLogPayloads)
          && confirmPayloadParams(9, 135, slowLogPayloads)
          && confirmPayloadParams(10, 133, slowLogPayloads)
          && confirmPayloadParams(11, 131, slowLogPayloads)
          && confirmPayloadParams(12, 129, slowLogPayloads)
          && confirmPayloadParams(13, 127, slowLogPayloads);
      })
    );

    AdminProtos.SlowLogResponseRequest largeLogRequest =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(14 * 11)
        .setLogType(AdminProtos.SlowLogResponseRequest.LogType.LARGE_LOG)
        .build();

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(largeLogRequest).size() == 14));

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> {
        List<SlowLogPayload> largeLogPayloads = getSlowLogPayloads(largeLogRequest);

        // confirm strict order of slow log payloads
        return largeLogPayloads.size() == 14
          && confirmPayloadParams(0, 154, largeLogPayloads)
          && confirmPayloadParams(1, 152, largeLogPayloads)
          && confirmPayloadParams(2, 150, largeLogPayloads)
          && confirmPayloadParams(3, 148, largeLogPayloads)
          && confirmPayloadParams(4, 146, largeLogPayloads)
          && confirmPayloadParams(5, 144, largeLogPayloads)
          && confirmPayloadParams(6, 142, largeLogPayloads)
          && confirmPayloadParams(7, 140, largeLogPayloads)
          && confirmPayloadParams(8, 138, largeLogPayloads)
          && confirmPayloadParams(9, 136, largeLogPayloads)
          && confirmPayloadParams(10, 134, largeLogPayloads)
          && confirmPayloadParams(11, 132, largeLogPayloads)
          && confirmPayloadParams(12, 130, largeLogPayloads)
          && confirmPayloadParams(13, 128, largeLogPayloads);
      })
    );
  }

  @Test
  public void testSlowLogMixedFilters() throws Exception {

    Configuration conf = applySlowLogRecorderConf(30);
    Constructor<NamedQueueRecorder> constructor =
      NamedQueueRecorder.class.getDeclaredConstructor(Configuration.class);
    constructor.setAccessible(true);
    namedQueueRecorder = constructor.newInstance(conf);
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .setUserName("userName_87")
        .setClientAddress("client_88")
        .build();

    Assert.assertEquals(getSlowLogPayloads(request).size(), 0);

    for (int i = 0; i < 100; i++) {
      RpcLogDetails rpcLogDetails =
        getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      namedQueueRecorder.addRecord(rpcLogDetails);
    }

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(request).size() == 2));

    AdminProtos.SlowLogResponseRequest request2 = AdminProtos.SlowLogResponseRequest.newBuilder()
      .setLimit(15)
      .setUserName("userName_1")
      .setClientAddress("client_2")
      .build();
    Assert.assertEquals(0, getSlowLogPayloads(request2).size());

    AdminProtos.SlowLogResponseRequest request3 =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .setUserName("userName_87")
        .setClientAddress("client_88")
        .setFilterByOperator(AdminProtos.SlowLogResponseRequest.FilterByOperator.AND)
        .build();
    Assert.assertEquals(0, getSlowLogPayloads(request3).size());

    AdminProtos.SlowLogResponseRequest request4 =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .setUserName("userName_87")
        .setClientAddress("client_87")
        .setFilterByOperator(AdminProtos.SlowLogResponseRequest.FilterByOperator.AND)
        .build();
    Assert.assertEquals(1, getSlowLogPayloads(request4).size());

    AdminProtos.SlowLogResponseRequest request5 =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .setUserName("userName_88")
        .setClientAddress("client_89")
        .setFilterByOperator(AdminProtos.SlowLogResponseRequest.FilterByOperator.OR)
        .build();
    Assert.assertEquals(2, getSlowLogPayloads(request5).size());

    AdminProtos.SlowLogResponseRequest requestSlowLog =
      AdminProtos.SlowLogResponseRequest.newBuilder()
        .setLimit(15)
        .build();
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(3000,
      () -> getSlowLogPayloads(requestSlowLog).size() == 15));
  }

  static RpcLogDetails getRpcLogDetails(String userName, String clientAddress, String className) {
    RpcCall rpcCall = getRpcCall(userName);
    return new RpcLogDetails(rpcCall, rpcCall.getParam(), clientAddress, 0, className, true, true);
  }

  private RpcLogDetails getRpcLogDetails(String userName, String clientAddress,
      String className, boolean isSlowLog, boolean isLargeLog) {
    RpcCall rpcCall = getRpcCall(userName);
    return new RpcLogDetails(rpcCall, rpcCall.getParam(), clientAddress, 0, className, isSlowLog,
      isLargeLog);
  }

  private static RpcCall getRpcCall(String userName) {
    RpcCall rpcCall = new RpcCall() {
      @Override
      public BlockingService getService() {
        return null;
      }

      @Override
      public Descriptors.MethodDescriptor getMethod() {
        return null;
      }

      @Override
      public Message getParam() {
        return getMessage();
      }

      @Override
      public CellScanner getCellScanner() {
        return null;
      }

      @Override
      public long getReceiveTime() {
        return 0;
      }

      @Override
      public long getStartTime() {
        return 0;
      }

      @Override
      public void setStartTime(long startTime) {

      }

      @Override
      public int getTimeout() {
        return 0;
      }

      @Override
      public int getPriority() {
        return 0;
      }

      @Override
      public long getDeadline() {
        return 0;
      }

      @Override
      public long getSize() {
        return 0;
      }

      @Override
      public RPCProtos.RequestHeader getHeader() {
        return null;
      }

      @Override
      public int getRemotePort() {
        return 0;
      }

      @Override
      public void setResponse(Message param, CellScanner cells,
        Throwable errorThrowable, String error) {
      }

      @Override
      public void sendResponseIfReady() throws IOException {
      }

      @Override
      public void cleanup() {
      }

      @Override
      public String toShortString() {
        return null;
      }

      @Override
      public long disconnectSince() {
        return 0;
      }

      @Override
      public boolean isClientCellBlockSupported() {
        return false;
      }

      @Override
      public Optional<User> getRequestUser() {
        return getUser(userName);
      }

      @Override
      public InetAddress getRemoteAddress() {
        return null;
      }

      @Override
      public HBaseProtos.VersionInfo getClientVersionInfo() {
        return null;
      }

      @Override
      public void setCallBack(RpcCallback callback) {
      }

      @Override
      public boolean isRetryImmediatelySupported() {
        return false;
      }

      @Override
      public long getResponseCellSize() {
        return 0;
      }

      @Override
      public void incrementResponseCellSize(long cellSize) {
      }

      @Override
      public long getResponseBlockSize() {
        return 0;
      }

      @Override
      public void incrementResponseBlockSize(long blockSize) {
      }

      @Override
      public long getResponseExceptionSize() {
        return 0;
      }

      @Override
      public void incrementResponseExceptionSize(long exceptionSize) {
      }
    };
    return rpcCall;
  }

  private static Message getMessage() {

    i = (i + 1) % 3;

    Message message = null;

    switch (i) {

      case 0: {
        message = ClientProtos.ScanRequest.newBuilder()
          .setRegion(HBaseProtos.RegionSpecifier.newBuilder()
            .setValue(ByteString.copyFromUtf8("region1"))
            .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
            .build())
          .build();
        break;
      }
      case 1: {
        message = ClientProtos.MutateRequest.newBuilder()
          .setRegion(HBaseProtos.RegionSpecifier.newBuilder()
            .setValue(ByteString.copyFromUtf8("region2"))
            .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME))
          .setMutation(ClientProtos.MutationProto.newBuilder()
            .setRow(ByteString.copyFromUtf8("row123"))
            .build())
          .build();
        break;
      }
      case 2: {
        message = ClientProtos.GetRequest.newBuilder()
          .setRegion(HBaseProtos.RegionSpecifier.newBuilder()
            .setValue(ByteString.copyFromUtf8("region2"))
            .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME))
          .setGet(ClientProtos.Get.newBuilder()
            .setRow(ByteString.copyFromUtf8("row123"))
            .build())
          .build();
        break;
      }
      default:
        throw new RuntimeException("Not supposed to get here?");
    }

    return message;

  }

  private static Optional<User> getUser(String userName) {

    return Optional.of(new User() {
      @Override
      public String getShortName() {
        return userName;
      }

      @Override
      public <T> T runAs(PrivilegedAction<T> action) {
        return null;
      }

      @Override
      public <T> T runAs(PrivilegedExceptionAction<T> action) {
        return null;
      }
    });

  }

}
