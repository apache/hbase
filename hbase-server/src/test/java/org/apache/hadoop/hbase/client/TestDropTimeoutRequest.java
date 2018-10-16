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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a drop timeout request.
 * This test used to be in TestHCM but it has particulare requirements -- i.e. one handler only --
 * so run it apart from the rest of TestHCM.
 */
@Category({MediumTests.class})
public class TestDropTimeoutRequest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDropTimeoutRequest.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestDropTimeoutRequest.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final int RPC_RETRY = 5;

  /**
   * Coprocessor that sleeps a while the first time you do a Get
   */
  public static class SleepLongerAtFirstCoprocessor implements RegionCoprocessor, RegionObserver {
    public static final int SLEEP_TIME = 2000;
    static final AtomicLong ct = new AtomicLong(0);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Get get, final List<Cell> results) throws IOException {
      // After first sleep, all requests are timeout except the last retry. If we handle
      // all the following requests, finally the last request is also timeout. If we drop all
      // timeout requests, we can handle the last request immediately and it will not timeout.
      if (ct.incrementAndGet() <= 1) {
        Threads.sleep(SLEEP_TIME * RPC_RETRY * 2);
      } else {
        Threads.sleep(SLEEP_TIME);
      }
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.STATUS_PUBLISHED, true);
    // Up the handlers; this test needs more than usual.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, RPC_RETRY);
    // Simulate queue blocking in testDropTimeoutRequest
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 1);
    TEST_UTIL.startMiniCluster(2);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testDropTimeoutRequest() throws Exception {
    // Simulate the situation that the server is slow and client retries for several times because
    // of timeout. When a request can be handled after waiting in the queue, we will drop it if
    // it has been considered as timeout at client. If we don't drop it, the server will waste time
    // on handling timeout requests and finally all requests timeout and client throws exception.
    TableDescriptorBuilder builder =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()));
    builder.setCoprocessor(SleepLongerAtFirstCoprocessor.class.getName());
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(FAM_NAM).build();
    builder.setColumnFamily(cfd);
    TableDescriptor td = builder.build();
    try (Admin admin = TEST_UTIL.getConnection().getAdmin()) {
      admin.createTable(td);
    }
    TableBuilder tb = TEST_UTIL.getConnection().getTableBuilder(td.getTableName(), null);
    tb.setReadRpcTimeout(SleepLongerAtFirstCoprocessor.SLEEP_TIME * 2);
    tb.setWriteRpcTimeout(SleepLongerAtFirstCoprocessor.SLEEP_TIME * 2);
    try (Table table = tb.build()) {
      table.get(new Get(FAM_NAM));
    }
  }
}
