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

package org.apache.hadoop.hbase.regionserver.slowlog;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for SlowLog System Table
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestSlowLogAccessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSlowLogAccessor.class);

  private static final HBaseTestingUtility HBASE_TESTING_UTILITY = new HBaseTestingUtility();

  private SlowLogRecorder slowLogRecorder;

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.setBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY, true);
    conf.setBoolean(HConstants.SLOW_LOG_SYS_TABLE_ENABLED_KEY, true);
    HBASE_TESTING_UTILITY.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    HBASE_TESTING_UTILITY.shutdownMiniHBaseCluster();
  }

  @Test
  public void testSlowLogRecords() throws Exception {

    slowLogRecorder = new SlowLogRecorder(HBASE_TESTING_UTILITY.getConfiguration());
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().setLimit(15).build();

    slowLogRecorder.clearSlowLogPayloads();
    Assert.assertEquals(slowLogRecorder.getSlowLogPayloads(request).size(), 0);

    int i = 0;

    Connection connection = waitForSlowLogTableCreation();
    // add 5 records initially
    for (; i < 5; i++) {
      RpcLogDetails rpcLogDetails = TestSlowLogRecorder
        .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      slowLogRecorder.addSlowLogPayload(rpcLogDetails);
    }

    // add 2 more records
    for (; i < 7; i++) {
      RpcLogDetails rpcLogDetails = TestSlowLogRecorder
        .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      slowLogRecorder.addSlowLogPayload(rpcLogDetails);
    }

    // add 3 more records
    for (; i < 10; i++) {
      RpcLogDetails rpcLogDetails = TestSlowLogRecorder
        .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      slowLogRecorder.addSlowLogPayload(rpcLogDetails);
    }

    // add 4 more records
    for (; i < 14; i++) {
      RpcLogDetails rpcLogDetails = TestSlowLogRecorder
        .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
      slowLogRecorder.addSlowLogPayload(rpcLogDetails);
    }

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY
      .waitFor(3000, () -> slowLogRecorder.getSlowLogPayloads(request).size() == 14));

    Assert.assertNotEquals(-1,
      HBASE_TESTING_UTILITY.waitFor(3000, () -> getTableCount(connection) == 14));
  }

  private int getTableCount(Connection connection) {
    try (Table table = connection.getTable(TableName.SLOW_LOG_TABLE_NAME)) {
      ResultScanner resultScanner = table.getScanner(new Scan());
      int count = 0;
      for (Result result : resultScanner) {
        ++count;
      }
      return count;
    } catch (Exception e) {
      return 0;
    }
  }

  private Connection waitForSlowLogTableCreation() {
    Connection connection =
      HBASE_TESTING_UTILITY.getMiniHBaseCluster().getRegionServer(0).getConnection();
    slowLogRecorder.setupConnection(connection);
    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY.waitFor(2000, () -> {
      try {
        return MetaTableAccessor.tableExists(connection, TableName.SLOW_LOG_TABLE_NAME);
      } catch (IOException e) {
        return false;
      }
    }));
    return connection;
  }

  @Test
  public void testHigherSlowLogs() throws Exception {
    Configuration conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.setInt("hbase.regionserver.slowlog.ringbuffer.size", 50000);
    slowLogRecorder = new SlowLogRecorder(conf);
    Connection connection = waitForSlowLogTableCreation();

    slowLogRecorder.clearSlowLogPayloads();
    AdminProtos.SlowLogResponseRequest request =
      AdminProtos.SlowLogResponseRequest.newBuilder().setLimit(500000).build();
    Assert.assertEquals(slowLogRecorder.getSlowLogPayloads(request).size(), 0);

    for (int j = 0; j < 100; j++) {
      CompletableFuture.runAsync(() -> {
        for (int i = 0; i < 350; i++) {
          RpcLogDetails rpcLogDetails = TestSlowLogRecorder
            .getRpcLogDetails("userName_" + (i + 1), "client_" + (i + 1), "class_" + (i + 1));
          slowLogRecorder.addSlowLogPayload(rpcLogDetails);
        }
      });
    }

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY
      .waitFor(7000, () -> slowLogRecorder.getSlowLogPayloads(request).size() > 4000));

    Assert.assertNotEquals(-1, HBASE_TESTING_UTILITY
      .waitFor(3000, () -> getTableCount(connection) > 4000));
  }

}
