/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.*;

/**
 * Test case demonstrating client interactions with the {@link RowCountEndpoint}
 * sample coprocessor Service implementation.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestRowCountEndpoint {
  private static final TableName TEST_TABLE = TableName.valueOf("testrowcounter");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
  private static final byte[] TEST_COLUMN = Bytes.toBytes("col");

  private static HBaseTestingUtility TEST_UTIL = null;
  private static Configuration CONF = null;

  // @Ignore @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    CONF = TEST_UTIL.getConfiguration();
    CONF.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        RowCountEndpoint.class.getName());

    TEST_UTIL.startMiniCluster();
    TEST_UTIL.createTable(TEST_TABLE, new byte[][]{TEST_FAMILY});
  }

  // @Ignore @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  // @Ignore @Test
  public void testEndpoint() throws Throwable {
    Table table = TEST_UTIL.getConnection().getTable(TEST_TABLE);

    // insert some test rows
    for (int i=0; i<5; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.add(TEST_FAMILY, TEST_COLUMN, iBytes);
      table.put(p);
    }

    final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.getDefaultInstance();
    Map<byte[],Long> results = table.coprocessorService(ExampleProtos.RowCountService.class,
        null, null,
        new Batch.Call<ExampleProtos.RowCountService,Long>() {
          public Long call(ExampleProtos.RowCountService counter) throws IOException {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback =
                new BlockingRpcCallback<ExampleProtos.CountResponse>();
            counter.getRowCount(controller, request, rpcCallback);
            ExampleProtos.CountResponse response = rpcCallback.get();
            if (controller.failedOnException()) {
              throw controller.getFailedOn();
            }
            return (response != null && response.hasCount()) ? response.getCount() : 0;
          }
        });
    // should be one region with results
    assertEquals(1, results.size());
    Iterator<Long> iter = results.values().iterator();
    Long val = iter.next();
    assertNotNull(val);
    assertEquals(5l, val.longValue());
  }

}
