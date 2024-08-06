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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.AsyncAggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, CoprocessorTests.class })
public class TestAsyncAggregationClientPartialResult {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncAggregationClientPartialResult.class);

  private static HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME =
    TableName.valueOf("TestAsyncAggregationClientPartialResult");

  private static byte[] CF = Bytes.toBytes("CF");

  private static byte[] CQ = Bytes.toBytes("CQ");

  private static AsyncConnection CONN;

  private static AsyncTable<AdvancedScanResultConsumer> TABLE;

  @Before
  public void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      MockPartialResultAggregateImplemention.class.getName());
    UTIL.startMiniCluster(3);
    byte[][] splitKeys = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    UTIL.createTable(TABLE_NAME, CF, splitKeys);
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
    TABLE = CONN.getTable(TABLE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    CONN.close();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMax() throws InterruptedException, ExecutionException {
    assertEquals(10, AsyncAggregationClient
      .max(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get().longValue());
  }

  @Test
  public void testMin() throws InterruptedException, ExecutionException {
    assertEquals(2, AsyncAggregationClient
      .min(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get().longValue());
  }

}
