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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.AsyncAggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, CoprocessorTests.class })
public class TestAsyncAggregationClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncAggregationClient.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("TestAsyncAggregationClient");

  private static byte[] CF = Bytes.toBytes("CF");

  private static byte[] CQ = Bytes.toBytes("CQ");

  private static byte[] CQ2 = Bytes.toBytes("CQ2");

  private static int COUNT = 1000;

  private static AsyncConnection CONN;

  private static AsyncTable<AdvancedScanResultConsumer> TABLE;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      AggregateImplementation.class.getName());
    UTIL.startMiniCluster(3);
    byte[][] splitKeys = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    UTIL.createTable(TABLE_NAME, CF, splitKeys);
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
    TABLE = CONN.getTable(TABLE_NAME);
    TABLE.putAll(LongStream.range(0, COUNT)
        .mapToObj(l -> new Put(Bytes.toBytes(String.format("%03d", l)))
            .addColumn(CF, CQ, Bytes.toBytes(l)).addColumn(CF, CQ2, Bytes.toBytes(l * l)))
        .collect(Collectors.toList())).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMax() throws InterruptedException, ExecutionException {
    assertEquals(COUNT - 1, AsyncAggregationClient
        .max(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get().longValue());
  }

  @Test
  public void testMin() throws InterruptedException, ExecutionException {
    assertEquals(0, AsyncAggregationClient
        .min(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get().longValue());
  }

  @Test
  public void testRowCount() throws InterruptedException, ExecutionException {
    assertEquals(COUNT,
      AsyncAggregationClient
          .rowCount(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get()
          .longValue());
  }

  @Test
  public void testSum() throws InterruptedException, ExecutionException {
    assertEquals(COUNT * (COUNT - 1) / 2, AsyncAggregationClient
        .sum(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get().longValue());
  }

  private static final double DELTA = 1E-3;

  @Test
  public void testAvg() throws InterruptedException, ExecutionException {
    assertEquals((COUNT - 1) / 2.0, AsyncAggregationClient
        .avg(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get().doubleValue(),
      DELTA);
  }

  @Test
  public void testStd() throws InterruptedException, ExecutionException {
    double avgSq =
        LongStream.range(0, COUNT).map(l -> l * l).reduce((l1, l2) -> l1 + l2).getAsLong()
            / (double) COUNT;
    double avg = (COUNT - 1) / 2.0;
    double std = Math.sqrt(avgSq - avg * avg);
    assertEquals(std, AsyncAggregationClient
        .std(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get().doubleValue(),
      DELTA);
  }

  @Test
  public void testMedian() throws InterruptedException, ExecutionException {
    long halfSum = COUNT * (COUNT - 1) / 4;
    long median = 0L;
    long sum = 0L;
    for (int i = 0; i < COUNT; i++) {
      sum += i;
      if (sum > halfSum) {
        median = i - 1;
        break;
      }
    }
    assertEquals(median,
      AsyncAggregationClient
          .median(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ)).get()
          .longValue());
  }

  @Test
  public void testMedianWithWeight() throws InterruptedException, ExecutionException {
    long halfSum =
        LongStream.range(0, COUNT).map(l -> l * l).reduce((l1, l2) -> l1 + l2).getAsLong() / 2;
    long median = 0L;
    long sum = 0L;
    for (int i = 0; i < COUNT; i++) {
      sum += i * i;
      if (sum > halfSum) {
        median = i - 1;
        break;
      }
    }
    assertEquals(median, AsyncAggregationClient
        .median(TABLE, new LongColumnInterpreter(), new Scan().addColumn(CF, CQ).addColumn(CF, CQ2))
        .get().longValue());
  }
}
