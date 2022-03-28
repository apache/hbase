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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableBatchRetryImmediately {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableBatchRetryImmediately.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUAL = Bytes.toBytes("cq");

  private static byte[] VALUE_PREFIX = new byte[768];

  private static int COUNT = 1000;

  private static AsyncConnection CONN;

  private static String LOG_LEVEL;

  @BeforeClass
  public static void setUp() throws Exception {
    // disable the debug log to avoid flooding the output
    LOG_LEVEL = Log4jUtils.getEffectiveLevel(AsyncRegionLocatorHelper.class.getName());
    Log4jUtils.setLogLevel(AsyncRegionLocatorHelper.class.getName(), "INFO");
    UTIL.getConfiguration().setLong(HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY, 1024);
    UTIL.startMiniCluster(1);
    Table table = UTIL.createTable(TABLE_NAME, FAMILY);
    UTIL.waitTableAvailable(TABLE_NAME);
    Bytes.random(VALUE_PREFIX);
    for (int i = 0; i < COUNT; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUAL,
        Bytes.add(VALUE_PREFIX, Bytes.toBytes(i))));
    }
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (LOG_LEVEL != null) {
      Log4jUtils.setLogLevel(AsyncRegionLocatorHelper.class.getName(), LOG_LEVEL);
    }
    CONN.close();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() {
    AsyncTable<?> table = CONN.getTable(TABLE_NAME);
    // if we do not deal with RetryImmediatelyException, we will timeout here since we need to retry
    // hundreds times.
    List<Get> gets = IntStream.range(0, COUNT).mapToObj(i -> new Get(Bytes.toBytes(i)))
      .collect(Collectors.toList());
    List<Result> results = table.getAll(gets).join();
    for (int i = 0; i < COUNT; i++) {
      byte[] value = results.get(i).getValue(FAMILY, QUAL);
      assertEquals(VALUE_PREFIX.length + 4, value.length);
      assertArrayEquals(VALUE_PREFIX, Arrays.copyOf(value, VALUE_PREFIX.length));
      assertEquals(i, Bytes.toInt(value, VALUE_PREFIX.length));
    }
  }
}
