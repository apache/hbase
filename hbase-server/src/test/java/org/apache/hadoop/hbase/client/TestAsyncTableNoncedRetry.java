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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableNoncedRetry {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection ASYNC_CONN;

  private static long NONCE = 1L;

  private static NonceGenerator NONCE_GENERATOR = new NonceGenerator() {

    @Override
    public long newNonce() {
      return NONCE;
    }

    @Override
    public long getNonceGroup() {
      return 1L;
    }
  };

  @Rule
  public TestName testName = new TestName();

  private byte[] row;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    ASYNC_CONN = new AsyncConnectionImpl(TEST_UTIL.getConfiguration(), User.getCurrent()) {

      @Override
      public NonceGenerator getNonceGenerator() {
        return NONCE_GENERATOR;
      }

    };
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ASYNC_CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    row = Bytes.toBytes(testName.getMethodName().replaceAll("[^0-9A-Za-z]", "_"));
    NONCE++;
  }

  @Test
  public void testAppend() throws InterruptedException, ExecutionException {
    RawAsyncTable table = ASYNC_CONN.getRawTable(TABLE_NAME);
    Result result = table.append(new Append(row).add(FAMILY, QUALIFIER, VALUE)).get();
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    result = table.append(new Append(row).add(FAMILY, QUALIFIER, VALUE)).get();
    // the second call should have no effect as we always generate the same nonce.
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    result = table.get(new Get(row)).get();
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
  }

  @Test
  public void testIncrement() throws InterruptedException, ExecutionException {
    RawAsyncTable table = ASYNC_CONN.getRawTable(TABLE_NAME);
    assertEquals(1L, table.incrementColumnValue(row, FAMILY, QUALIFIER, 1L).get().longValue());
    // the second call should have no effect as we always generate the same nonce.
    assertEquals(1L, table.incrementColumnValue(row, FAMILY, QUALIFIER, 1L).get().longValue());
    Result result = table.get(new Get(row)).get();
    assertEquals(1L, Bytes.toLong(result.getValue(FAMILY, QUALIFIER)));
  }
}
