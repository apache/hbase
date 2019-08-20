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
package org.apache.hadoop.hbase.rest.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test RemoteHTable retries.
 */
@Category({RestTests.class, SmallTests.class})
public class TestRemoteHTableRetries {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRemoteHTableRetries.class);

  private static final int SLEEP_TIME = 50;
  private static final int RETRIES = 3;
  private static final long MAX_TIME = SLEEP_TIME * (RETRIES - 1);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] ROW_1 = Bytes.toBytes("testrow1");
  private static final byte[] COLUMN_1 = Bytes.toBytes("a");
  private static final byte[] QUALIFIER_1 = Bytes.toBytes("1");
  private static final byte[] VALUE_1 = Bytes.toBytes("testvalue1");

  private Client client;
  private RemoteHTable remoteTable;

  @Before
  public void setup() throws Exception {
    client = mock(Client.class);
    Response response = new Response(509);
    when(client.get(anyString(), anyString())).thenReturn(response);
    when(client.delete(anyString())).thenReturn(response);
    when(client.put(anyString(), anyString(), any())).thenReturn(
        response);
    when(client.post(anyString(), anyString(), any())).thenReturn(
        response);

    Configuration configuration = TEST_UTIL.getConfiguration();
    configuration.setInt("hbase.rest.client.max.retries", RETRIES);
    configuration.setInt("hbase.rest.client.sleep", SLEEP_TIME);

    remoteTable = new RemoteHTable(client, TEST_UTIL.getConfiguration(),
        "MyTable");
  }

  @After
  public void tearDownAfterClass() throws Exception {
    remoteTable.close();
  }

  @Test
  public void testDelete() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        Delete delete = new Delete(Bytes.toBytes("delete"));
        remoteTable.delete(delete);
      }
    });
    verify(client, times(RETRIES)).delete(anyString());
  }

  @Test
  public void testGet() throws Exception {
    testTimedOutGetCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteTable.get(new Get(Bytes.toBytes("Get")));
      }
    });
  }

  @Test
  public void testSingleRowPut() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteTable.put(new Put(Bytes.toBytes("Row")));
      }
    });
    verify(client, times(RETRIES)).put(anyString(), anyString(), any());
  }

  @Test
  public void testMultiRowPut() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        Put[] puts = { new Put(Bytes.toBytes("Row1")), new Put(Bytes.toBytes("Row2")) };
        remoteTable.put(Arrays.asList(puts));
      }
    });
    verify(client, times(RETRIES)).put(anyString(), anyString(), any());
  }

  @Test
  public void testGetScanner() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteTable.getScanner(new Scan());
      }
    });
    verify(client, times(RETRIES)).post(anyString(), anyString(), any());
  }

  @Test
  public void testCheckAndPut() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        Put put = new Put(ROW_1);
        put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
        remoteTable.checkAndMutate(ROW_1, COLUMN_1).qualifier(QUALIFIER_1)
            .ifEquals(VALUE_1).thenPut(put);
      }
    });
    verify(client, times(RETRIES)).put(anyString(), anyString(), any());
  }

  @Test
  public void testCheckAndDelete() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        Put put = new Put(ROW_1);
        put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
        Delete delete= new Delete(ROW_1);
        remoteTable.checkAndMutate(ROW_1, COLUMN_1).qualifier(QUALIFIER_1)
            .ifEquals(VALUE_1).thenDelete(delete);
      }
    });
  }

  private void testTimedOutGetCall(CallExecutor callExecutor) throws Exception {
    testTimedOutCall(callExecutor);
    verify(client, times(RETRIES)).get(anyString(), anyString());
  }

  private void testTimedOutCall(CallExecutor callExecutor) throws Exception {
    long start = System.currentTimeMillis();
    try {
      callExecutor.run();
      fail("should be timeout exception!");
    } catch (IOException e) {
      assertTrue(Pattern.matches(".*request timed out", e.toString()));
    }
    assertTrue((System.currentTimeMillis() - start) > MAX_TIME);
  }

  private interface CallExecutor {
    void run() throws Exception;
  }
}
