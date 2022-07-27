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
package org.apache.hadoop.hbase.client.coprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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
public class TestAggregationClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAggregationClient.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("TestAggregationClient");

  private static final byte[] CF = Bytes.toBytes("CF");

  private static Connection CONN;

  private static Table TABLE;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      AggregateImplementation.class.getName());
    UTIL.startMiniCluster(1);
    UTIL.createTable(TABLE_NAME, CF);
    CONN = ConnectionFactory.createConnection(conf);
    TABLE = CONN.getTable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void itCreatesConnectionless() throws Throwable {
    AggregationClient client = new AggregationClient();
    assertFalse(client.isClosed());

    try {
      client.rowCount(TABLE_NAME, new LongColumnInterpreter(), new Scan());
      fail("Expected IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Connection not initialized"));
    }

    client.rowCount(TABLE, new LongColumnInterpreter(), new Scan());

    client.close();
    assertFalse(CONN.isClosed());
    assertFalse(client.isClosed());

  }

  @Test
  public void itCreatesExternalConnection() throws Throwable {
    AggregationClient client = new AggregationClient(CONN);
    assertFalse(client.isClosed());

    client.rowCount(TABLE_NAME, new LongColumnInterpreter(), new Scan());
    client.rowCount(TABLE, new LongColumnInterpreter(), new Scan());

    client.close();
    assertFalse(CONN.isClosed());
    assertFalse(client.isClosed());
  }

  @Test
  public void itCreatesManagedConnection() throws Throwable {
    AggregationClient client = new AggregationClient(CONN.getConfiguration());
    assertFalse(client.isClosed());

    client.rowCount(TABLE_NAME, new LongColumnInterpreter(), new Scan());
    client.rowCount(TABLE, new LongColumnInterpreter(), new Scan());

    client.close();
    assertFalse(CONN.isClosed());
    assertTrue(client.isClosed());
  }
}
