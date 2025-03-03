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

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestBufferedMutator2 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBufferedMutator2.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("example-table");

  private static byte[] CF = Bytes.toBytes("cf");
  private static byte[] CQ = Bytes.toBytes("cq");
  private static byte[] VALUE = new byte[1024];

  private static Connection CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, CF);
    CONN = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    Bytes.random(VALUE);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMaxMutationsFlush() throws IOException {
    BufferedMutator mutator =
      CONN.getBufferedMutator(new BufferedMutatorParams(TABLE_NAME).setMaxMutations(3));
    mutator.mutate(new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE));
    mutator.mutate(new Put(Bytes.toBytes(1)).addColumn(CF, CQ, VALUE));
    mutator.mutate(new Put(Bytes.toBytes(2)).addColumn(CF, CQ, VALUE));
    Table table = CONN.getTable(TABLE_NAME);
    assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(0))).getValue(CF, CQ));
    assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(1))).getValue(CF, CQ));
    assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(2))).getValue(CF, CQ));
  }

}
