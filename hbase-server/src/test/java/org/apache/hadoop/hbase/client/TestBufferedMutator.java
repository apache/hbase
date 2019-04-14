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

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
public class TestBufferedMutator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBufferedMutator.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("test");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static int COUNT = 1024;

  private static byte[] VALUE = new byte[1024];

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, CF);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    try (BufferedMutator mutator = TEST_UTIL.getConnection().getBufferedMutator(TABLE_NAME)) {
      mutator.mutate(IntStream.range(0, COUNT / 2)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE))
        .collect(Collectors.toList()));
      mutator.flush();
      mutator.mutate(IntStream.range(COUNT / 2, COUNT)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE))
        .collect(Collectors.toList()));
      mutator.close();
      verifyData();
    }
  }

  private void verifyData() throws IOException {
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < COUNT; i++) {
        Result r = table.get(new Get(Bytes.toBytes(i)));
        assertArrayEquals(VALUE, ((Result) r).getValue(CF, CQ));
      }
    }
  }
}
