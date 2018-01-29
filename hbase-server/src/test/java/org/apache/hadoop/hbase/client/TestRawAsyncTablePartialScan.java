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

import java.util.List;
import java.util.concurrent.ExecutionException;
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
public class TestRawAsyncTablePartialScan {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRawAsyncTablePartialScan.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[][] CQS =
    new byte[][] { Bytes.toBytes("cq1"), Bytes.toBytes("cq2"), Bytes.toBytes("cq3") };

  private static int COUNT = 100;

  private static AsyncConnection CONN;

  private static AsyncTable<?> TABLE;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    TABLE = CONN.getTable(TABLE_NAME);
    TABLE
        .putAll(IntStream.range(0, COUNT)
            .mapToObj(i -> new Put(Bytes.toBytes(String.format("%02d", i)))
                .addColumn(FAMILY, CQS[0], Bytes.toBytes(i))
                .addColumn(FAMILY, CQS[1], Bytes.toBytes(2 * i))
                .addColumn(FAMILY, CQS[2], Bytes.toBytes(3 * i)))
            .collect(Collectors.toList()))
        .get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBatchDoNotAllowPartial() throws InterruptedException, ExecutionException {
    // we set batch to 2 and max result size to 1, then server will only returns one result per call
    // but we should get 2 + 1 for every row.
    List<Result> results = TABLE.scanAll(new Scan().setBatch(2).setMaxResultSize(1)).get();
    assertEquals(2 * COUNT, results.size());
    for (int i = 0; i < COUNT; i++) {
      Result firstTwo = results.get(2 * i);
      assertEquals(String.format("%02d", i), Bytes.toString(firstTwo.getRow()));
      assertEquals(2, firstTwo.size());
      assertEquals(i, Bytes.toInt(firstTwo.getValue(FAMILY, CQS[0])));
      assertEquals(2 * i, Bytes.toInt(firstTwo.getValue(FAMILY, CQS[1])));

      Result secondOne = results.get(2 * i + 1);
      assertEquals(String.format("%02d", i), Bytes.toString(secondOne.getRow()));
      assertEquals(1, secondOne.size());
      assertEquals(3 * i, Bytes.toInt(secondOne.getValue(FAMILY, CQS[2])));
    }
  }

  @Test
  public void testReversedBatchDoNotAllowPartial() throws InterruptedException, ExecutionException {
    // we set batch to 2 and max result size to 1, then server will only returns one result per call
    // but we should get 2 + 1 for every row.
    List<Result> results =
      TABLE.scanAll(new Scan().setBatch(2).setMaxResultSize(1).setReversed(true)).get();
    assertEquals(2 * COUNT, results.size());
    for (int i = 0; i < COUNT; i++) {
      int row = COUNT - i - 1;
      Result firstTwo = results.get(2 * i);
      assertEquals(String.format("%02d", row), Bytes.toString(firstTwo.getRow()));
      assertEquals(2, firstTwo.size());
      assertEquals(row, Bytes.toInt(firstTwo.getValue(FAMILY, CQS[0])));
      assertEquals(2 * row, Bytes.toInt(firstTwo.getValue(FAMILY, CQS[1])));

      Result secondOne = results.get(2 * i + 1);
      assertEquals(String.format("%02d", row), Bytes.toString(secondOne.getRow()));
      assertEquals(1, secondOne.size());
      assertEquals(3 * row, Bytes.toInt(secondOne.getValue(FAMILY, CQS[2])));
    }
  }
}
