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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

@Category({MediumTests.class, ClientTests.class})
public class TestRegionLocationCaching {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionLocationCaching.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static int SLAVES = 1;
  private static int PER_REGIONSERVER_QUEUE_SIZE = 100000;
  private static TableName TABLE_NAME = TableName.valueOf("TestRegionLocationCaching");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
    TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY });
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCachingForHTableMultiplexerSinglePut() throws Exception {
    HTableMultiplexer multiplexer =
        new HTableMultiplexer(TEST_UTIL.getConfiguration(), PER_REGIONSERVER_QUEUE_SIZE);
    byte[] row = Bytes.toBytes("htable_multiplexer_single_put");
    byte[] value = Bytes.toBytes("value");

    Put put = new Put(row);
    put.addColumn(FAMILY, QUALIFIER, value);
    assertTrue("Put request not accepted by multiplexer queue", multiplexer.put(TABLE_NAME, put));

    checkRegionLocationIsCached(TABLE_NAME, multiplexer.getConnection());
    checkExistence(TABLE_NAME, row, FAMILY, QUALIFIER);

    multiplexer.close();
  }

  @Test
  public void testCachingForHTableMultiplexerMultiPut() throws Exception {
    HTableMultiplexer multiplexer =
        new HTableMultiplexer(TEST_UTIL.getConfiguration(), PER_REGIONSERVER_QUEUE_SIZE);

    List<Put> multiput = new ArrayList<Put>();
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("htable_multiplexer_multi_put" + i));
      byte[] value = Bytes.toBytes("value_" + i);
      put.addColumn(FAMILY, QUALIFIER, value);
      multiput.add(put);
    }

    List<Put> failedPuts = multiplexer.put(TABLE_NAME, multiput);
    assertNull("All put requests were not accepted by multiplexer queue", failedPuts);

    checkRegionLocationIsCached(TABLE_NAME, multiplexer.getConnection());
    for (int i = 0; i < 10; i++) {
      checkExistence(TABLE_NAME, Bytes.toBytes("htable_multiplexer_multi_put" + i), FAMILY,
        QUALIFIER);
    }

    multiplexer.close();
  }

  @Test
  public void testCachingForHTableSinglePut() throws Exception {
    byte[] row = Bytes.toBytes("htable_single_put");
    byte[] value = Bytes.toBytes("value");

    Put put = new Put(row);
    put.addColumn(FAMILY, QUALIFIER, value);

    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      table.put(put);
    }

    checkRegionLocationIsCached(TABLE_NAME, TEST_UTIL.getConnection());
    checkExistence(TABLE_NAME, row, FAMILY, QUALIFIER);
  }

  @Test
  public void testCachingForHTableMultiPut() throws Exception {
    List<Put> multiput = new ArrayList<Put>();
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("htable_multi_put" + i));
      byte[] value = Bytes.toBytes("value_" + i);
      put.addColumn(FAMILY, QUALIFIER, value);
      multiput.add(put);
    }

    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      table.put(multiput);
    }
    checkRegionLocationIsCached(TABLE_NAME, TEST_UTIL.getConnection());
    for (int i = 0; i < 10; i++) {
      checkExistence(TABLE_NAME, Bytes.toBytes("htable_multi_put" + i), FAMILY, QUALIFIER);
    }
  }

  /**
   * Method to check whether the cached region location is non-empty for the given table. It repeats
   * the same check several times as clearing of cache by some async operations may not reflect
   * immediately.
   */
  private void checkRegionLocationIsCached(final TableName tableName, final Connection conn)
      throws InterruptedException, IOException {
    for (int count = 0; count < 50; count++) {
      int number = ((ConnectionImplementation) conn).getNumberOfCachedRegionLocations(tableName);
      assertNotEquals("Expected non-zero number of cached region locations", 0, number);
      Thread.sleep(100);
    }
  }

  /**
   * Method to check whether the passed row exists in the given table
   */
  private static void checkExistence(final TableName tableName, final byte[] row,
      final byte[] family, final byte[] qualifier) throws Exception {
    // verify that the row exists
    Result r;
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    int nbTry = 0;
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      do {
        assertTrue("Failed to get row after " + nbTry + " tries", nbTry < 50);
        nbTry++;
        Thread.sleep(100);
        r = table.get(get);
      } while (r == null || r.getValue(family, qualifier) == null);
    }
  }
}
