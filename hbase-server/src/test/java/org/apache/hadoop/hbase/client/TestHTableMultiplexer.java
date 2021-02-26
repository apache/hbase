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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({LargeTests.class, ClientTests.class})
public class TestHTableMultiplexer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHTableMultiplexer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHTableMultiplexer.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[] VALUE1 = Bytes.toBytes("testValue1");
  private static byte[] VALUE2 = Bytes.toBytes("testValue2");
  private static int SLAVES = 3;
  private static int PER_REGIONSERVER_QUEUE_SIZE = 100000;

  @Rule
  public TestName name = new TestName();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void checkExistence(Table htable, byte[] row, byte[] family, byte[] quality)
      throws Exception {
    // verify that the Get returns the correct result
    Result r;
    Get get = new Get(row);
    get.addColumn(FAMILY, QUALIFIER);
    int nbTry = 0;
    do {
      assertTrue("Fail to get from " + htable.getName() + " after " + nbTry + " tries", nbTry < 50);
      nbTry++;
      Thread.sleep(100);
      r = htable.get(get);
    } while (r == null || r.getValue(FAMILY, QUALIFIER) == null);
    assertEquals("value", Bytes.toStringBinary(VALUE1),
      Bytes.toStringBinary(r.getValue(FAMILY, QUALIFIER)));
  }

  @Test
  public void testHTableMultiplexer() throws Exception {
    final TableName tableName1 = TableName.valueOf(name.getMethodName() + "_1");
    final TableName tableName2 = TableName.valueOf(name.getMethodName() + "_2");
    final int NUM_REGIONS = 10;
    final int VERSION = 3;
    List<Put> failedPuts;
    boolean success;

    HTableMultiplexer multiplexer = new HTableMultiplexer(TEST_UTIL.getConfiguration(),
        PER_REGIONSERVER_QUEUE_SIZE);

    Table htable1 =
        TEST_UTIL.createTable(tableName1, new byte[][] { FAMILY }, VERSION,
        Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
    Table htable2 =
        TEST_UTIL.createTable(tableName2, new byte[][] { FAMILY }, VERSION, Bytes.toBytes("aaaaa"),
          Bytes.toBytes("zzzzz"), NUM_REGIONS);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName1);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName2);

    try (RegionLocator rl = TEST_UTIL.getConnection().getRegionLocator(tableName1)) {
      byte[][] startRows = rl.getStartKeys();
      byte[][] endRows = rl.getEndKeys();

      // SinglePut case
      for (int i = 0; i < NUM_REGIONS; i++) {
        byte [] row = startRows[i];
        if (row == null || row.length <= 0) continue;
        Put put = new Put(row).addColumn(FAMILY, QUALIFIER, VALUE1);
        success = multiplexer.put(tableName1, put);
        assertTrue("multiplexer.put returns", success);

        put = new Put(row).addColumn(FAMILY, QUALIFIER, VALUE1);
        success = multiplexer.put(tableName2, put);
        assertTrue("multiplexer.put failed", success);

        LOG.info("Put for " + Bytes.toStringBinary(startRows[i]) + " @ iteration " + (i + 1));

        // verify that the Get returns the correct result
        checkExistence(htable1, startRows[i], FAMILY, QUALIFIER);
        checkExistence(htable2, startRows[i], FAMILY, QUALIFIER);
      }

      // MultiPut case
      List<Put> multiput = new ArrayList<>();
      for (int i = 0; i < NUM_REGIONS; i++) {
        byte [] row = endRows[i];
        if (row == null || row.length <= 0) continue;
        Put put = new Put(row);
        put.addColumn(FAMILY, QUALIFIER, VALUE2);
        multiput.add(put);
      }
      failedPuts = multiplexer.put(tableName1, multiput);
      assertTrue(failedPuts == null);

      // verify that the Get returns the correct result
      for (int i = 0; i < NUM_REGIONS; i++) {
        byte [] row = endRows[i];
        if (row == null || row.length <= 0) continue;
        Get get = new Get(row);
        get.addColumn(FAMILY, QUALIFIER);
        Result r;
        int nbTry = 0;
        do {
          assertTrue(nbTry++ < 50);
          Thread.sleep(100);
          r = htable1.get(get);
        } while (r == null || r.getValue(FAMILY, QUALIFIER) == null ||
            Bytes.compareTo(VALUE2, r.getValue(FAMILY, QUALIFIER)) != 0);
      }
    }
  }
}
