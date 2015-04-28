/**
 * Copyright The Apache Software Foundation
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LargeTests.class, ClientTests.class})
public class TestHTableMultiplexer {
  private static final Log LOG = LogFactory.getLog(TestHTableMultiplexer.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[] VALUE1 = Bytes.toBytes("testValue1");
  private static byte[] VALUE2 = Bytes.toBytes("testValue2");
  private static int SLAVES = 3;
  private static int PER_REGIONSERVER_QUEUE_SIZE = 100000;

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

  private static void checkExistence(HTable htable, byte[] row, byte[] family, byte[] quality)
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
    TableName TABLE_1 = TableName.valueOf("testHTableMultiplexer_1");
    TableName TABLE_2 = TableName.valueOf("testHTableMultiplexer_2");
    final int NUM_REGIONS = 10;
    final int VERSION = 3;
    List<Put> failedPuts;
    boolean success;
    
    HTableMultiplexer multiplexer = new HTableMultiplexer(TEST_UTIL.getConfiguration(), 
        PER_REGIONSERVER_QUEUE_SIZE);

    HTable htable1 =
        TEST_UTIL.createTable(TABLE_1, new byte[][] { FAMILY }, VERSION,
        Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
    HTable htable2 =
        TEST_UTIL.createTable(TABLE_2, new byte[][] { FAMILY }, VERSION, Bytes.toBytes("aaaaa"),
          Bytes.toBytes("zzzzz"), NUM_REGIONS);
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_1);
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_2);

    byte[][] startRows = htable1.getStartKeys();
    byte[][] endRows = htable1.getEndKeys();

    // SinglePut case
    for (int i = 0; i < NUM_REGIONS; i++) {
      byte [] row = startRows[i];
      if (row == null || row.length <= 0) continue;
      Put put = new Put(row).add(FAMILY, QUALIFIER, VALUE1);
      success = multiplexer.put(TABLE_1, put);
      assertTrue("multiplexer.put returns", success);

      put = new Put(row).add(FAMILY, QUALIFIER, VALUE1);
      success = multiplexer.put(TABLE_2, put);
      assertTrue("multiplexer.put failed", success);

      LOG.info("Put for " + Bytes.toStringBinary(startRows[i]) + " @ iteration " + (i + 1));

      // verify that the Get returns the correct result
      checkExistence(htable1, startRows[i], FAMILY, QUALIFIER);
      checkExistence(htable2, startRows[i], FAMILY, QUALIFIER);
    }

    // MultiPut case
    List<Put> multiput = new ArrayList<Put>();
    for (int i = 0; i < NUM_REGIONS; i++) {
      byte [] row = endRows[i];
      if (row == null || row.length <= 0) continue;
      Put put = new Put(row);
      put.add(FAMILY, QUALIFIER, VALUE2);
      multiput.add(put);
    }
    failedPuts = multiplexer.put(TABLE_1, multiput);
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
