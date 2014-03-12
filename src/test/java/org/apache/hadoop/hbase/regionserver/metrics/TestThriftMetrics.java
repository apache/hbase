/**
 * Copyright 2013 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver.metrics;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * Test to verify that the thrift metrics are calculated and propagated in the
 * HBaseRpcMetrics.
 */
public class TestThriftMetrics extends TestCase {
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final int SLAVES = 1;
  static final byte[] TABLE = Bytes.toBytes("testTable");
  static final byte[] FAMILY = Bytes.toBytes("family");
  static final byte[][] FAMILIES = new byte[][] { FAMILY };

  /**
   * A simple test to see if the metrics are populated.
   * @throws IOException
   */
  @Test
  public void testThriftMetricsArePopulated() throws IOException,
    InterruptedException {
    TEST_UTIL.startMiniCluster(SLAVES);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
      true);

    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");

    // Make a simple put call through thrift.
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.put(put);
    ht.flushCommits();

    // Make a simple get call through thrift
    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    ht.get(g);

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLE);

    boolean containsPut = false, containsGet = false;
    for (String key : rs.getThriftMetrics().registry.getKeyList()) {
      // Check if there is a key which ends with MultiPut and Get.
      if (key.endsWith("multiPut")) {
        containsPut = true;
      } else if (key.endsWith("get")) {
        containsGet = true;
      }
    }
    assertTrue("Did not find an RPC Metrics entry for put", containsPut);
    assertTrue("Did not find an RPC Metrics entry for get", containsGet);
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test if we report operations which are too slow and large.
   * @throws IOException
   */
  public void testAbnormalOperationsAreReported() throws IOException,
    InterruptedException {
    // Set the warning time to 0, so that we get tooSlow alarms triggered.
    TEST_UTIL.getConfiguration().setInt("hbase.ipc.warn.response.time", 0);
    // Set the warning size to 0, so that we get largeResponse alarms triggered.
    TEST_UTIL.getConfiguration().setLong("hbase.ipc.warn.response.size", 0);
    TEST_UTIL.startMiniCluster(SLAVES);

    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");

    // Make a simple put call through thrift.
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.put(put);
    ht.flushCommits();

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLE);
    boolean containsSlowResponse = false;
    boolean containsLargeResponse = false;
    for (String key : rs.getThriftMetrics().registry.getKeyList()) {
      if (key.endsWith("slowResponse.")) {
        containsSlowResponse = true;
      } else if (key.endsWith("largeResponse.kb.")) {
        containsLargeResponse = true;
      }
    }
    assertTrue("Did not find an RPC Metrics entry for slow response",
               containsSlowResponse);
    assertTrue("Did not find an RPC Metrics entry for large response",
               containsLargeResponse);
    TEST_UTIL.shutdownMiniCluster();
  }
}
