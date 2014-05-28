/**
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
package org.apache.hadoop.hbase.thrift;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableAsync;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.ProfilingData;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Check whether we correctly receive profiling data with the header protocol
 * enabled
 *
 */
@Category(MediumTests.class)
public class TestHeaderSendReceive {
  private static HBaseTestingUtility TEST_UTIL;
  private static final int SLAVES = 1;
  static final byte[] TABLE1 = Bytes.toBytes("testTable");
  static final byte[] FAMILY = Bytes.toBytes("family");
  static final byte[][] FAMILIES = new byte[][] { FAMILY };
  byte[] r1 = Bytes.toBytes("r1");
  byte[] r2 = Bytes.toBytes("r2");
  byte[] value = Bytes.toBytes("test-value");

  @Before
  public void setUp() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(TABLE1)) {
      if (admin.isTableEnabled(TABLE1)) {
        admin.disableTable(TABLE1);
      }
      admin.deleteTable(TABLE1);
    }
    TEST_UTIL.createTable(TABLE1, FAMILIES);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * The client requests profiling
   */
  @Test
  public void testProfilingData() throws Exception {
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), TABLE1);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.setProfiling(true);
    ht.put(put);
    ProfilingData pd = ht.getProfilingData();
    assertNotNull(pd);
    System.out.println("profiling data after first put: " + pd);
    // check if we are gettng all necessary profiling params
    checkProfiling(false, true, pd);


    // disable profiling and check that we get no profiling data back
    ht.setProfiling(false);
    ht.put(put);
    pd = ht.getProfilingData();
    assertNull(pd);

    put = new Put(r2);
    put.add(FAMILY, null, value);
    ht.setProfiling(true);
    ht.put(put);
    pd = ht.getProfilingData();
    assertNotNull(pd);
    System.out.println("profiling data after second put: " + pd);
    checkProfiling(false, true, pd);

    // make a get
    Get get = new Get.Builder(r1).addFamily(FAMILY).create();
    ht.setProfiling(true);
    ht.get(get);
    pd = ht.getProfilingData();
    System.out.println("profiling data after get: " + pd);
    assertNotNull(pd);
    checkProfiling(true, false, pd);

    // test async get
    ht.setProfiling(true);
    try (HTableAsync async = new HTableAsync(ht)) {
      async.getAsync(get).get();
      pd = async.getProfilingData();
      System.out.println("profiling data after get: " + pd);
      assertNotNull(pd);
    }
    checkProfiling(true, false, pd);
    ht.close();
  }

  /**
   * Check that all the necessary profiling info is in profiling data and is not
   * equal to zero
   *
   * @param isGet
   *          - if the last htable operation was get
   * @param isPut
   *          - !isGet (if the last htable operation was put
   * @param pd
   *          - profiling data object
   */
  public void checkProfiling(boolean isGet, boolean isPut, ProfilingData pd) {
    assertNotNull(ProfilingData.CLIENT_NETWORK_LATENCY_MS + " is null",
        pd.getLong(ProfilingData.CLIENT_NETWORK_LATENCY_MS));
    Assert.assertTrue(ProfilingData.CLIENT_NETWORK_LATENCY_MS
        + " should be greater than zero",
        pd.getLong(ProfilingData.CLIENT_NETWORK_LATENCY_MS).longValue() >= 0L);
    assertNotNull(ProfilingData.TOTAL_SERVER_TIME_MS + " is null",
        pd.getLong(ProfilingData.TOTAL_SERVER_TIME_MS));
    Assert.assertTrue(ProfilingData.TOTAL_SERVER_TIME_MS
        + " should be greater than zero",
        pd.getLong(ProfilingData.TOTAL_SERVER_TIME_MS).longValue() >= 0L);
    if (isPut) {
      assertNotNull(ProfilingData.HLOG_SYNC_TIME_MS + " is null",
          pd.getLong(ProfilingData.HLOG_SYNC_TIME_MS));
    }
  }

  /**
   * Check whether profiling works if it is enabled from the serverside and NOT
   * on the client side
   *
   */
  @Test
  public void testServerSideEnabledProfiling() throws IOException {
    HRegionServer.enableServerSideProfilingForAllCalls.set(true);
    HTable ht;
    try {
      ht = new HTable(TEST_UTIL.getConfiguration(), TABLE1);
      Put p = new Put(r1);
      p.add(FAMILY, null, value);
      ht.put(p);
      ProfilingData pd = ht.getProfilingData();
      assertNotNull(pd);
      System.out.println("profiling data: " + pd);
      ht.close();
    } finally {
      HRegionServer.enableServerSideProfilingForAllCalls.set(false);
    }
  }

}
