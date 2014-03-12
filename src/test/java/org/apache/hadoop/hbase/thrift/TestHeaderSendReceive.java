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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.ProfilingData;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.RubyProcess.Sys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Chech whether we correctly receive profiling data with the header protocol enabled
 *
 */

public class TestHeaderSendReceive {
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final int SLAVES = 1;
  static final byte[] TABLE = Bytes.toBytes("testTable");
  static final byte[] FAMILY = Bytes.toBytes("family");
  static final byte[][] FAMILIES = new byte[][] { FAMILY };
  byte[] r1 = Bytes.toBytes("r1");
  byte[] r2 = Bytes.toBytes("r2");
  byte[] value = Bytes.toBytes("test-value");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * The client requests profiling
   */
  @Test
  public void testProfilingData() throws IOException {
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.setProfiling(true);
    ht.put(put);
    ProfilingData pd = ht.getProfilingData();
    assertTrue(pd != null);
    System.out.println("profiling data after first put: " + pd);

    // disable profiling and check that we get no profiling data back
    ht.setProfiling(false);
    ht.put(put);
    pd = ht.getProfilingData();
    assertTrue(pd == null);

    put = new Put(r2);
    put.add(FAMILY, null, value);
    ht.setProfiling(true);
    ht.put(put);
    pd = ht.getProfilingData();
    assertTrue(pd != null);
    System.out.println("profiling data after second put: " + pd);

    // make a get
    Get get = new Get.Builder(r1).addFamily(FAMILY).create();
    ht.setProfiling(true);
    ht.get(get);
    pd = ht.getProfilingData();
    System.out.println("profiling data after get: " + pd);
    assertTrue(pd != null);
  }

  /**
   * Check whether profiling works if it is enabled from the serverside and NOT
   * on the client side
   *
   * @throws IOException
   */
  @Test
  public void testServerSideEnabledProfiling() throws IOException {
    HRegionServer.enableServerSideProfilingForAllCalls.set(true);
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put p = new Put(r1);
    p.add(FAMILY, null, value);
    ht.put(p);
    ProfilingData pd = ht.getProfilingData();
    assertTrue(pd != null);
    System.out.println("profiling data: " + pd);
  }

}
