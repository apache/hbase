/*
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
package org.apache.hadoop.hbase.thrift.swift;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableAsync;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.regionserver.FailureInjectingThriftHRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestServerSideExceptionDestructive {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int REGION_SERVERS = 2;

  static final byte[] TABLE = Bytes.toBytes("testTable");
  static final byte[] FAMILY = Bytes.toBytes("testFamily");
  static final byte[] ROW = Bytes.toBytes("testRow");
  static final byte[] VALUE = Bytes.toBytes("testValue");

  private static HTableAsync table = null;


  @Before
  public void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    conf.setClass(HConstants.THRIFT_REGION_SERVER_IMPL,
        FailureInjectingThriftHRegionServer.class, ThriftHRegionInterface.Async.class);

    conf.setInt(HConstants.CLIENT_RETRY_NUM_STRING, 5);

    TEST_UTIL.startMiniCluster(REGION_SERVERS);

    table = TEST_UTIL.createTable(TABLE, FAMILY);

    Put put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    table.flushCommits();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test if HTableAsync is able to retry in network failure (TTransportException).
   *
   * WARNING: This test affects number of region servers. Please add new tests above
   * it to avoid possible unnecessary debugging.
   *
   * @throws Exception
   */
  @Test
  public void testTTransportException() throws Exception {
    HTableAsync table = TEST_UTIL.createTable(new StringBytes("testTable2"),
        new byte[][] { FAMILY }, 3, Bytes.toBytes("bbb"),
        Bytes.toBytes("yyy"), 6);

    Put put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    table.flushCommits();

    FailureInjectingThriftHRegionServer.setFailureMode(
        FailureInjectingThriftHRegionServer.FailureType.STOP, 1);

    Get get = new Get.Builder(ROW).addFamily(FAMILY).create();
    ListenableFuture<Result> future = table.getAsync(get);
    Result result = future.get();
    Assert.assertTrue(Bytes.equals(result.getValue(FAMILY, null), VALUE));
  }
}
