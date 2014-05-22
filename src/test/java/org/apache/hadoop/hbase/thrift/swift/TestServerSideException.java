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

import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableAsync;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.regionserver.FailureInjectingThriftHRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionOverloadedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.experimental.categories.Category;

/**
 * Test failure handling in HTableAsync. We can also use it in
 */
@Category(MediumTests.class)
public class TestServerSideException {
  private static final Log LOG = LogFactory.getLog(TestServerSideException.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int REGION_SERVERS = 2;

  static final byte[] TABLE = Bytes.toBytes("testTable");
  static final byte[] FAMILY = Bytes.toBytes("testFamily");
  static final byte[] ROW = Bytes.toBytes("testRow");
  static final byte[] VALUE = Bytes.toBytes("testValue");

  private static HTableAsync table = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    conf.setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    conf.setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);

    conf.setClass(HConstants.THRIFT_REGION_SERVER_IMPL,
        FailureInjectingThriftHRegionServer.class, ThriftHRegionInterface.Sync.class);

    conf.setInt(HConstants.CLIENT_RETRY_NUM_STRING, 5);
    // Server will allow client to retry once when there is RegionOverloadedException
    conf.setInt(HConstants.SERVER_REQUESTED_RETRIES_STRING, 1);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 200);

    TEST_UTIL.startMiniCluster(REGION_SERVERS);

    table = TEST_UTIL.createTable(TABLE, FAMILY);

    Put put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    table.put(put);
    table.flushCommits();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test if HTableAsync behaves correctly when the server throws IOException.
   *
   * @throws Exception
   */
  @Test
  public void testRetriesExhaustedException() throws Exception {
    // When server continues to throw IOException, we should see RetriesExhaustedException
    FailureInjectingThriftHRegionServer.setFailureMode(
        FailureInjectingThriftHRegionServer.FailureType.MIXEDRETRIABLEEXCEPTIONS, Integer.MAX_VALUE);

    Get get = new Get.Builder(ROW).addFamily(FAMILY).create();
    ListenableFuture<Result> future = table.getAsync(get);
    boolean hasIOE = false;
    try {
      future.get();
    } catch (ExecutionException e) {
      LOG.debug("Got exception", e);
      if (e.getCause() instanceof RetriesExhaustedException) {
        hasIOE = true;
      }
    }
    Assert.assertTrue(hasIOE);
  }

  /**
   * Test if HTableAsync behaves correctly when server requests it to wait.
   *
   * @throws Exception
   */
  @Test
  public void testRegionOverloadedExceptions() throws Exception {
    // Excpect the client to retry once and succeed on the second time
    FailureInjectingThriftHRegionServer.setFailureMode(
        FailureInjectingThriftHRegionServer.FailureType.REGIONOVERLOADEDEXCEPTION, 1);

    Get get = new Get.Builder(ROW).addFamily(FAMILY).create();
    ListenableFuture<Result> future = table.getAsync(get);
    Result result = future.get();
    Assert.assertTrue(Bytes.equals(result.getValue(FAMILY, null), VALUE));

    // Expect the client to retry once and fail on the second attempt.
    // In the mean time, it should sleep enough time as the server requested.
    FailureInjectingThriftHRegionServer.setFailureMode(
        FailureInjectingThriftHRegionServer.FailureType.REGIONOVERLOADEDEXCEPTION, 2);

    future = table.getAsync(get);
    long futureStartTime = System.currentTimeMillis();
    boolean hasROE = false;
    try {
      future.get();
    } catch (ExecutionException e) {
      LOG.debug("Got exception", e);
      if (e.getCause() instanceof RegionOverloadedException) {
        long futureFinishTime = System.currentTimeMillis();
        // The default value is 1000ms, larger than 200ms set for normal retries.
        // So we know client listens to the instruction of server.
        Assert.assertTrue(futureFinishTime - futureStartTime > HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
        hasROE = true;
      }
    }
    Assert.assertTrue(hasROE);
  }

  /**
   * Test if HTableAsync is able to retry for different types of exceptions.
   *
   * @throws Exception
   */
  @Test
  public void testRetriableExceptions() throws Exception {
    // There are 5 retries. First 4 should fail and the last one should succeed.
    FailureInjectingThriftHRegionServer.setFailureMode(
        FailureInjectingThriftHRegionServer.FailureType.MIXEDRETRIABLEEXCEPTIONS, 4);

    Get get = new Get.Builder(ROW).addFamily(FAMILY).create();
    ListenableFuture<Result> future = table.getAsync(get);
    Result result = future.get();
    Assert.assertTrue(Bytes.equals(result.getValue(FAMILY, null), VALUE));
  }

  /**
   * Test if HTableAsync immediately fail on some exceptions as expected.
   *
   * @throws Exception
   */
  @Test
  public void testNonRetriableExceptions() throws Exception {
    FailureInjectingThriftHRegionServer.setFailureMode(
        FailureInjectingThriftHRegionServer.FailureType.DONOTRETRYEXCEPTION, 1);
    Get get = new Get.Builder(ROW).addFamily(FAMILY).create();
    ListenableFuture<Result> future = table.getAsync(get);
    boolean hasDoNotRetryIOE = false;
    try {
      future.get();
    } catch (ExecutionException e) {
      LOG.debug("Got exception", e);
      if (e.getCause() instanceof DoNotRetryIOException) {
        hasDoNotRetryIOE = true;
      }
    }
    Assert.assertTrue(hasDoNotRetryIOE);
  }


}
