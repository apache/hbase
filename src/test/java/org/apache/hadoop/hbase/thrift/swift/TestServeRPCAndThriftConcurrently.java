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

package org.apache.hadoop.hbase.thrift.swift;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Verify that we are able to serve Thrift and RPC requests in parallel. This
 * is to ensure that we can serve Hadoop RPC requests while we are transitioning
 * to Thrift.
 */
public class TestServeRPCAndThriftConcurrently extends TestCase {
  protected final static Log LOG =
    LogFactory.getLog(TestServeRPCAndThriftConcurrently.class);

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final int SLAVES = 1;
  static final byte[] TABLE = Bytes.toBytes("testTable");
  static final byte[] FAMILY = Bytes.toBytes("family");
  static final byte[][] FAMILIES = new byte[][] { FAMILY };
  private Configuration conf;

  /**
   * Create two clients, one which talks Thrift, the other which talks
   * Hadoop RPC. Let one of them do puts, the other will do gets to verify
   * those puts.
   */
  public void testSimpleGetsAndPutsInParallel(boolean writeThriftPort)
    throws IOException, InterruptedException {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.REGIONSERVER_USE_HADOOP_RPC, true);
    conf.setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT, true);
    conf.setBoolean(
      HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, writeThriftPort);
    conf.setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
      false);
    TEST_UTIL.startMiniCluster(SLAVES);

    HTable thriftClient = TEST_UTIL.createTable(TABLE, FAMILIES);

    conf.setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT, false);
    HTable rpcClient = new HTable(conf, TABLE);

    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("r1");
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    thriftClient.put(put);

    Result result;
    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    result = rpcClient.get(g);
    assertTrue(Bytes.equals(result.getValue(FAMILY, null), value));

    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test if we can run both of them if the Thrift port was written in meta.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public void testConcurrentWorkingWithThriftPortInMeta()
    throws IOException, InterruptedException {
    testSimpleGetsAndPutsInParallel(true);
  }

  /**
   * Test if we can run both of them if the Hadoop port was written in meta.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public void testConcurrentWorkingWithHadoopPortInMeta()
    throws IOException, InterruptedException {
    testSimpleGetsAndPutsInParallel(false);
  }
}
