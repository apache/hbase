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

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.ipc.thrift.HBaseThriftRPC;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This is a test class to verify if the Thrift Exceptions work as expected.
 */
@Category(MediumTests.class)
public class TestThriftExceptions {
  /**
   * Test if the ThriftHBaseException can be serialized and deserialized
   * correctly.
   */
  @Test
  public void testSerialization() throws Exception {
    IOException ioe = new IOException("FooBar");
    ThriftHBaseException thriftHBaseException = new ThriftHBaseException(ioe);
    byte[] thriftHBaseExceptionBytes =
      Bytes.writeThriftBytes(thriftHBaseException, ThriftHBaseException.class);

    ThriftHBaseException deserThriftHBaseException =
      Bytes.readThriftBytes(thriftHBaseExceptionBytes,
                            ThriftHBaseException.class);

    assertEquals(deserThriftHBaseException, thriftHBaseException);
  }


  /**
   * Test if the exceptions raised on the server end, are wrapped correctly
   * in the ThriftHBaseException and are decoded correctly using the
   * HBaseThriftAdapter.
   */
  @Test
  public void testExceptionTranslation() throws InterruptedException, IOException {
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    testUtil.getConfiguration().setBoolean(
      HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    testUtil.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
      true);
    testUtil.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
      true);
    testUtil.startMiniCluster(1);
    Configuration conf = testUtil.getConfiguration();

    int port = testUtil.getHBaseCluster().getRegionServer(0).getThriftServerPort();

    InetSocketAddress addr = new InetSocketAddress(port);
    HRegionInterface client = (HRegionInterface) HBaseThriftRPC.getClient(addr,
      conf, ThriftHRegionInterface.Async.class, HBaseRPCOptions.DEFAULT);

    boolean illegalArgumentException = false;
    try {
      client.flushRegion(Bytes.toBytes("foobar"));
    } catch (IllegalArgumentException e) {
      illegalArgumentException = true;
    } catch (Exception e) {
      assertFalse(true);
    }
    assertTrue("Expected IllegalArgumentException", illegalArgumentException);

    boolean notServingRegionException = false;
    try {
      client.getClosestRowBefore(Bytes.toBytes("foo"),
      Bytes.toBytes("bar"), Bytes.toBytes("baz"));
    } catch (NotServingRegionException e) {
      notServingRegionException = true;
    }
    assertTrue("Expected NotServingRegionException", notServingRegionException);
    testUtil.shutdownMiniCluster();
  }
}
