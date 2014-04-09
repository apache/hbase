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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.endpoints.EndpointLib;
import org.apache.hadoop.hbase.coprocessor.endpoints.EndpointLib.IAggregator;
import org.apache.hadoop.hbase.coprocessor.endpoints.EndpointManager;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpoint;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointClient;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointClient.Caller;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointContext;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase for endpoint
 */
public class TestEndpoint {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TABLE_NAME = Bytes.toBytes("cp");
  private static final byte[] FAMILY_NAME = Bytes.toBytes("f");
  private static final byte[] QUALITY_NAME = Bytes.toBytes("q");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
    // Register an endpoint in the server side.
    EndpointManager.get().register(ISummer.class,
        new IEndpointFactory<ISummer>() {
          @Override
          public ISummer create() {
            return new Summer();
          }
        });
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This is an example of an endpoint interface. It computes the total sum of
   * all the values of family FAMILY_NAME at quality QUALITY_NAME as longs.
   *
   */
  public static interface ISummer extends IEndpoint {
    byte[] sum() throws IOException;
  }

  /**
   * The implementation of ISummer.
   */
  public static class Summer implements ISummer, IAggregator {
    IEndpointContext context;
    long result;

    @Override
    public void setContext(IEndpointContext context) {
      this.context = context;
    }

    @Override
    public byte[] sum() throws IOException {
      HRegion region = context.getRegion();
      Scan scan = new Scan();
      scan.addFamily(FAMILY_NAME);
      scan.addColumn(FAMILY_NAME, QUALITY_NAME);

      this.result = 0L;
      EndpointLib.aggregateScan(region, scan, this);
      return Bytes.toBytes(this.result);
    }

    @Override
    public void aggregate(KeyValue kv) {
      this.result += Bytes.toLong(kv.getBuffer(), kv.getValueOffset(),
          kv.getValueLength());
    }
  }

  @Test
  public void testSummer() throws Exception {
    // Create the table
    HTableInterface table = TEST_UTIL.createTable(TABLE_NAME, FAMILY_NAME);

    // Put some values
    for (int i = 1; i <= 10; i++) {
      table.put(new Put(Bytes.toBytes("row" + i)).add(FAMILY_NAME,
          QUALITY_NAME, Bytes.toBytes((long) i)));
    }

    // Calling endpoints.
    IEndpointClient cp = (IEndpointClient) table;
    Map<byte[], byte[]> results = cp.coprocessorEndpoint(ISummer.class, null,
        null, new Caller<ISummer>() {
          @Override
          public byte[] call(ISummer client) throws IOException {
            return client.sum();
          }
        });

    // Aggregates results from all regions
    long sum = 0;
    for (byte[] res : results.values()) {
      sum += Bytes.toLong(res);
    }

    // Check the final results
    Assert.assertEquals("sum", 55, sum);
  }
}
