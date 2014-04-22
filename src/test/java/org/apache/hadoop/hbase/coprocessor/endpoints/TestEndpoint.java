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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.endpoints.EndpointLib.IAggregator;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointClient.Caller;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase for endpoint
 */
public class TestEndpoint {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final StringBytes TABLE_NAME = new StringBytes("cp");
  private static final byte[] FAMILY_NAME = Bytes.toBytes("f");
  private static final byte[] QUALITY_NAME = Bytes.toBytes("q");

  @Before
  public void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    conf.set(HBaseTestingUtility.FS_TYPE_KEY, HBaseTestingUtility.FS_TYPE_LFS);

    conf.setStrings(EndpointLoader.FACTORY_CLASSES_KEY,
        new String[] { SummerFactory.class.getName() });

    TEST_UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static class SummerFactory implements IEndpointFactory<ISummer> {
    @Override
    public ISummer create() {
      return new Summer();
    }

    @Override
    public Class<ISummer> getEndpointInterface() {
      return ISummer.class;
    }
  }

  /**
   * This is an example of an endpoint interface. It computes the total sum of
   * all the values of family FAMILY_NAME at quality QUALITY_NAME as longs.
   *
   */
  public static interface ISummer extends IEndpoint {
    long sum(int offset) throws IOException;
  }

  /**
   * The implementation of ISummer.
   */
  public static class Summer implements ISummer, IAggregator {
    IEndpointContext context;
    int offset;
    long result;

    @Override
    public void setContext(IEndpointContext context) {
      this.context = context;
    }

    @Override
    public long sum(int offset) throws IOException {
      HRegion region = context.getRegion();
      Scan scan = new Scan();
      scan.addFamily(FAMILY_NAME);
      scan.addColumn(FAMILY_NAME, QUALITY_NAME);

      this.offset = offset;
      this.result = 0L;
      EndpointLib.aggregateScan(region, scan, this);
      return this.result;
    }

    @Override
    public void aggregate(KeyValue kv) {
      long vl = Bytes.toLong(kv.getBuffer(), kv.getValueOffset(),
          kv.getValueLength());
      this.result += vl + offset;
    }
  }

  @Test(timeout = 180000)
  public void testSummer() throws Exception {
    // Create the table
    HTableInterface table = TEST_UTIL.createTable(TABLE_NAME, FAMILY_NAME);

    // Put some values
    for (int i = 1; i <= 10; i++) {
      table.put(new Put(Bytes.toBytes("row" + i)).add(FAMILY_NAME,
          QUALITY_NAME, Bytes.toBytes((long) i)));
    }

    IEndpointClient cp = (IEndpointClient) table;

    // Calling endpoints with zero offsets.
    Map<HRegionInfo, Long> results = cp.coprocessorEndpoint(ISummer.class, null,
        null, new Caller<ISummer, Long>() {
          @Override
          public Long call(ISummer client) throws IOException {
            return client.sum(0);
          }
        });

    // Aggregates results from all regions
    long sum = 0;
    for (Long res : results.values()) {
      sum += res;
    }

    // Check the final results
    Assert.assertEquals("sum", 55, sum);

    // Calling endpoints with -1 offsets.
    results = cp.coprocessorEndpoint(ISummer.class, null,
        null, new Caller<ISummer, Long>() {
          @Override
          public Long call(ISummer client) throws IOException {
            return client.sum(-1);
          }
        });

    // Aggregates results from all regions
    sum = 0;
    for (Long res : results.values()) {
      sum += res;
    }

    // Check the final results
    Assert.assertEquals("sum", 45, sum);
  }
}
