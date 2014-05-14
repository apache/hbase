/**
 * Copyright 2014 The Apache Software Foundation
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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointClient.Caller;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcases for endpoints defined in LongAggregators.
 */
@Category(MediumTests.class)
public class TestLongAggregator {
  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static final byte[] FAMILY_NAME = Bytes.toBytes("f");
  private static final byte[] QUALITY_NAME = Bytes.toBytes("q");

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setStrings(EndpointLoader.FACTORY_CLASSES_KEY,
        LongAggregator.Factory.class.getName());

    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCall() throws Exception {
    final StringBytes TABLE_NAME = new StringBytes("testCall");
    // Create the table
    HTableInterface table = TEST_UTIL.createTable(TABLE_NAME, FAMILY_NAME);

    final byte[] PREFIX = new byte[] { 'f', 'b' };

    // Put some values
    for (int i = 1; i <= 10; i++) {
      table.put(new Put(Bytes.toBytes("row" + i)).add(FAMILY_NAME,
          QUALITY_NAME, Bytes.add(PREFIX, Bytes.toBytes((long) i))));
    }

    // Calling endpoints.
    IEndpointClient cp = (IEndpointClient) table;
    Map<HRegionInfo, Long> results =
        cp.coprocessorEndpoint(ILongAggregator.class, null, null,
            new Caller<ILongAggregator, Long>() {
              @Override
              public Long call(ILongAggregator client) throws IOException {
                return client.sum(FAMILY_NAME, null, PREFIX.length);
              }
            });

    // Aggregates results from all regions
    long sum = 0;
    for (Long res : results.values()) {
      sum += res;
    }

    // Check the final results
    Assert.assertEquals("sum", 55, sum);

    results =
        cp.coprocessorEndpoint(ILongAggregator.class, null, null,
            new Caller<ILongAggregator, Long>() {
              @Override
              public Long call(ILongAggregator client) throws IOException {
                return client.max(FAMILY_NAME, null, PREFIX.length);
              }
            });

    // Aggregates results from all regions
    long max = Long.MIN_VALUE;
    for (Long res : results.values()) {
      max = Math.max(max, res);
    }

    // Check the final results
    Assert.assertEquals("max", 10, max);

    results =
        cp.coprocessorEndpoint(ILongAggregator.class, null, null,
            new Caller<ILongAggregator, Long>() {
              @Override
              public Long call(ILongAggregator client) throws IOException {
                return client.min(FAMILY_NAME, null, PREFIX.length);
              }
            });

    // Aggregates results from all regions
    long min = Long.MAX_VALUE;
    for (Long res : results.values()) {
      min = Math.min(min, res);
    }

    // Check the final results
    Assert.assertEquals("min", 1, min);
  }

  @Test
  public void testCallWithRange() throws Exception {
    final StringBytes TABLE_NAME = new StringBytes("testCallWithRange");
    final byte[] PREFIX = Bytes.toBytes("fb");

    // Create the table
    try (HTableInterface table = TEST_UTIL.createTable(TABLE_NAME, FAMILY_NAME)) {
      // Put some values
      for (int i = 1; i <= 9; i++) {
        table.put(new Put(Bytes.toBytes("row" + i)).add(FAMILY_NAME,
            QUALITY_NAME, Bytes.add(PREFIX, Bytes.toBytes((long) i))));
      }

      // Calling endpoints.
      IEndpointClient cp = (IEndpointClient) table;
      Map<HRegionInfo, Long> results =
          cp.coprocessorEndpoint(ILongAggregator.class,
              Bytes.toBytes("row" + 2), Bytes.toBytes("row" + 8),
              new Caller<ILongAggregator, Long>() {
                @Override
                public Long call(ILongAggregator client) throws IOException {
                  return client.sum(FAMILY_NAME, null, PREFIX.length);
                }
              });

      // Aggregates results from all regions
      long sum = 0;
      for (Long res : results.values()) {
        sum += res;
      }

      // Check the final results
      Assert.assertEquals("sum", 27, sum);

      results = cp.coprocessorEndpoint(ILongAggregator.class,
          Bytes.toBytes("row" + 2), Bytes.toBytes("row" + 8),
          new Caller<ILongAggregator, Long>() {
            @Override
            public Long call(ILongAggregator client) throws IOException {
              return client.max(FAMILY_NAME, null, PREFIX.length);
            }
          });

      // Aggregates results from all regions
      long max = Long.MIN_VALUE;
      for (Long res : results.values()) {
        max = Math.max(max, res);
      }

      // Check the final results
      Assert.assertEquals("max", 7, max);

      results = cp.coprocessorEndpoint(ILongAggregator.class,
          Bytes.toBytes("row" + 2), Bytes.toBytes("row" + 8),
          new Caller<ILongAggregator, Long>() {
            @Override
            public Long call(ILongAggregator client) throws IOException {
              return client.min(FAMILY_NAME, null, PREFIX.length);
            }
          });

      // Aggregates results from all regions
      long min = Long.MAX_VALUE;
      for (Long res : results.values()) {
        min = Math.min(min, res);
      }

      // Check the final results
      Assert.assertEquals("min", 2, min);
    }
  }
}
