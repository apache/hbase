/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.
    StoreMetricType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test metrics incremented on region server operations.
 */
@Category(MediumTests.class)
public class TestRegionServerMetrics {

  private static final Log LOG =
      LogFactory.getLog(TestRegionServerMetrics.class.getName());

  private final static String TABLE_NAME =
      TestRegionServerMetrics.class.getSimpleName() + "Table";
  private String[] FAMILIES = new String[] { "cf1", "cf2", "anotherCF" };
  private static final int MAX_VERSIONS = 1;
  private static final int NUM_COLS_PER_ROW = 15;
  private static final int NUM_FLUSHES = 3;
  private static final int NUM_REGIONS = 4;

  private static final SchemaMetrics ALL_METRICS =
      SchemaMetrics.ALL_SCHEMA_METRICS;

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private Map<String, Long> startingMetrics;

  private final int META_AND_ROOT = 2;

  @Before
  public void setUp() throws Exception {
    SchemaMetrics.setUseTableNameInTest(true);
    startingMetrics = SchemaMetrics.getMetricsSnapshot();
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    SchemaMetrics.validateMetricChanges(startingMetrics);
  }

  private void assertStoreMetricEquals(long expected,
      SchemaMetrics schemaMetrics, StoreMetricType storeMetricType) {
    final String storeMetricName =
        schemaMetrics.getStoreMetricName(storeMetricType);
    Long startValue = startingMetrics.get(storeMetricName);
    assertEquals("Invalid value for store metric " + storeMetricName
        + " (type " + storeMetricType + ")", expected,
        HRegion.getNumericMetric(storeMetricName)
            - (startValue != null ? startValue : 0));
  }

  @Test
  public void testMultipleRegions() throws IOException, InterruptedException {

    TEST_UTIL.createRandomTable(
        TABLE_NAME,
        Arrays.asList(FAMILIES),
        MAX_VERSIONS, NUM_COLS_PER_ROW, NUM_FLUSHES, NUM_REGIONS, 1000);

    final HRegionServer rs =
        TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);

    assertEquals(NUM_REGIONS + META_AND_ROOT, rs.getOnlineRegions().size());

    rs.doMetrics();
    for (HRegion r : TEST_UTIL.getMiniHBaseCluster().getRegions(
        Bytes.toBytes(TABLE_NAME))) {
      for (Map.Entry<byte[], Store> storeEntry : r.getStores().entrySet()) {
        LOG.info("For region " + r.getRegionNameAsString() + ", CF " +
            Bytes.toStringBinary(storeEntry.getKey()) + " found store files " +
            ": " + storeEntry.getValue().getStorefiles());
      }
    }

    assertStoreMetricEquals(NUM_FLUSHES * NUM_REGIONS * FAMILIES.length
        + META_AND_ROOT, ALL_METRICS, StoreMetricType.STORE_FILE_COUNT);

    for (String cf : FAMILIES) {
      SchemaMetrics schemaMetrics = SchemaMetrics.getInstance(TABLE_NAME, cf);
      assertStoreMetricEquals(NUM_FLUSHES * NUM_REGIONS,
          schemaMetrics, StoreMetricType.STORE_FILE_COUNT);
    }
  }

}
