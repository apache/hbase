/*
 * Copyright 2011 The Apache Software Foundation
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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.StoreMetricType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test metrics incremented on region server operations.
 */
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

  private final HBaseTestingUtility testUtil =
      new HBaseTestingUtility();
  private final Configuration conf = testUtil.getConfiguration();

  private Map<String, Long> startingMetrics;

  private final int META_AND_ROOT = 2;

  private final int NUM_ROWS = 10000;

  @Before
  public void setUp() throws Exception {
    SchemaMetrics.setUseTableNameInTest(true);
    startingMetrics = SchemaMetrics.getMetricsSnapshot();
    testUtil.startMiniCluster();
  }

  @After
  public void tearDown() throws IOException {
    testUtil.shutdownMiniCluster();
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

    testUtil.createRandomTable(TABLE_NAME, Arrays.asList(FAMILIES), MAX_VERSIONS, NUM_COLS_PER_ROW,
        NUM_FLUSHES, NUM_REGIONS, 1000);

    final HRegionServer rs =
        testUtil.getMiniHBaseCluster().getRegionServer(0);

    assertEquals(NUM_REGIONS + META_AND_ROOT, rs.getOnlineRegions().size());

    rs.doMetrics();
    for (HRegion r : rs.getOnlineRegions()) {
      for (Map.Entry<byte[], Store> storeEntry : r.getStores().entrySet()) {
        LOG.info("For region " + r.getRegionNameAsString() + ", CF " +
            Bytes.toStringBinary(storeEntry.getKey()) + " found store files " +
            ": " + storeEntry.getValue().getStorefiles());
      }
    }

    assertStoreMetricEquals(NUM_FLUSHES * NUM_REGIONS * FAMILIES.length
        + 1 /*colfamily historian in META*/
        + 1 /*colfamily info in META*/
        + 1 /*ROOT */,
        ALL_METRICS, StoreMetricType.STORE_FILE_COUNT);

    for (String cf : FAMILIES) {
      SchemaMetrics schemaMetrics = SchemaMetrics.getInstance(TABLE_NAME, cf);
      assertStoreMetricEquals(NUM_FLUSHES * NUM_REGIONS, schemaMetrics,
          StoreMetricType.STORE_FILE_COUNT);
    }

    // ensure that the max value is also maintained
    final String storeMetricName = ALL_METRICS
        .getStoreMetricNameMax(StoreMetricType.STORE_FILE_COUNT);
    assertEquals("Invalid value for store metric " + storeMetricName,
        NUM_FLUSHES, HRegion.getNumericMetric(storeMetricName));
  }


  private void assertSizeMetric(String table, String[] cfs, int[] metrics) {
    // we have getsize & nextsize for each column family
    assertEquals(cfs.length * 2, metrics.length);

    for (int i = 0; i < cfs.length; ++i) {
      String prefix = SchemaMetrics.generateSchemaMetricsPrefix(table, cfs[i]);
      String getMetric = prefix + HRegion.METRIC_GETSIZE;
      String nextMetric = prefix + HRegion.METRIC_NEXTSIZE;

      // verify getsize and nextsize matches
      int getSize = HRegion.numericMetrics.containsKey(getMetric) ?
          HRegion.numericMetrics.get(getMetric).intValue() : 0;
      int nextSize = HRegion.numericMetrics.containsKey(nextMetric) ?
          HRegion.numericMetrics.get(nextMetric).intValue() : 0;

      assertEquals(metrics[i], getSize);
      assertEquals(metrics[cfs.length + i], nextSize);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testGetNextSize() throws IOException, InterruptedException {
    String rowName = "row1";
    byte[] ROW = Bytes.toBytes(rowName);
    String tableName = "SizeMetricTest";
    byte[] TABLE = Bytes.toBytes(tableName);
    String cf1Name = "cf1";
    String cf2Name = "cf2";
    String[] cfs = new String[] {cf1Name, cf2Name};
    byte[] CF1 = Bytes.toBytes(cf1Name);
    byte[] CF2 = Bytes.toBytes(cf2Name);

    long ts = 1234;
    HTable hTable = testUtil.createTable(TABLE, new byte[][]{CF1, CF2});

    Put p = new Put(ROW);
    p.add(CF1, CF1, ts, CF1);
    p.add(CF2, CF2, ts, CF2);
    hTable.put(p);

    KeyValue kv1 = new KeyValue(ROW, CF1, CF1, ts, CF1);
    KeyValue kv2 = new KeyValue(ROW, CF2, CF2, ts, CF2);
    int kvLength = kv1.getLength();
    assertEquals(kvLength, kv2.getLength());

    // only cf1.getsize is set on Get
    hTable.get(new Get(ROW).addFamily(CF1));
    assertSizeMetric(tableName, cfs, new int[] {kvLength, 0, 0, 0});

    // only cf2.getsize is set on Get
    hTable.get(new Get(ROW).addFamily(CF2));
    assertSizeMetric(tableName, cfs, new int[] {kvLength, kvLength, 0, 0});

    // only cf2.nextsize is set
    for (Result res : hTable.getScanner(CF2)) {
    }

    assertSizeMetric(tableName, cfs,
        new int[] {kvLength, kvLength, 0, kvLength});

    // only cf2.nextsize is set
    for (Result res : hTable.getScanner(CF1)) {
    }

    assertSizeMetric(tableName, cfs,
        new int[] {kvLength, kvLength, kvLength, kvLength});

    // getsize/nextsize should not be set on flush or compaction
    for (HRegion hr : testUtil.getMiniHBaseCluster().getRegions(TABLE)) {
      hr.flushcache();
      hr.compactStores();
    }
    assertSizeMetric(tableName, cfs,
        new int[] {kvLength, kvLength, kvLength, kvLength});
  }

  @Test
  public void testEncodingInCache() throws Exception {
    HTable t = null;
    try {
      HColumnDescriptor hcd = new HColumnDescriptor(HTestConst.DEFAULT_CF_BYTES)
          .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
      testUtil.createTable(HTestConst.DEFAULT_TABLE_BYTES, new HColumnDescriptor[]{hcd});
      t = new HTable(conf, HTestConst.DEFAULT_TABLE_BYTES);

      // Write some test data
      for (int iRow = 0; iRow < NUM_ROWS; ++iRow) {
        String rowStr = "row" + iRow;
        byte[] rowBytes = Bytes.toBytes(rowStr);
        Put p = new Put(rowBytes);
        for (int iQual = 0; iQual < NUM_COLS_PER_ROW; ++iQual) {
          String qualStr = "q" + iQual;
          byte[] qualBytes = Bytes.toBytes(qualStr);
          String valueStr = "v" + Integer.toString(iRow + iQual);
          byte[] valueBytes = Bytes.toBytes(valueStr);
          p.add(HTestConst.DEFAULT_CF_BYTES, qualBytes, valueBytes);
        }
        t.put(p);
      }
      HBaseAdmin adm = new HBaseAdmin(conf);
      adm.flush(HTestConst.DEFAULT_TABLE_STR);
      adm.close();

      LOG.info("Clearing cache and reading");
      testUtil.getBlockCache().clearCache();
      startingMetrics = SchemaMetrics.getMetricsSnapshot();
      // Read all data to bring it into cache.
      for (int iRow = 0; iRow < NUM_ROWS; ++iRow) {
        Get g = new Get(Bytes.toBytes("row" + iRow));
        g.addFamily(HTestConst.DEFAULT_CF_BYTES);
        t.get(g);
      }

      // Check metrics
      Map<String, Long> m = SchemaMetrics.diffMetrics(this.startingMetrics,
          SchemaMetrics.getMetricsSnapshot());
      LOG.info("Metrics after reading:\n" + SchemaMetrics.formatMetrics(m));
      long dataBlockEncodedSize = SchemaMetrics.getLong(m,
          SchemaMetrics.ALL_SCHEMA_METRICS.getBlockMetricName(
          BlockType.BlockCategory.DATA, false, SchemaMetrics.BlockMetricType.CACHE_SIZE));
      long dataBlockUnencodedSize = SchemaMetrics.getLong(m,
          SchemaMetrics.ALL_SCHEMA_METRICS.getBlockMetricName(BlockType.BlockCategory.DATA, false,
              SchemaMetrics.BlockMetricType.UNENCODED_CACHE_SIZE));
      LOG.info("Data block encoded size in cache: " + dataBlockEncodedSize);
      LOG.info("Data block unencoded size in cache: " + dataBlockEncodedSize);
      assertTrue(dataBlockEncodedSize * 2 < dataBlockUnencodedSize);
    } finally {
      if (t != null) {
        t.close();
      }
      testUtil.dropDefaultTable();
    }
  }
  
  @Test
  public void testCompactionWriteMetric() throws Exception { 
    long ts = 2342;
    String cfName = "cf1";
    byte[] CF = Bytes.toBytes(cfName);
    String tableName = "CompactionWriteSize";
    byte[] TABLE = Bytes.toBytes(tableName);
    HTable hTable = testUtil.createTable(TABLE, CF);

    final String storeMetricName = ALL_METRICS.getStoreMetricName(
        StoreMetricType.COMPACTION_WRITE_SIZE);
    String storeMetricFullName = 
      SchemaMetrics.generateSchemaMetricsPrefix(tableName, cfName) 
      + storeMetricName;
    long startValue = HRegion.getNumericPersistentMetric(storeMetricFullName);

    int compactionThreshold = conf.getInt("hbase.hstore.compaction.min", 
        HConstants.DEFAULT_MIN_FILES_TO_COMPACT);
    for (int i=0; i<=compactionThreshold; i++) {
      String rowName = "row" + i;
      byte[] ROW = Bytes.toBytes(rowName);
      Put p = new Put(ROW);
      p.add(CF, CF, ts, ROW);
      hTable.put(p);
      for (HRegion hr : testUtil.getMiniHBaseCluster().getRegions(TABLE)) {
        hr.flushcache();
      }
    }

    for (HRegion hr : testUtil.getMiniHBaseCluster().getRegions(TABLE)) {
      hr.flushcache();
      hr.compactStores();
    }

    long compactionWriteSizeAfterCompaction = 
      HRegion.getNumericPersistentMetric(storeMetricFullName);
    Assert.assertTrue(startValue < compactionWriteSizeAfterCompaction);
    
    for (int i=0; i<=compactionThreshold; i++) {
      String rowName = "row" + i;
      byte[] ROW = Bytes.toBytes(rowName);
      Delete del = new Delete(ROW);
      hTable.delete(del);
      for (HRegion hr : testUtil.getMiniHBaseCluster().getRegions(TABLE)) {
        hr.flushcache();
      }
    }
    
    for (HRegion hr : testUtil.getMiniHBaseCluster().getRegions(TABLE)) {
        hr.flushcache();
        hr.compactStores();
      }

    long compactionWriteSizeAfterCompactionAfterDelete = 
      HRegion.getNumericPersistentMetric(storeMetricFullName);
    Assert.assertTrue(compactionWriteSizeAfterCompactionAfterDelete 
                    == compactionWriteSizeAfterCompaction);
  }
}
