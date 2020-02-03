/**
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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestScannerFromBucketCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestScannerFromBucketCache.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestScannerFromBucketCache.class);
  @Rule
  public TestName name = new TestName();

  HRegion region = null;
  private HBaseTestingUtility test_util;
  private Configuration conf;
  private final int MAX_VERSIONS = 2;
  byte[] val = new byte[512 * 1024];

  // Test names
  private TableName tableName;

  private void setUp(boolean useBucketCache) throws IOException {
    test_util = HBaseTestingUtility.createLocalHTU();
    conf = test_util.getConfiguration();
    if (useBucketCache) {
      conf.setInt("hbase.bucketcache.size", 400);
      conf.setStrings(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
      conf.setInt("hbase.bucketcache.writer.threads", 10);
      conf.setFloat("hfile.block.cache.size", 0.2f);
      conf.setFloat("hbase.regionserver.global.memstore.size", 0.1f);
    }
    tableName = TableName.valueOf(name.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManagerTestHelper.reset();
    LOG.info("Cleaning test directory: " + test_util.getDataTestDir());
    test_util.cleanupTestDir();
  }

  String getName() {
    return name.getMethodName();
  }

  @Test
  public void testBasicScanWithLRUCache() throws IOException {
    setUp(false);
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("lrucache");

    long ts1 = 1; // System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, conf, test_util, fam1);
    try {
      List<Cell> expected = insertData(row1, qf1, qf2, fam1, ts1, ts2, ts3, false);

      List<Cell> actual = performScan(row1, fam1);
      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertFalse(actual.get(i) instanceof ByteBufferKeyValue);
        assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }
      // do the scan again and verify. This time it should be from the lru cache
      actual = performScan(row1, fam1);
      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertFalse(actual.get(i) instanceof ByteBufferKeyValue);
        assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testBasicScanWithOffheapBucketCache() throws IOException {
    setUp(true);
    byte[] row1 = Bytes.toBytes("row1offheap");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("famoffheap");

    long ts1 = 1; // System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, conf, test_util, fam1);
    try {
      List<Cell> expected = insertData(row1, qf1, qf2, fam1, ts1, ts2, ts3, false);

      List<Cell> actual = performScan(row1, fam1);
      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertFalse(actual.get(i) instanceof ByteBufferKeyValue);
        assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }
      // Wait for the bucket cache threads to move the data to offheap
      Thread.sleep(500);
      // do the scan again and verify. This time it should be from the bucket cache in offheap mode
      actual = performScan(row1, fam1);
      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertTrue(actual.get(i) instanceof ByteBufferKeyValue);
        assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }

    } catch (InterruptedException e) {
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testBasicScanWithOffheapBucketCacheWithMBB() throws IOException {
    setUp(true);
    byte[] row1 = Bytes.toBytes("row1offheap");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("famoffheap");

    long ts1 = 1; // System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, conf, test_util, fam1);
    try {
      List<Cell> expected = insertData(row1, qf1, qf2, fam1, ts1, ts2, ts3, true);

      List<Cell> actual = performScan(row1, fam1);
      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertFalse(actual.get(i) instanceof ByteBufferKeyValue);
        assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }
      // Wait for the bucket cache threads to move the data to offheap
      Thread.sleep(500);
      // do the scan again and verify. This time it should be from the bucket cache in offheap mode
      // but one of the cell will be copied due to the asSubByteBuff call
      Scan scan = new Scan().withStartRow(row1).addFamily(fam1).readVersions(10);
      actual = new ArrayList<>();
      InternalScanner scanner = region.getScanner(scan);

      boolean hasNext = scanner.next(actual);
      assertEquals(false, hasNext);
      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        if (i != 5) {
          // the last cell fetched will be of type shareable but not offheap because
          // the MBB is copied to form a single cell
          assertTrue(actual.get(i) instanceof ByteBufferKeyValue);
        }
      }

    } catch (InterruptedException e) {
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  private List<Cell> insertData(byte[] row1, byte[] qf1, byte[] qf2, byte[] fam1, long ts1,
      long ts2, long ts3, boolean withVal) throws IOException {
    // Putting data in Region
    Put put = null;
    KeyValue kv13 = null;
    KeyValue kv12 = null;
    KeyValue kv11 = null;

    KeyValue kv23 = null;
    KeyValue kv22 = null;
    KeyValue kv21 = null;
    if (!withVal) {
      kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
      kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
      kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);

      kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
      kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
      kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);
    } else {
      kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, val);
      kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, val);
      kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, val);

      kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, val);
      kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, val);
      kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, val);
    }

    put = new Put(row1);
    put.add(kv13);
    put.add(kv12);
    put.add(kv11);
    put.add(kv23);
    put.add(kv22);
    put.add(kv21);
    region.put(put);
    region.flush(true);
    HStore store = region.getStore(fam1);
    while (store.getStorefilesCount() <= 0) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
      }
    }

    // Expected
    List<Cell> expected = new ArrayList<>();
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv23);
    expected.add(kv22);
    return expected;
  }

  private List<Cell> performScan(byte[] row1, byte[] fam1) throws IOException {
    Scan scan = new Scan().withStartRow(row1).addFamily(fam1).readVersions(MAX_VERSIONS);
    List<Cell> actual = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);

    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);
    return actual;
  }

  private static HRegion initHRegion(TableName tableName, String callingMethod, Configuration conf,
      HBaseTestingUtility test_util, byte[]... families) throws IOException {
    return initHRegion(tableName, null, null, callingMethod, conf, test_util, false, families);
  }

  private static HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
      String callingMethod, Configuration conf, HBaseTestingUtility testUtil, boolean isReadOnly,
      byte[]... families) throws IOException {
    RegionInfo regionInfo =
        RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(stopKey).build();
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setReadOnly(isReadOnly).setDurability(Durability.SYNC_WAL);
    for (byte[] family : families) {
      builder.setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(family).setMaxVersions(Integer.MAX_VALUE)
              .build());
    }
    return HBaseTestingUtility
        .createRegionAndWAL(regionInfo, testUtil.getDataTestDir(callingMethod), conf,
            builder.build(), BlockCacheFactory.createBlockCache(conf));
  }
}
