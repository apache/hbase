/*
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
package org.apache.hadoop.hbase.io.compress.zstd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.DictionaryCache;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestZstdDictionarySplitMerge {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZstdDictionarySplitMerge.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @BeforeClass
  public static void setUp() throws Exception {
    // NOTE: Don't put configuration settings in global site schema. We are testing if per
    // CF or per table schema settings are applied correctly.
    conf = TEST_UTIL.getConfiguration();
    conf.set(Compression.ZSTD_CODEC_CLASS_KEY, ZstdCodec.class.getCanonicalName());
    Compression.Algorithm.ZSTD.reload(conf);
    conf.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 1000);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    // Create the table

    final TableName tableName = TableName.valueOf("TestZstdDictionarySplitMerge");
    final byte[] cfName = Bytes.toBytes("info");
    final String dictionaryPath = DictionaryCache.RESOURCE_SCHEME + "zstd.test.dict";
    final TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cfName)
        .setCompressionType(Compression.Algorithm.ZSTD)
        .setConfiguration(ZstdCodec.ZSTD_DICTIONARY_KEY, dictionaryPath).build())
      .build();
    final Admin admin = TEST_UTIL.getAdmin();
    admin.createTable(td, new byte[][] { Bytes.toBytes(1) });
    TEST_UTIL.waitTableAvailable(tableName);

    // Load some data

    Table t = ConnectionFactory.createConnection(conf).getTable(tableName);
    TEST_UTIL.loadNumericRows(t, cfName, 0, 100_000);
    admin.flush(tableName);
    assertTrue("Dictionary was not loaded", DictionaryCache.contains(dictionaryPath));
    TEST_UTIL.verifyNumericRows(t, cfName, 0, 100_000, 0);

    // Test split procedure

    admin.split(tableName, Bytes.toBytes(50_000));
    TEST_UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getMiniHBaseCluster().getRegions(tableName).size() == 3;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Split has not finished yet";
      }
    });
    TEST_UTIL.waitUntilNoRegionsInTransition();
    TEST_UTIL.verifyNumericRows(t, cfName, 0, 100_000, 0);

    // Test merge procedure

    RegionInfo regionA = null;
    RegionInfo regionB = null;
    for (RegionInfo region : admin.getRegions(tableName)) {
      if (region.getStartKey().length == 0) {
        regionA = region;
      } else if (Bytes.equals(region.getStartKey(), Bytes.toBytes(1))) {
        regionB = region;
      }
    }
    assertNotNull(regionA);
    assertNotNull(regionB);
    admin
      .mergeRegionsAsync(new byte[][] { regionA.getRegionName(), regionB.getRegionName() }, false)
      .get(30, TimeUnit.SECONDS);
    assertEquals(2, admin.getRegions(tableName).size());
    ServerName expected = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
    assertEquals(expected, TEST_UTIL.getConnection().getRegionLocator(tableName)
      .getRegionLocation(Bytes.toBytes(1), true).getServerName());
    try (AsyncConnection asyncConn = ConnectionFactory.createAsyncConnection(conf).get()) {
      assertEquals(expected, asyncConn.getRegionLocator(tableName)
        .getRegionLocation(Bytes.toBytes(1), true).get().getServerName());
    }
    TEST_UTIL.verifyNumericRows(t, cfName, 0, 100_000, 0);
  }

}
