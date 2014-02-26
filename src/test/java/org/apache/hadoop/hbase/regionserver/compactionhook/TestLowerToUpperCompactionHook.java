/**
 * Copyright The Apache Software Foundation.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.hadoop.hbase.regionserver.compactionhook;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.StoreMetricType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestLowerToUpperCompactionHook {
  private static String TABLE_STRING = "TestCompactionHook";
  private static byte[] TABLE = Bytes.toBytes(TABLE_STRING);
  private static String FAMILY_STRING = "family";
  private static byte[] FAMILY = Bytes.toBytes(FAMILY_STRING);
  private static byte[] START_KEY = Bytes.toBytes("aaa");
  private static byte[] END_KEY = Bytes.toBytes("zzz");
  private static int BLOCK_SIZE = 70;
  private static final SchemaMetrics ALL_METRICS =
      SchemaMetrics.ALL_SCHEMA_METRICS;

  private static HBaseTestingUtility TEST_UTIL = null;
  private static HTableDescriptor TESTTABLEDESC = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    SchemaMetrics.setUseTableNameInTest(true);
    TEST_UTIL = new HBaseTestingUtility();
    TESTTABLEDESC = new HTableDescriptor(TABLE);

    TESTTABLEDESC.addFamily(new HColumnDescriptor(FAMILY).setMaxVersions(10)
        .setBlockCacheEnabled(true).setBlocksize(BLOCK_SIZE)
        .setCompressionType(Compression.Algorithm.NONE));
    TEST_UTIL
        .getConfiguration()
        .set(
            HConstants.COMPACTION_HOOK,
            "org.apache.hadoop.hbase.regionserver.compactionhook.LowerToUpperCompactionHook");
  }

  @Test
  public void testLowerToUpperHook() throws Exception {
    HRegion r = HBaseTestCase.createNewHRegion(TESTTABLEDESC, START_KEY,
        END_KEY, TEST_UTIL.getConfiguration());
    Put[] puts = new Put[25];

    String kvsTransformedFullName = SchemaMetrics.generateSchemaMetricsPrefix(
        TABLE_STRING, FAMILY_STRING)
        + ALL_METRICS
            .getStoreMetricName(StoreMetricType.STORE_COMPHOOK_KVS_TRANSFORMED);
    long startValueTransformedKVs = HRegion
        .getNumericMetric(kvsTransformedFullName);

    String bytesSavedFullName = SchemaMetrics.generateSchemaMetricsPrefix(
        TABLE_STRING, FAMILY_STRING)
        + ALL_METRICS
            .getStoreMetricName(StoreMetricType.STORE_COMPHOOK_BYTES_SAVED);

    String kvsErrorsFullName = SchemaMetrics.generateSchemaMetricsPrefix(
        TABLE_STRING, FAMILY_STRING)
        + ALL_METRICS
            .getStoreMetricName(StoreMetricType.STORE_COMPHOOK_KVS_ERRORS);
    long startValueKvsErrors = HRegion.getNumericMetric(kvsErrorsFullName);

    long startValueBytesSaved = HRegion.getNumericMetric(bytesSavedFullName);
    // put some lowercase strings
    for (int i = 0; i < 25; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      byte[] qualifier = Bytes.toBytes("qual" + i);
      byte[] value = Bytes.toBytes("ab" + (char) (i + 97));
      put.add(FAMILY, qualifier, value);
      puts[i] = put;
    }
    r.put(puts);
    // without calling this, the compaction will not happen
    r.flushcache();
    r.compactStores(true);

    long transformedKVsAfterCompaction = HRegion.getNumericPersistentMetric(kvsTransformedFullName);
    // for one of them we are throwing exception - so it will not be transformed
    Assert.assertEquals(24, transformedKVsAfterCompaction - startValueTransformedKVs);
    long bytesSavedAfterCompaction = HRegion.getNumericPersistentMetric(bytesSavedFullName);
    // kv with value abc should be skipped
    Assert.assertTrue((bytesSavedAfterCompaction - startValueBytesSaved) > 0);
    long kvsErrors = HRegion.getNumericPersistentMetric(kvsErrorsFullName);
    Assert.assertEquals(1, kvsErrors - startValueKvsErrors);

    Scan scan = new Scan();
    InternalScanner s = r.getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (s.next(results))
      ;
    s.close();
    // check if the values in results are in upper case
    // this should return true, we are flushing the HRegion, so we expect the
    // compaction to be done
    Assert.assertTrue(checkIfLowerCase(results));
    // check if we got 24 results back since one of them should be skipped
    Assert.assertEquals(24, results.size());
  }

  public boolean checkIfLowerCase(List<KeyValue> result) {
    for (KeyValue kv : result) {
      String currValue = new String(kv.getValue());
      String uppercase = currValue.toUpperCase();
      // we ignore "aba" because in that case we thew IllegalArgumentException
      // and value stayed unchanged
      if (!currValue.equals(uppercase) && !currValue.equals("aba")) {
        return false;
      }
    }
    return true;
  }

}
