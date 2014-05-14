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
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
@SuppressWarnings("deprecation")
public class TestSkipCompactionHook {

  private static byte[] TABLE = Bytes.toBytes("TestCompactionHook");
  private static byte[] FAMILY = Bytes.toBytes("family");
  private static byte[] START_KEY = Bytes.toBytes("aaa");
  private static byte[] END_KEY = Bytes.toBytes("zzz");
  private static int BLOCK_SIZE = 70;

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
            "org.apache.hadoop.hbase.regionserver.compactionhook.SkipCompactionHook");
  }

  @Test
  public void testSkipHook () throws Exception {
    HRegion r = HBaseTestCase.createNewHRegion(TESTTABLEDESC, START_KEY,
        END_KEY, TEST_UTIL.getConfiguration());
    Put[] puts = new Put[25];
    // put some strings
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
    //
    Scan scan = new Scan();
    InternalScanner s = r.getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (s.next(results))
      ;
    s.close();
    // check if we got 24 results back, since "abc" should be skipped.
    Assert.assertEquals(24, results.size());
  }

}
