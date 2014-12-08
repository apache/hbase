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
package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@SuppressWarnings("deprecation")
@Category(SmallTests.class)
public class TestBlocksScanned extends HBaseTestCase {
  private static byte [] FAMILY = Bytes.toBytes("family");
  private static byte [] COL = Bytes.toBytes("col");
  private static byte [] START_KEY = Bytes.toBytes("aaa");
  private static byte [] END_KEY = Bytes.toBytes("zzz");
  private static int BLOCK_SIZE = 70;

  private static HBaseTestingUtility TEST_UTIL = null;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    TEST_UTIL = new HBaseTestingUtility();
  }

  @Test
  public void testBlocksScanned() throws Exception {
    byte [] tableName = Bytes.toBytes("TestBlocksScanned");
    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

    table.addFamily(
        new HColumnDescriptor(FAMILY)
        .setMaxVersions(10)
        .setBlockCacheEnabled(true)
        .setBlocksize(BLOCK_SIZE)
        .setCompressionType(Compression.Algorithm.NONE)
        );
    _testBlocksScanned(table);
  }

  @Test
  public void testBlocksScannedWithEncoding() throws Exception {
    byte [] tableName = Bytes.toBytes("TestBlocksScannedWithEncoding");
    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

    table.addFamily(
        new HColumnDescriptor(FAMILY)
        .setMaxVersions(10)
        .setBlockCacheEnabled(true)
        .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
        .setBlocksize(BLOCK_SIZE)
        .setCompressionType(Compression.Algorithm.NONE)
        );
    _testBlocksScanned(table);
  }

  private void _testBlocksScanned(HTableDescriptor table) throws Exception {
    HRegion r = createNewHRegion(table, START_KEY, END_KEY,
        TEST_UTIL.getConfiguration());
    addContent(r, FAMILY, COL);
    r.flushcache();

    CacheStats stats = new CacheConfig(TEST_UTIL.getConfiguration()).getBlockCache().getStats();
    long before = stats.getHitCount() + stats.getMissCount();
    // Do simple test of getting one row only first.
    Scan scan = new Scan(Bytes.toBytes("aaa"), Bytes.toBytes("aaz"));
    scan.addColumn(FAMILY, COL);
    scan.setMaxVersions(1);

    InternalScanner s = r.getScanner(scan);
    List<Cell> results = new ArrayList<Cell>();
    while (s.next(results));
    s.close();

    int expectResultSize = 'z' - 'a';
    assertEquals(expectResultSize, results.size());

    int kvPerBlock = (int) Math.ceil(BLOCK_SIZE / 
        (double) KeyValueUtil.ensureKeyValue(results.get(0)).getLength());
    Assert.assertEquals(2, kvPerBlock);

    long expectDataBlockRead = (long) Math.ceil(expectResultSize / (double) kvPerBlock);
    long expectIndexBlockRead = expectDataBlockRead;

    assertEquals(expectIndexBlockRead+expectDataBlockRead, stats.getHitCount() + stats.getMissCount() - before);
  }
}
