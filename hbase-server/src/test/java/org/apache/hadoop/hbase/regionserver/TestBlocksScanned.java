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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HTestConst.addContent;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@SuppressWarnings("deprecation")
@Category({ RegionServerTests.class, SmallTests.class })
public class TestBlocksScanned {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBlocksScanned.class);

  private static byte[] FAMILY = Bytes.toBytes("family");
  private static byte[] COL = Bytes.toBytes("col");
  private static byte[] START_KEY = Bytes.toBytes("aaa");
  private static byte[] END_KEY = Bytes.toBytes("zzz");
  private static int BLOCK_SIZE = 70;

  private static HBaseTestingUtil TEST_UTIL = null;
  private Configuration conf;
  private Path testDir;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf = TEST_UTIL.getConfiguration();
    testDir = TEST_UTIL.getDataTestDir("TestBlocksScanned");
  }

  @Test
  public void testBlocksScanned() throws Exception {
    byte[] tableName = Bytes.toBytes("TestBlocksScanned");
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(10)
          .setBlockCacheEnabled(true).setBlocksize(BLOCK_SIZE)
          .setCompressionType(Compression.Algorithm.NONE).build())
        .build();
    _testBlocksScanned(tableDescriptor);
  }

  @Test
  public void testBlocksScannedWithEncoding() throws Exception {
    byte[] tableName = Bytes.toBytes("TestBlocksScannedWithEncoding");
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(10)
          .setBlockCacheEnabled(true).setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
          .setBlocksize(BLOCK_SIZE).setCompressionType(Compression.Algorithm.NONE).build())
        .build();
    _testBlocksScanned(tableDescriptor);
  }

  private void _testBlocksScanned(TableDescriptor td) throws Exception {
    BlockCache blockCache = BlockCacheFactory.createBlockCache(conf);
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(td.getTableName()).setStartKey(START_KEY)
      .setEndKey(END_KEY).build();
    HRegion r = HBaseTestingUtil.createRegionAndWAL(regionInfo, testDir, conf, td, blockCache);
    addContent(r, FAMILY, COL);
    r.flush(true);

    CacheStats stats = blockCache.getStats();
    long before = stats.getHitCount() + stats.getMissCount();
    // Do simple test of getting one row only first.
    Scan scan = new Scan().withStartRow(Bytes.toBytes("aaa")).withStopRow(Bytes.toBytes("aaz"))
      .setReadType(Scan.ReadType.PREAD);
    scan.addColumn(FAMILY, COL);
    scan.readVersions(1);

    InternalScanner s = r.getScanner(scan);
    List<ExtendedCell> results = new ArrayList<>();
    while (s.next(results))
      ;
    s.close();

    int expectResultSize = 'z' - 'a';
    assertEquals(expectResultSize, results.size());

    int kvPerBlock = (int) Math
      .ceil(BLOCK_SIZE / (double) KeyValueUtil.ensureKeyValue(results.get(0)).getLength());
    assertEquals(2, kvPerBlock);

    long expectDataBlockRead = (long) Math.ceil(expectResultSize / (double) kvPerBlock);
    long expectIndexBlockRead = expectDataBlockRead;

    assertEquals(expectIndexBlockRead + expectDataBlockRead,
      stats.getHitCount() + stats.getMissCount() - before);
  }
}
