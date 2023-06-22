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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * A UT to make sure that everything is fine when we fail to load bloom filter.
 * <p>
 * See HBASE-27936 for more details.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestBloomFilterFaulty {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBloomFilterFaulty.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final byte[] FAMILY = Bytes.toBytes("family");

  private static final byte[] QUAL = Bytes.toBytes("qualifier");

  private static final TableDescriptor TD =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("test"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
        .setBloomFilterType(BloomType.ROWPREFIX_FIXED_LENGTH)
        .setConfiguration("RowPrefixBloomFilter.prefix_length", "2").build())
      .build();

  private static final RegionInfo RI = RegionInfoBuilder.newBuilder(TD.getTableName()).build();

  @AfterClass
  public static void tearDownAfterClass() {
    UTIL.cleanupTestDir();
  }

  private HRegion region;

  @Rule
  public final TestName name = new TestName();

  private void generateHFiles() throws IOException {
    for (int i = 0; i < 4; i++) {
      long ts = EnvironmentEdgeManager.currentTime();
      for (int j = 0; j < 5; j++) {
        byte[] row = Bytes.toBytes(j);
        region.put(new Put(row).addColumn(FAMILY, QUAL, ts, Bytes.toBytes(i * 10 + j)));
        region.delete(new Delete(row).addFamilyVersion(FAMILY, ts));
      }

      for (int j = 5; j < 10; j++) {
        byte[] row = Bytes.toBytes(j);
        region.put(new Put(row).addColumn(FAMILY, QUAL, ts + 1, Bytes.toBytes(i * 10 + j)));
      }

      FlushResult result = region.flush(true);
      if (
        result.getResult() == FlushResult.Result.CANNOT_FLUSH
          || result.getResult() == FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY
      ) {
        throw new IOException("Can not flush region, flush result: " + result);
      }
    }
  }

  @Before
  public void setUp() throws IOException {
    Path rootDir = UTIL.getDataTestDir(name.getMethodName());
    // generate some hfiles so we can have StoreFileReader which has bloomfilters
    region = HBaseTestingUtil.createRegionAndWAL(RI, rootDir, UTIL.getConfiguration(), TD);
    generateHFiles();
    HStore store = region.getStore(FAMILY);
    for (HStoreFile storefile : store.getStorefiles()) {
      storefile.initReader();
      StoreFileReader reader = storefile.getReader();
      // make sure we load bloom filters correctly
      assertNotNull(reader.generalBloomFilter);
      assertNotNull(reader.deleteFamilyBloomFilter);
    }
  }

  @After
  public void tearDown() throws IOException {
    if (region != null) {
      HBaseTestingUtil.closeRegionAndWAL(region);
    }
  }

  private void setFaulty(BlockType type) {
    HStore store = region.getStore(FAMILY);
    for (HStoreFile storefile : store.getStorefiles()) {
      storefile.getReader().setBloomFilterFaulty(type);
    }
  }

  private void testGet() throws IOException {
    for (int i = 0; i < 5; i++) {
      assertTrue(region.get(new Get(Bytes.toBytes(i))).isEmpty());
    }
    for (int i = 5; i < 10; i++) {
      assertEquals(30 + i,
        Bytes.toInt(region.get(new Get(Bytes.toBytes(i))).getValue(FAMILY, QUAL)));
    }
  }

  private void testStreamScan() throws IOException {
    try (RegionAsTable table = new RegionAsTable(region);
      ResultScanner scanner = table.getScanner(new Scan().setReadType(ReadType.STREAM))) {
      for (int i = 5; i < 10; i++) {
        Result result = scanner.next();
        assertEquals(i, Bytes.toInt(result.getRow()));
        assertEquals(30 + i, Bytes.toInt(result.getValue(FAMILY, QUAL)));
      }
      assertNull(scanner.next());
    }
  }

  private void testRegion() throws IOException {
    // normal read
    testGet();
    // scan with stream reader
    testStreamScan();
    // major compact
    region.compact(true);
    // test read and scan again
    testGet();
    testStreamScan();
  }

  @Test
  public void testNoGeneralBloomFilter() throws IOException {
    setFaulty(BlockType.GENERAL_BLOOM_META);
    testRegion();
  }

  @Test
  public void testNoDeleteFamilyBloomFilter() throws IOException {
    setFaulty(BlockType.DELETE_FAMILY_BLOOM_META);
    testRegion();
  }

  @Test
  public void testNoAnyBloomFilter() throws IOException {
    setFaulty(BlockType.GENERAL_BLOOM_META);
    setFaulty(BlockType.DELETE_FAMILY_BLOOM_META);
    testRegion();
  }
}
