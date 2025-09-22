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
package org.apache.hadoop.hbase.tool;

import static org.apache.hadoop.hbase.HConstants.HFILE_BLOCK_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_ACTIVATE_MIN_HFILES_KEY;
import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_SIZE_KEY;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RowCache;
import org.apache.hadoop.hbase.regionserver.RowCacheKey;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MiscTests.class, MediumTests.class })
public class TestBulkLoadHFilesRowCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadHFilesRowCache.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();;
  private static Admin admin;

  final static int NUM_CFS = 2;
  final static byte[] QUAL = Bytes.toBytes("qual");
  final static int ROWCOUNT = 10;

  private TableName tableName;
  private Table table;
  private HRegion region;

  @Rule
  public TestName testName = new TestName();

  static String family(int i) {
    return String.format("family_%04d", i);
  }

  public static void buildHFiles(FileSystem fs, Path dir) throws IOException {
    byte[] val = "value".getBytes();
    for (int i = 0; i < NUM_CFS; i++) {
      Path testIn = new Path(dir, family(i));

      TestHRegionServerBulkLoad.createHFile(fs, new Path(testIn, "hfile_" + i),
        Bytes.toBytes(family(i)), QUAL, val, ROWCOUNT);
    }
  }

  private TableDescriptor createTableDesc(TableName name) {
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(name).setRowCacheEnabled(true);
    IntStream.range(0, NUM_CFS).mapToObj(i -> ColumnFamilyDescriptorBuilder.of(family(i)))
      .forEachOrdered(builder::setColumnFamily);
    return builder.build();
  }

  private Path buildBulkFiles(TableName table) throws Exception {
    Path dir = TEST_UTIL.getDataTestDirOnTestFS(table.getNameAsString());
    Path bulk1 = new Path(dir, table.getNameAsString());
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    buildHFiles(fs, bulk1);
    return bulk1;
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    // Enable row cache but reduce the block cache size to fit in 80% of the heap
    conf.setFloat(ROW_CACHE_SIZE_KEY, 0.01f);
    conf.setFloat(HFILE_BLOCK_CACHE_SIZE_KEY, 0.38f);

    // To test simply, regardless of the number of HFiles
    conf.setInt(ROW_CACHE_ACTIVATE_MIN_HFILES_KEY, 0);
    TEST_UTIL.startMiniCluster(1);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    tableName = TableName.valueOf(testName.getMethodName());
    admin.createTable(createTableDesc(tableName));
    table = TEST_UTIL.getConnection().getTable(tableName);
    region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();
  }

  @After
  public void after() throws Exception {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  @Test
  public void testRowCache() throws Exception {
    RowCache rowCache = TEST_UTIL.getHBaseCluster().getRegionServer(0).getRSRpcServices()
      .getRowCacheService().getRowCache();

    // Put a row to populate a row cache
    Put put = new Put("row".getBytes());
    put.addColumn(family(0).getBytes(), "q1".getBytes(), "value".getBytes());
    table.put(put);

    // Ensure the region has a row cache
    Get get = new Get("row".getBytes());
    table.get(get);
    RowCacheKey keyPrev = new RowCacheKey(region, get.getRow());
    assertNotNull(rowCache.getBlock(keyPrev, true));

    // bulkload
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(conf);
    Path dir = buildBulkFiles(tableName);
    loader.bulkLoad(tableName, dir);

    // Ensure the row cache is removed after bulkload
    RowCacheKey keyCur = new RowCacheKey(region, get.getRow());
    assertNull(rowCache.getBlock(keyCur, true));
    // Ensure the row cache for keyPrev still exists, but it is not used any more.
    assertNotNull(rowCache.getBlock(keyPrev, true));
  }
}
