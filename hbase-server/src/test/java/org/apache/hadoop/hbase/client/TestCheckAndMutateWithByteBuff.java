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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.util.Threads.sleep;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.DeallocateRewriteByteBuffAllocator;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestCheckAndMutateWithByteBuff {
  private static final Logger LOG = LoggerFactory.getLogger(TestCheckAndMutateWithByteBuff.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCheckAndMutateWithByteBuff.class);

  @Rule
  public TestName name = new TestName();

  private static final byte[] CF = Bytes.toBytes("CF");
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf.set(HConstants.REGION_IMPL, TestCheckAndMutateRegion.class.getName());
    conf.set(ByteBuffAllocator.BYTEBUFF_ALLOCATOR_CLASS,
      DeallocateRewriteByteBuffAllocator.class.getName());
    conf.setBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    conf.setInt(ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY, 1);
    conf.setInt(BlockCacheFactory.BUCKET_CACHE_WRITER_THREADS_KEY, 20);
    conf.setInt(ByteBuffAllocator.BUFFER_SIZE_KEY, 1024);
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 64);
    conf.setInt("hbase.client.retries.number", 1);
    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCheckAndMutateWithByteBuffNoEncode() throws Exception {
    testCheckAndMutateWithByteBuff(TableName.valueOf(name.getMethodName()), DataBlockEncoding.NONE);
  }

  @Test
  public void testCheckAndMutateWithByteBuffEncode() throws Exception {
    // Tests for HBASE-26777.
    // As most HBase.getRegion() calls have been factored out from HBase, you'd need to revert
    // both HBASE-26777, and the HBase.get() replacements from HBASE-26036 for this test to fail
    testCheckAndMutateWithByteBuff(TableName.valueOf(name.getMethodName()), DataBlockEncoding.FAST_DIFF);
  }

  private void testCheckAndMutateWithByteBuff(TableName tableName, DataBlockEncoding dbe) throws Exception {
    Table testTable = createTable(tableName, dbe);
    byte[] checkRow = Bytes.toBytes("checkRow");
    byte[] checkQualifier = Bytes.toBytes("cq");
    byte[] checkValue = Bytes.toBytes("checkValue");

    Put put = new Put(checkRow);
    put.addColumn(CF, checkQualifier, checkValue);
    testTable.put(put);
    admin.flush(testTable.getName());

    assertTrue(testTable.checkAndMutate(checkRow, CF).qualifier(checkQualifier).
      ifEquals(checkValue)
      .thenPut(new Put(checkRow).addColumn(CF, Bytes.toBytes("q1"),
        Bytes.toBytes("testValue"))));
  }

  private Table createTable(TableName tableName, DataBlockEncoding dbe)
    throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF)
        .setBlocksize(100)
        .setDataBlockEncoding(dbe)
        .build())
      .build();
    return TEST_UTIL.createTable(td, null);
  }

  /**
   * An override of HRegion to allow sleep after get(), waiting for the release of DBB
   */
  public static class TestCheckAndMutateRegion extends HRegion {
    public TestCheckAndMutateRegion(Path tableDir, WAL log, FileSystem fs, Configuration confParam,
      RegionInfo info, TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, confParam, info, htd, rsServices);
    }

    public TestCheckAndMutateRegion(HRegionFileSystem fs, WAL wal, Configuration confParam,
      TableDescriptor htd, RegionServerServices rsServices) {
      super(fs, wal, confParam, htd, rsServices);
    }

    @Override
    public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {
      List<Cell> cells = super.get(get, withCoprocessor);
      sleep(600);
      return cells;
    }
  }
}
