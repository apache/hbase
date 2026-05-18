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

import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTOR_CLASS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue.KeyOnlyKeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestCompactorMemLeak {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final Configuration CONF = UTIL.getConfiguration();
  private static final AtomicBoolean IS_LAST_CELL_ON_HEAP = new AtomicBoolean(false);
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE = Bytes.toBytes("value");
  private String name;

  @BeforeEach
  public void setTestName(TestInfo testInfo) {
    this.name = testInfo.getTestMethod().get().getName();
  }

  @BeforeAll
  public static void setUp() throws Exception {
    IS_LAST_CELL_ON_HEAP.set(false);
    // Must use the ByteBuffAllocator here
    CONF.setBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    // Must use OFF-HEAP BucketCache here.
    CONF.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.1f);
    CONF.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    // 32MB for BucketCache.
    CONF.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 32);
    // Use the MyCompactor
    CONF.set(DEFAULT_COMPACTOR_CLASS_KEY, MyCompactor.class.getName());
    UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    IS_LAST_CELL_ON_HEAP.set(false);
    UTIL.shutdownMiniCluster();
  }

  private void assertMajorCompactionOK(TableName tableName) {
    List<HRegion> regions = UTIL.getHBaseCluster().getRegionServerThreads().get(0).getRegionServer()
      .getRegions(tableName);
    assertEquals(regions.size(), 1);
    HRegion region = regions.get(0);
    assertEquals(region.getStores().size(), 1);
    HStore store = region.getStore(FAMILY);
    assertEquals(store.getStorefilesCount(), 1);
  }

  @Test
  public void testMemLeak() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf(name);
    Table table = UTIL.createTable(tableName, FAMILY);

    // Put and Flush #1
    Put put = new Put(Bytes.toBytes("row1")).addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    UTIL.getAdmin().flush(tableName);

    // Put and Flush #2
    put = new Put(Bytes.toBytes("row2")).addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    UTIL.getAdmin().flush(tableName);

    // Major compact
    UTIL.getAdmin().majorCompact(tableName);
    Thread.sleep(6000);
    assertMajorCompactionOK(tableName);

    // The last cell before Compactor#commitWriter must be an heap one.
    assertTrue(IS_LAST_CELL_ON_HEAP.get());
  }

  public static class MyCompactor extends DefaultCompactor {

    public MyCompactor(Configuration conf, HStore store) {
      super(conf, store);
    }

    @Override
    protected List<Path> commitWriter(StoreFileWriter writer, FileDetails fd,
      CompactionRequestImpl request) throws IOException {
      HFileWriterImpl writerImpl = (HFileWriterImpl) writer.getLiveFileWriter();
      Cell cell = writerImpl.getLastCell();
      // The cell should be backend with an KeyOnlyKeyValue.
      IS_LAST_CELL_ON_HEAP.set(cell instanceof KeyOnlyKeyValue);
      return super.commitWriter(writer, fd, request);
    }
  }

}
