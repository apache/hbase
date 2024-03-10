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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A test similar to TestHRegion, but with in-memory flush families. Also checks wal truncation
 * after in-memory compaction.
 */
@Category({ VerySlowRegionServerTests.class, LargeTests.class })
public class TestHRegionWithInMemoryFlush extends TestHRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHRegionWithInMemoryFlush.class);

  /**
   * @return A region on which you must call {@link HBaseTestingUtil#closeRegionAndWAL(HRegion)}
   *         when done.
   */
  @Override
  protected HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
    Configuration conf, boolean isReadOnly, Durability durability, WAL wal, byte[]... families)
    throws IOException {
    boolean[] inMemory = new boolean[families.length];
    for (int i = 0; i < inMemory.length; i++) {
      inMemory[i] = true;
    }
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null,
      MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    return TEST_UTIL.createLocalHRegionWithInMemoryFlags(tableName, startKey, stopKey, conf,
      isReadOnly, durability, wal, inMemory, families);
  }

  @Override
  protected int getTestCountForTestWritesWhileScanning() {
    return 10;
  }

  /**
   * testWritesWhileScanning is flakey when called out of this class. Need to dig in. Meantime go
   * easy on it. See if that helps.
   */
  @Override
  protected int getNumQualifiersForTestWritesWhileScanning() {
    return 10;
  }

  /**
   * A test case of HBASE-21041
   */
  @Override
  @Test
  public void testFlushAndMemstoreSizeCounting() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    int count = 0;
    for (byte[] row : HBaseTestingUtil.ROWS) {
      Put put = new Put(row);
      put.addColumn(family, family, row);
      region.put(put);
      // In memory flush every 1000 puts
      if (count++ % 1000 == 0) {
        ((CompactingMemStore) (region.getStore(family).memstore)).flushInMemory();
      }
    }
    region.flush(true);
    // After flush, data size should be zero
    assertEquals(0, region.getMemStoreDataSize());
    // After flush, a new active mutable segment is created, so the heap size
    // should equal to MutableSegment.DEEP_OVERHEAD
    assertEquals(MutableSegment.DEEP_OVERHEAD, region.getMemStoreHeapSize());
    // After flush, offheap size should be zero
    assertEquals(0, region.getMemStoreOffHeapSize());
  }
}
