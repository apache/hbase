/**
 *
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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;


@Category(SmallTests.class)
public class TestBatchHRegionLockingAndWrites {
  private static final String FAMILY = "a";

  @Test
  @SuppressWarnings("unchecked")
  public void testRedundantRowKeys() throws Exception {

    final int batchSize = 100000;

    String tableName = getClass().getSimpleName();
    Configuration conf = HBaseConfiguration.create();
    conf.setClass(HConstants.REGION_IMPL, MockHRegion.class, HeapSize.class);
    MockHRegion region = (MockHRegion) TestHRegion.initHRegion(Bytes.toBytes(tableName), tableName, conf, Bytes.toBytes("a"));

    List<Pair<Mutation, Integer>> someBatch = Lists.newArrayList();
    int i = 0;
    while (i < batchSize) {
      if (i % 2 == 0) {
        someBatch.add(new Pair<Mutation, Integer>(new Put(Bytes.toBytes(0)), null));
      } else {
        someBatch.add(new Pair<Mutation, Integer>(new Put(Bytes.toBytes(1)), null));
      }
      i++;
    }
    long start = System.nanoTime();
    region.batchMutate(someBatch.toArray(new Pair[0]));
    long duration = System.nanoTime() - start;
    System.out.println("Batch mutate took: " + duration + "ns");
    assertEquals(2, region.getAcquiredLockCount());
  }

  @Test
  public void testGettingTheLockMatchesMyRow() throws Exception {
    MockHRegion region = getMockHRegion();
    HashedBytes rowKey = new HashedBytes(Bytes.toBytes(1));
    assertEquals(Integer.valueOf(2), region.getLock(null, rowKey, false));
    assertEquals(Integer.valueOf(2), region.getLock(2, rowKey, false));
  }

  private MockHRegion getMockHRegion() throws IOException {
    String tableName = getClass().getSimpleName();
    Configuration conf = HBaseConfiguration.create();
    conf.setClass(HConstants.REGION_IMPL, MockHRegion.class, HeapSize.class);
    return (MockHRegion) TestHRegion.initHRegion(Bytes.toBytes(tableName), tableName, conf, Bytes.toBytes(FAMILY));
  }

  private static class MockHRegion extends HRegion {
    private int acqioredLockCount = 0;

    public MockHRegion(Path tableDir, HLog log, FileSystem fs, Configuration conf, final HRegionInfo regionInfo, final HTableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, conf, regionInfo, htd, rsServices);
    }

    private int getAcquiredLockCount() {
      return acqioredLockCount;
    }

    @Override
    public Integer getLock(Integer lockid, HashedBytes row, boolean waitForLock) throws IOException {
      acqioredLockCount++;
      return super.getLock(lockid, row, waitForLock);
    }
  }
}
