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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;

@Category({RegionServerTests.class, MediumTests.class})
public class TestWALFiltering {
  private static final int NUM_MASTERS = 1;
  private static final int NUM_RS = 4;

  private static final TableName TABLE_NAME =
      TableName.valueOf("TestWALFiltering");
  private static final byte[] CF1 = Bytes.toBytes("MyCF1");
  private static final byte[] CF2 = Bytes.toBytes("MyCF2");
  private static final byte[][] FAMILIES = { CF1, CF2 };

  private HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    fillTable();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void fillTable() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createTable(TABLE_NAME, FAMILIES, 3,
        Bytes.toBytes("row0"), Bytes.toBytes("row99"), NUM_RS);
    Random rand = new Random(19387129L);
    for (int iStoreFile = 0; iStoreFile < 4; ++iStoreFile) {
      for (int iRow = 0; iRow < 100; ++iRow) {
        final byte[] row = Bytes.toBytes("row" + iRow);
        Put put = new Put(row);
        Delete del = new Delete(row);
        for (int iCol = 0; iCol < 10; ++iCol) {
          final byte[] cf = rand.nextBoolean() ? CF1 : CF2;
          final long ts = Math.abs(rand.nextInt());
          final byte[] qual = Bytes.toBytes("col" + iCol);
          if (rand.nextBoolean()) {
            final byte[] value = Bytes.toBytes("value_for_row_" + iRow +
                "_cf_" + Bytes.toStringBinary(cf) + "_col_" + iCol + "_ts_" +
                ts + "_random_" + rand.nextLong());
            put.add(cf, qual, ts, value);
          } else if (rand.nextDouble() < 0.8) {
            del.addColumn(cf, qual, ts);
          } else {
            del.addColumn(cf, qual, ts);
          }
        }
        table.put(put);
        table.delete(del);
      }
    }
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
  }

  @Test
  public void testFlushedSequenceIdsSentToHMaster()
  throws IOException, InterruptedException, ServiceException {
    SortedMap<byte[], Long> allFlushedSequenceIds =
        new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < NUM_RS; ++i) {
      flushAllRegions(i);
    }
    Thread.sleep(10000);
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    for (int i = 0; i < NUM_RS; ++i) {
      for (byte[] regionName : getRegionsByServer(i)) {
        if (allFlushedSequenceIds.containsKey(regionName)) {
          GetLastFlushedSequenceIdRequest req =
            RequestConverter.buildGetLastFlushedSequenceIdRequest(regionName);

          assertEquals((long)allFlushedSequenceIds.get(regionName),
            master.getMasterRpcServices().getLastFlushedSequenceId(
              null, req).getLastFlushedSequenceId());
        }
      }
    }
  }

  private List<byte[]> getRegionsByServer(int rsId) throws IOException {
    List<byte[]> regionNames = Lists.newArrayList();
    HRegionServer hrs = getRegionServer(rsId);
    for (Region r : hrs.getOnlineRegions(TABLE_NAME)) {
      regionNames.add(r.getRegionInfo().getRegionName());
    }
    return regionNames;
  }

  private HRegionServer getRegionServer(int rsId) {
    return TEST_UTIL.getMiniHBaseCluster().getRegionServer(rsId);
  }

  private void flushAllRegions(int rsId)
  throws ServiceException, IOException {
    HRegionServer hrs = getRegionServer(rsId);
    for (byte[] regionName : getRegionsByServer(rsId)) {
      FlushRegionRequest request =
        RequestConverter.buildFlushRegionRequest(regionName);
      hrs.getRSRpcServices().flushRegion(null, request);
    }
  }

}
