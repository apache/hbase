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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Unit tests to test retrieving table/region compaction state*/
@Category(LargeTests.class)
public class TestCompactionState {
  final static Log LOG = LogFactory.getLog(TestCompactionState.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static Random random = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout=60000)
  public void testMajorCompaction() throws IOException, InterruptedException {
    compaction("testMajorCompaction", 8, CompactionState.MAJOR);
  }

  @Test(timeout=60000)
  public void testMinorCompaction() throws IOException, InterruptedException {
    compaction("testMinorCompaction", 15, CompactionState.MINOR);
  }

  /**
   * Load data to a table, flush it to disk, trigger compaction,
   * confirm the compaction state is right and wait till it is done.
   *
   * @param tableName
   * @param flushes
   * @param expectedState
   * @throws IOException
   * @throws InterruptedException
   */
  private void compaction(final String tableName, final int flushes,
      final CompactionState expectedState) throws IOException, InterruptedException {
    // Create a table with regions
    byte [] table = Bytes.toBytes(tableName);
    byte [] family = Bytes.toBytes("family");
    HTable ht = null;
    try {
      ht = TEST_UTIL.createTable(table, family);
      loadData(ht, family, 3000, flushes);
      HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
      List<HRegion> regions = rs.getOnlineRegions(table);
      int countBefore = countStoreFiles(regions, family);
      assertTrue(countBefore > 0); // there should be some data files
      HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
      if (expectedState == CompactionState.MINOR) {
        admin.compact(tableName);
      } else {
        admin.majorCompact(table);
      }
      long curt = System.currentTimeMillis();
      long waitTime = 5000;
      long endt = curt + waitTime;
      CompactionState state = admin.getCompactionState(table);
      while (state == CompactionState.NONE && curt < endt) {
        Thread.sleep(10);
        state = admin.getCompactionState(table);
        curt = System.currentTimeMillis();
      }
      // Now, should have the right compaction state,
      // otherwise, the compaction should have already been done
      if (expectedState != state) {
        for (HRegion region: regions) {
          state = CompactionRequest.getCompactionState(region.getRegionId());
          assertEquals(CompactionState.NONE, state);
        }
      } else {
        curt = System.currentTimeMillis();
        waitTime = 20000;
        endt = curt + waitTime;
        state = admin.getCompactionState(table);
        while (state != CompactionState.NONE && curt < endt) {
          Thread.sleep(10);
          state = admin.getCompactionState(table);
          curt = System.currentTimeMillis();
        }
        // Now, compaction should be done.
        assertEquals(CompactionState.NONE, state);
      }
      int countAfter = countStoreFiles(regions, family);
      assertTrue(countAfter < countBefore);
      if (expectedState == CompactionState.MAJOR) assertTrue(1 == countAfter);
      else assertTrue(1 < countAfter);
    } finally {
      if (ht != null) {
        TEST_UTIL.deleteTable(table);
      }
    }
  }

  private static int countStoreFiles(
      List<HRegion> regions, final byte[] family) {
    int count = 0;
    for (HRegion region: regions) {
      count += region.getStoreFileList(new byte[][]{family}).size();
    }
    return count;
  }

  private static void loadData(final HTable ht, final byte[] family,
      final int rows, final int flushes) throws IOException {
    List<Put> puts = new ArrayList<Put>(rows);
    byte[] qualifier = Bytes.toBytes("val");
    for (int i = 0; i < flushes; i++) {
      for (int k = 0; k < rows; k++) {
        byte[] row = Bytes.toBytes(random.nextLong());
        Put p = new Put(row);
        p.add(family, qualifier, row);
        puts.add(p);
      }
      ht.put(puts);
      ht.flushCommits();
      TEST_UTIL.flush();
      puts.clear();
    }
  }
  
}
