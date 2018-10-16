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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests to test retrieving table/region compaction state*/
@Category({VerySlowRegionServerTests.class, LargeTests.class})
public class TestCompactionState {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionState.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactionState.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static Random random = new Random();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMajorCompaction() throws IOException, InterruptedException {
    compaction(name.getMethodName(), 8, CompactionState.MAJOR, false);
  }

  @Test
  public void testMinorCompaction() throws IOException, InterruptedException {
    compaction(name.getMethodName(), 15, CompactionState.MINOR, false);
  }

  @Test
  public void testMajorCompactionOnFamily() throws IOException, InterruptedException {
    compaction(name.getMethodName(), 8, CompactionState.MAJOR, true);
  }

  @Test
  public void testMinorCompactionOnFamily() throws IOException, InterruptedException {
    compaction(name.getMethodName(), 15, CompactionState.MINOR, true);
  }

  @Test
  public void testInvalidColumnFamily() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [] family = Bytes.toBytes("family");
    byte [] fakecf = Bytes.toBytes("fakecf");
    boolean caughtMinorCompact = false;
    boolean caughtMajorCompact = false;
    Table ht = null;
    try {
      ht = TEST_UTIL.createTable(tableName, family);
      Admin admin = TEST_UTIL.getAdmin();
      try {
        admin.compact(tableName, fakecf);
      } catch (IOException ioe) {
        caughtMinorCompact = true;
      }
      try {
        admin.majorCompact(tableName, fakecf);
      } catch (IOException ioe) {
        caughtMajorCompact = true;
      }
    } finally {
      if (ht != null) {
        TEST_UTIL.deleteTable(tableName);
      }
      assertTrue(caughtMinorCompact);
      assertTrue(caughtMajorCompact);
    }
  }

  /**
   * Load data to a table, flush it to disk, trigger compaction,
   * confirm the compaction state is right and wait till it is done.
   *
   * @param tableName
   * @param flushes
   * @param expectedState
   * @param singleFamily otherwise, run compaction on all cfs
   * @throws IOException
   * @throws InterruptedException
   */
  private void compaction(final String tableName, final int flushes,
      final CompactionState expectedState, boolean singleFamily)
      throws IOException, InterruptedException {
    // Create a table with regions
    TableName table = TableName.valueOf(tableName);
    byte [] family = Bytes.toBytes("family");
    byte [][] families =
      {family, Bytes.add(family, Bytes.toBytes("2")), Bytes.add(family, Bytes.toBytes("3"))};
    Table ht = null;
    try {
      ht = TEST_UTIL.createTable(table, families);
      loadData(ht, families, 3000, flushes);
      HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
      List<HRegion> regions = rs.getRegions(table);
      int countBefore = countStoreFilesInFamilies(regions, families);
      int countBeforeSingleFamily = countStoreFilesInFamily(regions, family);
      assertTrue(countBefore > 0); // there should be some data files
      Admin admin = TEST_UTIL.getAdmin();
      if (expectedState == CompactionState.MINOR) {
        if (singleFamily) {
          admin.compact(table, family);
        } else {
          admin.compact(table);
        }
      } else {
        if (singleFamily) {
          admin.majorCompact(table, family);
        } else {
          admin.majorCompact(table);
        }
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
        for (Region region: regions) {
          state = CompactionState.valueOf(region.getCompactionState().toString());
          assertEquals(CompactionState.NONE, state);
        }
      } else {
        // Wait until the compaction is done
        state = admin.getCompactionState(table);
        while (state != CompactionState.NONE && curt < endt) {
          Thread.sleep(10);
          state = admin.getCompactionState(table);
        }
        // Now, compaction should be done.
        assertEquals(CompactionState.NONE, state);
      }
      int countAfter = countStoreFilesInFamilies(regions, families);
      int countAfterSingleFamily = countStoreFilesInFamily(regions, family);
      assertTrue(countAfter < countBefore);
      if (!singleFamily) {
        if (expectedState == CompactionState.MAJOR) assertTrue(families.length == countAfter);
        else assertTrue(families.length < countAfter);
      } else {
        int singleFamDiff = countBeforeSingleFamily - countAfterSingleFamily;
        // assert only change was to single column family
        assertTrue(singleFamDiff == (countBefore - countAfter));
        if (expectedState == CompactionState.MAJOR) {
          assertTrue(1 == countAfterSingleFamily);
        } else {
          assertTrue(1 < countAfterSingleFamily);
        }
      }
    } finally {
      if (ht != null) {
        TEST_UTIL.deleteTable(table);
      }
    }
  }

  private static int countStoreFilesInFamily(
      List<HRegion> regions, final byte[] family) {
    return countStoreFilesInFamilies(regions, new byte[][]{family});
  }

  private static int countStoreFilesInFamilies(List<HRegion> regions, final byte[][] families) {
    int count = 0;
    for (HRegion region: regions) {
      count += region.getStoreFileList(families).size();
    }
    return count;
  }

  private static void loadData(final Table ht, final byte[][] families,
      final int rows, final int flushes) throws IOException {
    List<Put> puts = new ArrayList<>(rows);
    byte[] qualifier = Bytes.toBytes("val");
    for (int i = 0; i < flushes; i++) {
      for (int k = 0; k < rows; k++) {
        byte[] row = Bytes.toBytes(random.nextLong());
        Put p = new Put(row);
        for (int j = 0; j < families.length; ++j) {
          p.addColumn(families[j], qualifier, row);
        }
        puts.add(p);
      }
      ht.put(puts);
      TEST_UTIL.flush();
      puts.clear();
    }
  }
}
