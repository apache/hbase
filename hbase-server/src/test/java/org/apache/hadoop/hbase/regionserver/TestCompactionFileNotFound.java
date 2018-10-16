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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the scenario where a store refresh happens due to a file not found during scan,
 * after a compaction but before the compacted files are archived. At this state we test for a split
 * and compaction
 */
@Category(MediumTests.class)
public class TestCompactionFileNotFound {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionFileNotFound.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactionFileNotFound.class);
  private static final HBaseTestingUtility util = new HBaseTestingUtility();

  private static final TableName TEST_TABLE = TableName.valueOf("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] ROW_B = Bytes.toBytes("bbb");
  private static final byte[] ROW_C = Bytes.toBytes("ccc");

  private static final byte[] qualifierCol1 = Bytes.toBytes("col1");

  private static final byte[] bytes1 = Bytes.toBytes(1);
  private static final byte[] bytes2 = Bytes.toBytes(2);
  private static final byte[] bytes3 = Bytes.toBytes(3);

  private Table table;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.setInt("hbase.hfile.compaction.discharger.interval",
      Integer.MAX_VALUE);
    util.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @After
  public void after() throws Exception {
    try {
      if (table != null) {
        table.close();
      }
    } finally {
      util.deleteTable(TEST_TABLE);
    }
  }

  @Test
  public void testSplitAfterRefresh() throws Exception {
    Admin admin = util.getAdmin();
    table = util.createTable(TEST_TABLE, TEST_FAMILY);

    try {
      // Create Multiple store files
      Put puta = new Put(ROW_A);
      puta.addColumn(TEST_FAMILY, qualifierCol1, bytes1);
      table.put(puta);
      admin.flush(TEST_TABLE);

      Put putb = new Put(ROW_B);
      putb.addColumn(TEST_FAMILY, qualifierCol1, bytes2);
      table.put(putb);
      admin.flush(TEST_TABLE);

      Put putc = new Put(ROW_C);
      putc.addColumn(TEST_FAMILY, qualifierCol1, bytes3);
      table.put(putc);
      admin.flush(TEST_TABLE);

      admin.compact(TEST_TABLE);
      while (admin.getCompactionState(TEST_TABLE) != CompactionState.NONE) {
        Thread.sleep(1000);
      }
      table.put(putb);
      HRegion hr1 = (HRegion) util.getRSForFirstRegionInTable(TEST_TABLE)
          .getRegionByEncodedName(admin.getTableRegions(TEST_TABLE).get(0).getEncodedName());
      // Refresh store files post compaction, this should not open already compacted files
      hr1.refreshStoreFiles(true);
      int numRegionsBeforeSplit = admin.getTableRegions(TEST_TABLE).size();
      // Check if we can successfully split after compaction
      admin.splitRegion(admin.getTableRegions(TEST_TABLE).get(0).getEncodedNameAsBytes(), ROW_C);
      util.waitFor(20000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          int numRegionsAfterSplit = 0;
          List<RegionServerThread> rst = util.getMiniHBaseCluster().getLiveRegionServerThreads();
          for (RegionServerThread t : rst) {
            numRegionsAfterSplit += t.getRegionServer().getRegions(TEST_TABLE).size();
          }
          // Make sure that the split went through and all the regions are assigned
          return (numRegionsAfterSplit == numRegionsBeforeSplit + 1
              && admin.isTableAvailable(TEST_TABLE));
        }
      });
      // Split at this point should not result in the RS being aborted
      assertEquals(3, util.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  @Test
  public void testCompactionAfterRefresh() throws Exception {
    Admin admin = util.getAdmin();
    table = util.createTable(TEST_TABLE, TEST_FAMILY);
    try {
      // Create Multiple store files
      Put puta = new Put(ROW_A);
      puta.addColumn(TEST_FAMILY, qualifierCol1, bytes1);
      table.put(puta);
      admin.flush(TEST_TABLE);

      Put putb = new Put(ROW_B);
      putb.addColumn(TEST_FAMILY, qualifierCol1, bytes2);
      table.put(putb);
      admin.flush(TEST_TABLE);

      Put putc = new Put(ROW_C);
      putc.addColumn(TEST_FAMILY, qualifierCol1, bytes3);
      table.put(putc);
      admin.flush(TEST_TABLE);

      admin.compact(TEST_TABLE);
      while (admin.getCompactionState(TEST_TABLE) != CompactionState.NONE) {
        Thread.sleep(1000);
      }
      table.put(putb);
      HRegion hr1 = (HRegion) util.getRSForFirstRegionInTable(TEST_TABLE)
          .getRegionByEncodedName(admin.getTableRegions(TEST_TABLE).get(0).getEncodedName());
      // Refresh store files post compaction, this should not open already compacted files
      hr1.refreshStoreFiles(true);
      // Archive the store files and try another compaction to see if all is good
      for (HStore store : hr1.getStores()) {
        store.closeAndArchiveCompactedFiles();
      }
      try {
        hr1.compact(false);
      } catch (IOException e) {
        LOG.error("Got an exception during compaction", e);
        if (e instanceof FileNotFoundException) {
          Assert.fail("Got a FNFE during compaction");
        } else {
          Assert.fail();
        }
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
}
