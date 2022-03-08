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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class which verifies that region sizes are reported to the master.
 */
@Category(MediumTests.class)
public class TestRegionSizeUse {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionSizeUse.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionSizeUse.class);
  private static final int SIZE_PER_VALUE = 256;
  private static final int NUM_SPLITS = 10;
  private static final String F1 = "f1";
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private MiniHBaseCluster cluster;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Increase the frequency of some of the chores for responsiveness of the test
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    cluster = TEST_UTIL.startMiniCluster(2);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBasicRegionSizeReports() throws Exception {
    final long bytesWritten = 5L * 1024L * 1024L; // 5MB
    final TableName tn = writeData(bytesWritten);
    LOG.debug("Data was written to HBase");
    final Admin admin = TEST_UTIL.getAdmin();
    // Push the data to disk.
    admin.flush(tn);
    LOG.debug("Data flushed to disk");
    // Get the final region distribution
    final List<RegionInfo> regions = TEST_UTIL.getAdmin().getRegions(tn);

    HMaster master = cluster.getMaster();
    MasterQuotaManager quotaManager = master.getMasterQuotaManager();
    Map<RegionInfo,Long> regionSizes = quotaManager.snapshotRegionSizes();
    // Wait until we get all of the region reports for our table
    // The table may split, so make sure we have at least as many as expected right after we
    // finished writing the data.
    int observedRegions = numRegionsForTable(tn, regionSizes);
    while (observedRegions < regions.size()) {
      LOG.debug("Expecting more regions. Saw " + observedRegions
          + " region sizes reported, expected at least " + regions.size());
      Thread.sleep(1000);
      regionSizes = quotaManager.snapshotRegionSizes();
      observedRegions = numRegionsForTable(tn, regionSizes);
    }

    LOG.debug("Observed region sizes by the HMaster: " + regionSizes);
    long totalRegionSize = 0L;
    for (Long regionSize : regionSizes.values()) {
      totalRegionSize += regionSize;
    }
    assertTrue("Expected region size report to exceed " + bytesWritten + ", but was "
        + totalRegionSize + ". RegionSizes=" + regionSizes, bytesWritten < totalRegionSize);
  }

  /**
   * Writes at least {@code sizeInBytes} bytes of data to HBase and returns the TableName used.
   *
   * @param sizeInBytes The amount of data to write in bytes.
   * @return The table the data was written to
   */
  private TableName writeData(long sizeInBytes) throws IOException {
    final Connection conn = TEST_UTIL.getConnection();
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf(testName.getMethodName());

    // Delete the old table
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    // Create the table
    HTableDescriptor tableDesc = new HTableDescriptor(tn);
    tableDesc.addFamily(new HColumnDescriptor(F1));
    admin.createTable(tableDesc, Bytes.toBytes("1"), Bytes.toBytes("9"), NUM_SPLITS);

    final Table table = conn.getTable(tn);
    try {
      List<Put> updates = new ArrayList<>();
      long bytesToWrite = sizeInBytes;
      long rowKeyId = 0L;
      final StringBuilder sb = new StringBuilder();
      while (bytesToWrite > 0L) {
        sb.setLength(0);
        sb.append(Long.toString(rowKeyId));
        // Use the reverse counter as the rowKey to get even spread across all regions
        Put p = new Put(Bytes.toBytes(sb.reverse().toString()));
        byte[] value = new byte[SIZE_PER_VALUE];
        Bytes.random(value);
        p.addColumn(Bytes.toBytes(F1), Bytes.toBytes("q1"), value);
        updates.add(p);

        // Batch 50K worth of updates
        if (updates.size() > 50) {
          table.put(updates);
          updates.clear();
        }

        // Just count the value size, ignore the size of rowkey + column
        bytesToWrite -= SIZE_PER_VALUE;
        rowKeyId++;
      }

      // Write the final batch
      if (!updates.isEmpty()) {
        table.put(updates);
      }

      return tn;
    } finally {
      table.close();
    }
  }

  /**
   * Computes the number of regions for the given table that have a positive size.
   *
   * @param tn The TableName in question
   * @param regions A collection of region sizes
   * @return The number of regions for the given table.
   */
  private int numRegionsForTable(TableName tn, Map<RegionInfo,Long> regions) {
    int sum = 0;
    for (Entry<RegionInfo,Long> entry : regions.entrySet()) {
      if (tn.equals(entry.getKey().getTable()) && 0 < entry.getValue()) {
        sum++;
      }
    }
    return sum;
  }
}
