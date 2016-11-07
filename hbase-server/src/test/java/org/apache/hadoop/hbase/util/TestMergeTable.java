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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests merging a normal table's regions
 */
@Category({MiscTests.class, MediumTests.class})
public class TestMergeTable {
  private static final Log LOG = LogFactory.getLog(TestMergeTable.class);
  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte [] COLUMN_NAME = Bytes.toBytes("contents");
  private static final byte [] VALUE;
  static {
    // We will use the same value for the rows as that is not really important here
    String partialValue = String.valueOf(System.currentTimeMillis());
    StringBuilder val = new StringBuilder();
    while (val.length() < 1024) {
      val.append(partialValue);
    }
    VALUE = Bytes.toBytes(val.toString());
  }

  /**
   * Test merge.
   * Hand-makes regions of a mergeable size and adds the hand-made regions to
   * hand-made meta.  The hand-made regions are created offline.  We then start
   * up mini cluster, disables the hand-made table and starts in on merging.
   */
  @Test (timeout=300000)
  public void testMergeTable() throws Exception {
    // Table we are manually creating offline.
    HTableDescriptor desc = new HTableDescriptor(org.apache.hadoop.hbase.TableName.valueOf(Bytes.toBytes("test")));
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));

    // Set maximum regionsize down.
    UTIL.getConfiguration().setLong(HConstants.HREGION_MAX_FILESIZE, 64L * 1024L * 1024L);
    // Make it so we don't split.
    UTIL.getConfiguration().setInt("hbase.regionserver.regionSplitLimit", 0);
    // Startup hdfs.  Its in here we'll be putting our manually made regions.
    UTIL.startMiniDFSCluster(1);
    // Create hdfs hbase rootdir.
    UTIL.createRootDir();

    // Now create three data regions: The first is too large to merge since it
    // will be > 64 MB in size. The second two will be smaller and will be
    // selected for merging.

    // To ensure that the first region is larger than 64MB we need to write at
    // least 65536 rows. We will make certain by writing 70000
    byte [] row_70001 = Bytes.toBytes("row_70001");
    byte [] row_80001 = Bytes.toBytes("row_80001");

    // Create regions and populate them at same time.  Create the tabledir
    // for them first.
    MasterStorage masterStorage = MasterStorage.open(UTIL.getConfiguration(), true);
    masterStorage.createTableDescriptor(desc, false);
    HRegion [] regions = {
      createRegion(desc, null, row_70001, 1, 70000),
      createRegion(desc, row_70001, row_80001, 70001, 10000),
      createRegion(desc, row_80001, null, 80001, 11000)
    };

    // Now create the root and meta regions and insert the data regions
    // created above into hbase:meta
    setupMeta(regions);
    try {
      LOG.info("Starting mini zk cluster");
      UTIL.startMiniZKCluster();
      LOG.info("Starting mini hbase cluster");
      UTIL.startMiniHBaseCluster(1, 1);
      Configuration c = new Configuration(UTIL.getConfiguration());
      Connection connection = UTIL.getConnection();

      List<HRegionInfo> originalTableRegions =
        MetaTableAccessor.getTableRegions(connection, desc.getTableName());
      LOG.info("originalTableRegions size=" + originalTableRegions.size() +
        "; " + originalTableRegions);
      Admin admin = connection.getAdmin();
      admin.disableTable(desc.getTableName());
      admin.close();
      HMerge.merge(c, FileSystem.get(c), desc.getTableName());
      List<HRegionInfo> postMergeTableRegions =
        MetaTableAccessor.getTableRegions(connection, desc.getTableName());
      LOG.info("postMergeTableRegions size=" + postMergeTableRegions.size() +
        "; " + postMergeTableRegions);
      assertTrue("originalTableRegions=" + originalTableRegions.size() +
        ", postMergeTableRegions=" + postMergeTableRegions.size(),
        postMergeTableRegions.size() < originalTableRegions.size());
      LOG.info("Done with merge");
    } finally {
      UTIL.shutdownMiniCluster();
      LOG.info("After cluster shutdown");
    }
  }

  private HRegion createRegion(final HTableDescriptor desc,
      byte [] startKey, byte [] endKey, int firstRow, int nrows)
  throws IOException {
    HRegionInfo hri = new HRegionInfo(desc.getTableName(), startKey, endKey);
    HRegion region = UTIL.createLocalHRegion(hri, desc);
    LOG.info("Created region " + region.getRegionInfo().getRegionNameAsString());
    for(int i = firstRow; i < firstRow + nrows; i++) {
      Put put = new Put(Bytes.toBytes("row_" + String.format("%1$05d", i)));
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(COLUMN_NAME, null, VALUE);
      region.put(put);
      if (i % 10000 == 0) {
        LOG.info("Flushing write #" + i);
        region.flush(true);
      }
    }
    HBaseTestingUtility.closeRegionAndWAL(region);
    return region;
  }

  protected void setupMeta(final HRegion[] regions) throws IOException {
    HRegion meta = UTIL.createLocalHRegion(HRegionInfo.FIRST_META_REGIONINFO,
        UTIL.getMetaTableDescriptor());
    for (HRegion r : regions) {
      HRegion.addRegionToMETA(meta, r);
    }
    HBaseTestingUtility.closeRegionAndWAL(meta);
  }
}

