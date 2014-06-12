/**
 * Copyright 2014 The Apache Software Foundation
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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TestFSUtils {
  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Setting this make master start the info server.
    conf.setInt(HConstants.MASTER_INFO_PORT, 0);
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test method for {@link org.apache.hadoop.hbase.util.FSUtils#getRegionSizesInBytes(java.util.Set, org.apache.hadoop.fs.FileSystem, org.apache.hadoop.fs.Path)}.
   */
  @Test
  public void testGetRegionSizesInBytes() throws Exception {
    final StringBytes TABLE = new StringBytes("testGetRegionSizesInBytes");
    final byte[] FAMILY = Bytes.toBytes("f");
    Configuration conf = TEST_UTIL.getConfiguration();
    Path rootDir = FSUtils.getRootDir(conf);
    try (HTable table = TEST_UTIL.createTable(TABLE, FAMILY)) {
      TEST_UTIL.loadTable(table, FAMILY);

      // Flush to disk
      for (HRegionServer rs : TEST_UTIL.getOnlineRegionServers()) {
        for (HRegion region : rs.getOnlineRegions()) {
          rs.flushRegion(region.getRegionName());
        }
      }

      Map<HRegionInfo, HServerAddress> regions = table.getRegionsInfo();
      FileSystem fs = FileSystem.get(conf);
      Map<HRegionInfo, Long> regionSizes =
          FSUtils.getRegionSizesInBytes(regions.keySet(), fs, rootDir);
      System.out.println(regionSizes);

      Assert.assertEquals("Number of regions", regions.size(),
          regionSizes.size());

      for (HRegionInfo region : regions.keySet()) {
        Assert.assertNotEquals(
            "Size of region " + region.getRegionNameAsString(), 0L,
            regionSizes.get(region).longValue());
      }
    }
  }
}
