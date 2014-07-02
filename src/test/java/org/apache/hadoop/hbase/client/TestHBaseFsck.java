/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

/**
 * Class to test HBaseFsck.
 */
@Category(MediumTests.class)
public class TestHBaseFsck {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HBaseAdmin admin;
  private static final int NUM_REGION_SERVER = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.startMiniCluster(NUM_REGION_SERVER);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
  }


  @Test
  public void testCheckRegionInfo() throws Exception {
    HTableDescriptor desc = new HTableDescriptor("testCheckRegionInfo");
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("D")));
    admin.createTable(desc);
    HBaseFsck fsck = new HBaseFsck(TEST_UTIL.getConfiguration());
    Map<HRegionInfo, Path> result = fsck.checkRegionInfo();

    assertEquals(0, result.size());
  }


  @Test
  public void testFindMultipleHoles() throws IOException, InterruptedException {

    byte[] tableName = Bytes.toBytes("testCreateTableWithRegions");

    byte [][] splitKeys = {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 },
        new byte [] { 3, 3, 3 },
        new byte [] { 4, 4, 4 },
        new byte [] { 5, 5, 5 },
        new byte [] { 6, 6, 6 },
        new byte [] { 7, 7, 7 },
        new byte [] { 8, 8, 8 },
        new byte [] { 9, 9, 9 },
    };
    int expectedRegions = splitKeys.length + 1;

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);

    Configuration conf = TEST_UTIL.getConfiguration();
    HTable ht = new HTable(conf, tableName);
    HTableDescriptor htd = ht.getTableDescriptor();
    Map<HRegionInfo,HServerAddress> regions = ht.getRegionsInfo();
    System.out.println("Deleting a few rows from HDFS and META");

    deleteRegion(conf, regions, htd, new byte[] {2,2,2}, new byte[] {3,3,3});
    deleteRegion(conf, regions, htd, new byte[] {3,3,3}, new byte[] {4,4,4});
    deleteRegion(conf, regions, htd, new byte[] {5,5,5}, new byte[] {6,6,6});
    deleteRegion(conf, regions, htd, new byte[] {8,8,8}, new byte[] {9,9,9});
    deleteRegion(conf, regions, htd, new byte[] {9,9,9}, new byte[0] );

    System.err.println("Found " + regions.size() + " regions");

    HBaseFsck fsck = new HBaseFsck(TEST_UTIL.getConfiguration());
    fsck.displayFullReport();
    fsck.setTimeLag(0);
    fsck.doWork();
    assertEquals(fsck.getTotalHolesFound(), 3);
  }

  private void deleteRegion(Configuration conf, Map<HRegionInfo,HServerAddress> regions,
                            HTableDescriptor htd, byte[] startKey, byte[] endKey)
                            throws IOException, InterruptedException {

    for (Entry<HRegionInfo, HServerAddress> e: regions.entrySet()) {
      HRegionInfo hri = e.getKey();
      HServerAddress hsa = e.getValue();
      if (Bytes.compareTo(hri.getStartKey(), startKey) == 0
          && Bytes.compareTo(hri.getEndKey(), endKey) == 0) {

        byte[] deleteRow = hri.getRegionName();
        System.out.println("deleting hdfs data: " + hri.toString() + hsa.toString());
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path p = new Path(rootDir + "/" + htd.getNameAsString(), hri.getEncodedName());
        fs.delete(p, true);

        HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
        Delete delete = new Delete(deleteRow);
        meta.delete(delete);
      }
      System.out.println(hri.toString() + hsa.toString());
    }

    TEST_UTIL.getMetaTableRows(htd.getName());
  }

}
