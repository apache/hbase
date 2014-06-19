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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Class to test the checkRegionInfo() and fixRegionInfo() methods of HBaseFsck.
 */
@Category(MediumTests.class)
public class TestHBaseFsckFixRegionInfo {
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
  public void testFixRegionInfo() throws IOException, InterruptedException {

    byte[] tableName = Bytes.toBytes("testFixRegionInfo");

    byte[][] splitKeys = {
        new byte[] { 1, 1, 1 },
        new byte[] { 2, 2, 2 },
        new byte[] { 3, 3, 3 },
        new byte[] { 4, 4, 4 },
        new byte[] { 5, 5, 5 },
        new byte[] { 6, 6, 6 },
        new byte[] { 7, 7, 7 },
        new byte[] { 8, 8, 8 },
        new byte[] { 9, 9, 9 },
    };

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);

    Configuration conf = TEST_UTIL.getConfiguration();
    HBaseFsck fsckToCorrupt = new HBaseFsck(TEST_UTIL.getConfiguration());
    fsckToCorrupt.initAndScanRootMeta();
    /*
     * The following line is necessary because some .regioninfo files (specifically the ROOT and the META)
     * become outdated immediately after initialization. This is because some parameters are immediately changed
     * after initialization, and these changes are not updated in the .regioninfo. So here we update all files.
     * This allows us to accurate measure the # of corrupted files later.
     */
    fsckToCorrupt.fixRegionInfo();

    //Randomly corrupt some files
    int modifyCount = 0; // # of corrupted files
    for (HbckInfo hbi : fsckToCorrupt.getRegionInfo().values()) {
      if (Math.random() < 0.5) {
        Path tableDir = HTableDescriptor.getTableDir(FSUtils.getRootDir(conf),
            hbi.metaEntry.getTableDesc().getName());
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path regionPath = HRegion.getRegionDir(tableDir,
            hbi.metaEntry.getEncodedName());
        Path regionInfoPath = new Path(regionPath, HRegion.REGIONINFO_FILE);

        //read the original .regioninfo into an HRegionInfo
        FSDataInputStream in = fs.open(regionInfoPath);
        HRegionInfo hri_original = new HRegionInfo();
        hri_original.readFields(in);
        in.close();

        //change the HRegionInfo
        HRegionInfo hri_modified = randomlyModifyRegion(hri_original);

        //rewrite the original .regioninfo
        FSDataOutputStream out = fs.create(regionInfoPath, true);
        hri_modified.write(out);
        out.write('\n');
        out.write('\n');
        out.write(Bytes.toBytes(hri_modified.toString()));
        out.close();
        modifyCount++;
      }
    }

    //we incorrectly rewrote some .regioninfo files, so some .regioninfos are incorrect
    HBaseFsck fsckCorrupted = new HBaseFsck(TEST_UTIL.getConfiguration());
    fsckCorrupted.initAndScanRootMeta();
    Map<HRegionInfo, Path> risToRewrite = fsckCorrupted.checkRegionInfo();
    assertEquals(modifyCount, risToRewrite.size()); // # of files to rewrite must be the # of modified files

    //after fixing, we should see no errors
    HBaseFsck fsckFixed = new HBaseFsck(TEST_UTIL.getConfiguration());
    fsckFixed.initAndScanRootMeta();
    fsckFixed.fixRegionInfo();
    risToRewrite = fsckFixed.checkRegionInfo();
    assertEquals(0, risToRewrite.size());
  }

  HRegionInfo randomlyModifyRegion(HRegionInfo hri){
    HTableDescriptor tableDesc = hri.getTableDesc();
    double rand = Math.random();
    if (rand < 0.2){
      tableDesc.setReadOnly(!tableDesc.isReadOnly());
    }
    else if (rand < 0.4){
      tableDesc.setDeferredLogFlush(!tableDesc.isDeferredLogFlush());
    }
    else if (rand < 0.6){
      tableDesc.setName(Bytes.toBytes("NEWNAME"));
    }
    else if (rand < 0.8){
      tableDesc.setMaxFileSize( (long) 100);
    }
    else{
      tableDesc.setMemStoreFlushSize( (long) 200);
    }
    hri.setTableDesc(tableDesc);
    return hri;
  }

}
