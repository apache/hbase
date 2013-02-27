/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import junit.framework.TestCase;

@Category(SmallTests.class)
public class TestHRegionFileSystem {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Test
  public void testOnDiskRegionCreation() throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("testOnDiskRegionCreation");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();

    // Create a Region
    HRegionInfo hri = new HRegionInfo(Bytes.toBytes("TestTable"));
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, rootDir, hri);

    // Verify if the region is on disk
    Path regionDir = regionFs.getRegionDir();
    assertTrue("The region folder should be created", fs.exists(regionDir));

    // Verify the .regioninfo
    HRegionInfo hriVerify = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
    assertEquals(hri, hriVerify);

    // Open the region
    regionFs = HRegionFileSystem.openRegionFromFileSystem(conf, fs, rootDir, hri);
    assertEquals(regionDir, regionFs.getRegionDir());

    // Delete the region
    HRegionFileSystem.deleteRegionFromFileSystem(conf, fs, rootDir, hri);
    assertFalse("The region folder should be removed", fs.exists(regionDir));

    fs.delete(rootDir, true);
  }
}
