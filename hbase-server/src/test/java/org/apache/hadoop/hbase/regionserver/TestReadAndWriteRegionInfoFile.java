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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestReadAndWriteRegionInfoFile {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadAndWriteRegionInfoFile.class);

  private static final HBaseCommonTestingUtility UTIL = new HBaseTestingUtility();

  private static final Configuration CONF = UTIL.getConfiguration();

  private static FileSystem FS;

  private static Path ROOT_DIR;

  @BeforeClass
  public static void setUp() throws IOException {
    ROOT_DIR = UTIL.getDataTestDir();
    FS = ROOT_DIR.getFileSystem(CONF);
  }

  @AfterClass
  public static void tearDown() {
    UTIL.cleanupTestDir();
  }

  @Test
  public void testReadAndWriteRegionInfoFile() throws IOException, InterruptedException {
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build();
    // Create a region. That'll write the .regioninfo file.
    FSTableDescriptors fsTableDescriptors = new FSTableDescriptors(FS, ROOT_DIR);
    FSTableDescriptors.tryUpdateMetaTableDescriptor(CONF, FS, ROOT_DIR, null);
    HRegion r = HBaseTestingUtility.createRegionAndWAL(ri, ROOT_DIR, CONF,
      fsTableDescriptors.get(TableName.META_TABLE_NAME));
    // Get modtime on the file.
    long modtime = getModTime(r);
    HBaseTestingUtility.closeRegionAndWAL(r);
    Thread.sleep(1001);
    r = HRegion.openHRegion(ROOT_DIR, ri, fsTableDescriptors.get(TableName.META_TABLE_NAME), null,
      CONF);
    // Ensure the file is not written for a second time.
    long modtime2 = getModTime(r);
    assertEquals(modtime, modtime2);
    // Now load the file.
    HRegionFileSystem.loadRegionInfoFileContent(r.getRegionFileSystem().getFileSystem(),
      r.getRegionFileSystem().getRegionDir());
    HBaseTestingUtility.closeRegionAndWAL(r);
  }

  private long getModTime(final HRegion r) throws IOException {
    FileStatus[] statuses = r.getRegionFileSystem().getFileSystem().listStatus(
      new Path(r.getRegionFileSystem().getRegionDir(), HRegionFileSystem.REGION_INFO_FILE));
    assertTrue(statuses != null && statuses.length == 1);
    return statuses[0].getModificationTime();
  }
}
