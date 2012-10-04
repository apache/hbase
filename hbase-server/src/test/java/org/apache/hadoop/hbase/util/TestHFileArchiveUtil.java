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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test that the utility works as expected
 */
@Category(SmallTests.class)
public class TestHFileArchiveUtil {

  @Test
  public void testGetTableArchivePath() {
    assertNotNull(HFileArchiveUtil.getTableArchivePath(new Path("table")));
    assertNotNull(HFileArchiveUtil.getTableArchivePath(new Path("root", new Path("table"))));
  }

  @Test
  public void testGetArchivePath() throws Exception {
    Configuration conf = new Configuration();
    FSUtils.setRootDir(conf, new Path("root"));
    assertNotNull(HFileArchiveUtil.getArchivePath(conf));
  }
  
  @Test
  public void testRegionArchiveDir() {
    Path tableDir = new Path("table");
    Path regionDir = new Path("region");
    assertNotNull(HFileArchiveUtil.getRegionArchiveDir(null, tableDir, regionDir));
  }
  
  @Test
  public void testGetStoreArchivePath(){
      byte[] family = Bytes.toBytes("Family");
    Path tabledir = new Path("table");
    HRegionInfo region = new HRegionInfo(Bytes.toBytes("table"));
    Configuration conf = null;
    assertNotNull(HFileArchiveUtil.getStoreArchivePath(conf, region, tabledir, family));
    conf = new Configuration();
    assertNotNull(HFileArchiveUtil.getStoreArchivePath(conf, region, tabledir, family));

    // do a little mocking of a region to get the same results
    HRegion mockRegion = Mockito.mock(HRegion.class);
    Mockito.when(mockRegion.getRegionInfo()).thenReturn(region);
    Mockito.when(mockRegion.getTableDir()).thenReturn(tabledir);

    assertNotNull(HFileArchiveUtil.getStoreArchivePath(null, mockRegion, family));
    conf = new Configuration();
    assertNotNull(HFileArchiveUtil.getStoreArchivePath(conf, mockRegion, family));
  }
}
