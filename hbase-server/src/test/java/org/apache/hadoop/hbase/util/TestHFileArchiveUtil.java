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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Test that the utility works as expected
 */
@Category(SmallTests.class)
public class TestHFileArchiveUtil {
  private Path rootDir = new Path("./");
  @Test
  public void testGetTableArchivePath() {
    assertNotNull(HFileArchiveUtil.getTableArchivePath(rootDir,
        TableName.valueOf("table")));
  }

  @Test
  public void testGetArchivePath() throws Exception {
    Configuration conf = new Configuration();
    FSUtils.setRootDir(conf, new Path("root"));
    assertNotNull(HFileArchiveUtil.getArchivePath(conf));
  }
  
  @Test
  public void testRegionArchiveDir() {
    Path regionDir = new Path("region");
    assertNotNull(HFileArchiveUtil.getRegionArchiveDir(rootDir,
        TableName.valueOf("table"), regionDir));
  }
  
  @Test
  public void testGetStoreArchivePath() throws IOException {
      byte[] family = Bytes.toBytes("Family");
    Path tabledir = FSUtils.getTableDir(rootDir,
        TableName.valueOf("table"));
    HRegionInfo region = new HRegionInfo(TableName.valueOf("table"));
    Configuration conf = new Configuration();
    FSUtils.setRootDir(conf, new Path("root"));
    assertNotNull(HFileArchiveUtil.getStoreArchivePath(conf, region, tabledir, family));
  }
}
