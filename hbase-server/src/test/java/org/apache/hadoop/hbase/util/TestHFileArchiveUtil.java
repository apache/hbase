/*
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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test that the utility works as expected
 */
@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestHFileArchiveUtil {

  private Path rootDir = new Path("./");

  @Test
  public void testGetTableArchivePath(TestInfo testInfo) {
    assertNotNull(HFileArchiveUtil.getTableArchivePath(rootDir,
      TableName.valueOf(testInfo.getTestMethod().get().getName())));
  }

  @Test
  public void testGetArchivePath() throws Exception {
    Configuration conf = new Configuration();
    CommonFSUtils.setRootDir(conf, new Path("root"));
    assertNotNull(HFileArchiveUtil.getArchivePath(conf));
  }

  @Test
  public void testRegionArchiveDir(TestInfo testInfo) {
    Path regionDir = new Path("region");
    assertNotNull(HFileArchiveUtil.getRegionArchiveDir(rootDir,
      TableName.valueOf(testInfo.getTestMethod().get().getName()), regionDir));
  }

  @Test
  public void testGetStoreArchivePath(TestInfo testInfo) throws IOException {
    byte[] family = Bytes.toBytes("Family");
    Path tabledir = CommonFSUtils.getTableDir(rootDir,
      TableName.valueOf(testInfo.getTestMethod().get().getName()));
    RegionInfo region = RegionInfoBuilder
      .newBuilder(TableName.valueOf(testInfo.getTestMethod().get().getName())).build();
    Configuration conf = new Configuration();
    CommonFSUtils.setRootDir(conf, new Path("root"));
    assertNotNull(HFileArchiveUtil.getStoreArchivePath(conf, region, tabledir, family));
  }
}
