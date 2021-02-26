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

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test that the utility works as expected
 */
@Category({MiscTests.class, SmallTests.class})
public class TestHFileArchiveUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileArchiveUtil.class);

  private Path rootDir = new Path("./");

  @Rule
  public TestName name = new TestName();

  @Test
  public void testGetTableArchivePath() {
    assertNotNull(HFileArchiveUtil.getTableArchivePath(rootDir,
        TableName.valueOf(name.getMethodName())));
  }

  @Test
  public void testGetArchivePath() throws Exception {
    Configuration conf = new Configuration();
    CommonFSUtils.setRootDir(conf, new Path("root"));
    assertNotNull(HFileArchiveUtil.getArchivePath(conf));
  }

  @Test
  public void testRegionArchiveDir() {
    Path regionDir = new Path("region");
    assertNotNull(HFileArchiveUtil.getRegionArchiveDir(rootDir,
        TableName.valueOf(name.getMethodName()), regionDir));
  }

  @Test
  public void testGetStoreArchivePath() throws IOException {
      byte[] family = Bytes.toBytes("Family");
    Path tabledir = CommonFSUtils.getTableDir(rootDir, TableName.valueOf(name.getMethodName()));
    HRegionInfo region = new HRegionInfo(TableName.valueOf(name.getMethodName()));
    Configuration conf = new Configuration();
    CommonFSUtils.setRootDir(conf, new Path("root"));
    assertNotNull(HFileArchiveUtil.getStoreArchivePath(conf, region, tabledir, family));
  }
}
