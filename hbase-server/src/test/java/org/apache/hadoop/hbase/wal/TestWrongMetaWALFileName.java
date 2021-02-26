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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-21843. Used to confirm that we use the correct name for meta wal file when
 * using {@link RegionGroupingProvider}.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestWrongMetaWALFileName {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWrongMetaWALFileName.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, WALFactory.Providers.multiwal.name());
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    // create a table so that we will write some edits to meta
    TableName tableName = TableName.valueOf("whatever");
    UTIL.createTable(tableName, Bytes.toBytes("family"));
    UTIL.waitTableAvailable(tableName);
    HRegionServer rs = UTIL.getMiniHBaseCluster().getRegionServer(0);
    Path walDir = new Path(rs.getWALRootDir(),
      AbstractFSWALProvider.getWALDirectoryName(rs.getServerName().toString()));
    // we should have meta wal files.
    assertTrue(
      rs.getWALFileSystem().listStatus(walDir, AbstractFSWALProvider::isMetaFile).length > 0);
  }
}
