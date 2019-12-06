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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({IOTests.class, MediumTests.class})
public class TestPrefetchOnOpen {
  private static final String FAMILYNAME = "fam";
  private static final byte[] FAMILYBYTES = Bytes.toBytes(FAMILYNAME);
  private static final byte[] COLBYTES = Bytes.toBytes("col");

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPrefetchOnOpen.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPrefetchWithRegionOpenOnly() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    Table table = TEST_UTIL.createTable(tableName, FAMILYNAME);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tableName);
    assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    ServerName sn = TEST_UTIL.getMiniHBaseCluster().getServerHoldingRegion(tableName,
      region.getRegionInfo().getRegionName());
    BlockCache cache = TEST_UTIL.getHBaseCluster().getRegionServer(sn).getBlockCache().get();

    writeSomeRecords(table);
    TEST_UTIL.flush(tableName);

    Path regionDir = FSUtils.getRegionDirFromRootDir(
        TEST_UTIL.getDefaultRootDirPath(), region.getRegionInfo());
    Path famDir = FSUtils.getFamilyDirs(TEST_UTIL.getTestFileSystem(), regionDir).get(0);
    FileStatus[] files = TEST_UTIL.getTestFileSystem().listStatus(famDir);
    waitUntilPrefetchComplete(files);
    // no prefetch to do with flush
    assertEquals(0, cache.getBlockCount());

    writeSomeRecords(table);
    TEST_UTIL.flush(tableName);
    TEST_UTIL.compact(tableName, true);

    files = TEST_UTIL.getTestFileSystem().listStatus(famDir);
    waitUntilPrefetchComplete(files);
    // no prefetch to do with compaction
    assertEquals(0, cache.getBlockCount());

    TEST_UTIL.getAdmin().disableTable(tableName);
    TEST_UTIL.getAdmin().enableTable(tableName);
    files = TEST_UTIL.getTestFileSystem().listStatus(famDir);
    waitUntilPrefetchComplete(files);
    // do prefetch with region open
    assertEquals(1, cache.getBlockCount());
  }

  private void writeSomeRecords(Table table) throws IOException {
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(FAMILYBYTES, COLBYTES, Bytes.toBytes("value" + i));
      table.put(put);
    }
  }

  private void waitUntilPrefetchComplete(FileStatus[] files) throws InterruptedException {
    for (FileStatus file : files) {
      while (!PrefetchExecutor.isCompleted(file.getPath())) {
        // Sleep for a bit
        Thread.sleep(1000);
      }
    }
  }
}
