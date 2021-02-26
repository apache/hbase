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

package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the MOB feature when enable RPC ByteBuffAllocator (HBASE-22122)
 */
@Category({ MediumTests.class })
public class TestMobWithByteBuffAllocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobWithByteBuffAllocator.class);

  private static final String TABLE_NAME = "TestMobWithByteBuffAllocator";
  private static final Logger LOG = LoggerFactory.getLogger(TestMobWithByteBuffAllocator.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Configuration CONF = UTIL.getConfiguration();
  private static final byte[] FAMILY = Bytes.toBytes("f");

  @BeforeClass
  public static void setUp() throws Exception {
    // Must use the ByteBuffAllocator here
    CONF.setBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    // Must use OFF-HEAP BucketCache here.
    CONF.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.1f);
    CONF.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    // 32MB for BucketCache.
    CONF.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 32);
    CONF.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testReadingCellsFromHFile() throws Exception {
    TableName tableName = TableName.valueOf(TABLE_NAME);
    MobSnapshotTestingUtils.createMobTable(UTIL, tableName, 1, FAMILY);
    LOG.info("Create an mob table {} successfully.", tableName);

    int expectedRows = 500;
    SnapshotTestingUtils.loadData(UTIL, tableName, expectedRows, FAMILY);
    LOG.info("Load 500 rows data into table {} successfully.", tableName);

    // Flush all the data into HFiles.
    try (Admin admin = UTIL.getConnection().getAdmin()) {
      admin.flush(tableName);
    }

    // Scan the rows
    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, tableName, expectedRows);

    // Reversed scan the rows
    int rows = 0;
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      try (ResultScanner scanner = table.getScanner(new Scan().setReversed(true))) {
        Result res = scanner.next();
        while (res != null) {
          rows++;
          for (Cell cell : res.listCells()) {
            Assert.assertTrue(CellUtil.cloneValue(cell).length > 0);
          }
          res = scanner.next();
        }
      }
    }
    Assert.assertEquals(expectedRows, rows);
  }
}
