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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class TestCompactSplitThread {
  private static final Log LOG = LogFactory.getLog(TestCompactSplitThread.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());
  private final byte[] family = Bytes.toBytes("f");

  @Test
  public void testThreadPoolSizeTuning() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(CompactSplitThread.LARGE_COMPACTION_THREADS, 3);
    conf.setInt(CompactSplitThread.SMALL_COMPACTION_THREADS, 4);
    conf.setInt(CompactSplitThread.SPLIT_THREADS, 5);
    conf.setInt(CompactSplitThread.MERGE_THREADS, 6);
    TEST_UTIL.startMiniCluster(1);
    Connection conn = ConnectionFactory.createConnection(conf);
    try {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(family));
      htd.setCompactionEnabled(false);
      TEST_UTIL.getHBaseAdmin().createTable(htd);
      TEST_UTIL.waitTableAvailable(tableName);
      HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(tableName);

      // check initial configuration of thread pool sizes
      assertEquals(3, regionServer.compactSplitThread.getLargeCompactionThreadNum());
      assertEquals(4, regionServer.compactSplitThread.getSmallCompactionThreadNum());
      assertEquals(5, regionServer.compactSplitThread.getSplitThreadNum());
      assertEquals(6, regionServer.compactSplitThread.getMergeThreadNum());

      // change bigger configurations and do online update
      conf.setInt(CompactSplitThread.LARGE_COMPACTION_THREADS, 4);
      conf.setInt(CompactSplitThread.SMALL_COMPACTION_THREADS, 5);
      conf.setInt(CompactSplitThread.SPLIT_THREADS, 6);
      conf.setInt(CompactSplitThread.MERGE_THREADS, 7);
      try {
        regionServer.compactSplitThread.onConfigurationChange(conf);
      } catch (IllegalArgumentException iae) {
        Assert.fail("Update bigger configuration failed!");
      }

      // check again after online update
      assertEquals(4, regionServer.compactSplitThread.getLargeCompactionThreadNum());
      assertEquals(5, regionServer.compactSplitThread.getSmallCompactionThreadNum());
      assertEquals(6, regionServer.compactSplitThread.getSplitThreadNum());
      assertEquals(7, regionServer.compactSplitThread.getMergeThreadNum());

      // change smaller configurations and do online update
      conf.setInt(CompactSplitThread.LARGE_COMPACTION_THREADS, 2);
      conf.setInt(CompactSplitThread.SMALL_COMPACTION_THREADS, 3);
      conf.setInt(CompactSplitThread.SPLIT_THREADS, 4);
      conf.setInt(CompactSplitThread.MERGE_THREADS, 5);
      try {
        regionServer.compactSplitThread.onConfigurationChange(conf);
      } catch (IllegalArgumentException iae) {
        Assert.fail("Update smaller configuration failed!");
      }

      // check again after online update
      assertEquals(2, regionServer.compactSplitThread.getLargeCompactionThreadNum());
      assertEquals(3, regionServer.compactSplitThread.getSmallCompactionThreadNum());
      assertEquals(4, regionServer.compactSplitThread.getSplitThreadNum());
      assertEquals(5, regionServer.compactSplitThread.getMergeThreadNum());
    } finally {
      conn.close();
      TEST_UTIL.shutdownMiniCluster();
    }
  }
}
