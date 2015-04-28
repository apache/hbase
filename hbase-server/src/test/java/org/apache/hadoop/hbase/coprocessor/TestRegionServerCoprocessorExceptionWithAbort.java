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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests unhandled exceptions thrown by coprocessors running on a regionserver..
 * Expected result is that the regionserver will abort with an informative
 * error message describing the set of its loaded coprocessors for crash
 * diagnosis. (HBASE-4014).
 */
@Category(MediumTests.class)
public class TestRegionServerCoprocessorExceptionWithAbort {
  private static final Log LOG = LogFactory.getLog(
    TestRegionServerCoprocessorExceptionWithAbort.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("observed_table");

  @Test(timeout=60000)
  public void testExceptionDuringInitialization() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);  // Let's fail fast.
    conf.setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, true);
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
    TEST_UTIL.startMiniCluster(2);
    try {
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      // Trigger one regionserver to fail as if it came up with a coprocessor
      // that fails during initialization
      final HRegionServer regionServer = cluster.getRegionServer(0);
      conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        FailedInitializationObserver.class.getName());
      regionServer.getRegionServerCoprocessorHost().loadSystemCoprocessors(conf,
        CoprocessorHost.REGION_COPROCESSOR_CONF_KEY);
      TEST_UTIL.waitFor(10000, 1000, new Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return regionServer.isAborted();
        }
      });
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout=60000)
  public void testExceptionFromCoprocessorDuringPut() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);  // Let's fail fast.
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, BuggyRegionObserver.class.getName());
    conf.setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, true);
    TEST_UTIL.startMiniCluster(2);
    try {
      // When we try to write to TEST_TABLE, the buggy coprocessor will
      // cause a NullPointerException, which will cause the regionserver (which
      // hosts the region we attempted to write to) to abort.
      final byte[] TEST_FAMILY = Bytes.toBytes("aaa");

      HTable table = TEST_UTIL.createMultiRegionTable(TABLE_NAME, TEST_FAMILY);
      TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);

      // Note which regionServer will abort (after put is attempted).
      final HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);

      try {
        final byte[] ROW = Bytes.toBytes("aaa");
        Put put = new Put(ROW);
        put.add(TEST_FAMILY, ROW, ROW);
        table.put(put);
        table.flushCommits();
      } catch (IOException e) {
        // The region server is going to be aborted.
        // We may get an exception if we retry,
        // which is not guaranteed.
      }

      // Wait 10 seconds for the regionserver to abort: expected result is that
      // it will abort.
      boolean aborted = false;
      for (int i = 0; i < 10; i++) {
        aborted = regionServer.isAborted(); 
        if (aborted) {
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          fail("InterruptedException while waiting for regionserver " +
            "zk node to be deleted.");
        }
      }
      Assert.assertTrue("The region server should have aborted", aborted);
      table.close();
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  public static class FailedInitializationObserver extends SimpleRegionObserver {
    @SuppressWarnings("null")
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
      // Trigger a NPE to fail the coprocessor
      Integer i = null;
      i = i + 1;
    }
  }

  public static class BuggyRegionObserver extends SimpleRegionObserver {
    @SuppressWarnings("null")
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
                       final Put put, final WALEdit edit,
                       final Durability durability) {
      String tableName =
          c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
      if (tableName.equals("observed_table")) {
        // Trigger a NPE to fail the coprocessor
        Integer i = null;
        i = i + 1;
      }
    }
  }
}
