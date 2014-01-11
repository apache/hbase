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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Tests unhandled exceptions thrown by coprocessors running on a regionserver..
 * Expected result is that the regionserver will abort with an informative
 * error message describing the set of its loaded coprocessors for crash
 * diagnosis. (HBASE-4014).
 */
@Category(MediumTests.class)
public class TestRegionServerCoprocessorExceptionWithAbort {
  static final Log LOG = LogFactory.getLog(TestRegionServerCoprocessorExceptionWithAbort.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("observed_table");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);  // Let's fail fast.
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, BuggyRegionObserver.class.getName());
    conf.set("hbase.coprocessor.abortonerror", "true");
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testExceptionFromCoprocessorDuringPut()
    throws IOException, InterruptedException {
    // When we try to write to TEST_TABLE, the buggy coprocessor will
    // cause a NullPointerException, which will cause the regionserver (which
    // hosts the region we attempted to write to) to abort.
    TableName TEST_TABLE = TABLE_NAME;
    byte[] TEST_FAMILY = Bytes.toBytes("aaa");

    HTable table = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    TEST_UTIL.createMultiRegions(table, TEST_FAMILY);
    TEST_UTIL.waitUntilAllRegionsAssigned(TEST_TABLE);

    // Note which regionServer will abort (after put is attempted).
    final HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(TEST_TABLE);

    boolean threwIOE = false;
    try {
      final byte[] ROW = Bytes.toBytes("aaa");
      Put put = new Put(ROW);
      put.add(TEST_FAMILY, ROW, ROW);
      table.put(put);
      table.flushCommits();
      // We may need two puts to reliably get an exception
      table.put(put);
      table.flushCommits();
    } catch (IOException e) {
      threwIOE = true;
    } finally {
      assertTrue("The regionserver should have thrown an exception", threwIOE);
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