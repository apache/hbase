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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Tests unhandled exceptions thrown by coprocessors running on regionserver.
 * Expected result is that the region server will remove the buggy coprocessor from
 * its set of coprocessors and throw a org.apache.hadoop.hbase.exceptions.DoNotRetryIOException
 * back to the client.
 * (HBASE-4014).
 */
@Category(MediumTests.class)
public class TestRegionServerCoprocessorExceptionWithRemove {
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

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        BuggyRegionObserver.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout=60000)
  public void testExceptionFromCoprocessorDuringPut()
      throws IOException, InterruptedException {
    // Set watches on the zookeeper nodes for all of the regionservers in the
    // cluster. When we try to write to TEST_TABLE, the buggy coprocessor will
    // cause a NullPointerException, which will cause the regionserver (which
    // hosts the region we attempted to write to) to abort. In turn, this will
    // cause the nodeDeleted() method of the DeadRegionServer tracker to
    // execute, which will set the rsZKNodeDeleted flag to true, which will
    // pass this test.

    TableName TEST_TABLE =
        TableName.valueOf("observed_table");
    byte[] TEST_FAMILY = Bytes.toBytes("aaa");

    HTable table = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    TEST_UTIL.createMultiRegions(table, TEST_FAMILY);
    TEST_UTIL.waitUntilAllRegionsAssigned(TEST_TABLE);
    // Note which regionServer that should survive the buggy coprocessor's
    // prePut().
    HRegionServer regionServer =
        TEST_UTIL.getRSForFirstRegionInTable(TEST_TABLE);

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
    // it will survive and not abort.
    for (int i = 0; i < 10; i++) {
      assertFalse(regionServer.isAborted());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        fail("InterruptedException while waiting for regionserver " +
            "zk node to be deleted.");
      }
    }
    table.close();
  }

}

