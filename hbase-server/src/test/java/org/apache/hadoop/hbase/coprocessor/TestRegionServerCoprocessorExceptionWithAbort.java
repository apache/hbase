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

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.junit.AfterClass;
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
  private static final String TABLE_NAME = "observed_table";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);  // Let's fail fast.
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, BuggyRegionObserver.class.getName());
    conf.set("hbase.coprocessor.abortonerror", "true");
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testExceptionFromCoprocessorDuringPut()
    throws IOException {
    // When we try to write to TEST_TABLE, the buggy coprocessor will
    // cause a NullPointerException, which will cause the regionserver (which
    // hosts the region we attempted to write to) to abort.
    byte[] TEST_TABLE = Bytes.toBytes(TABLE_NAME);
    byte[] TEST_FAMILY = Bytes.toBytes("aaa");

    HTable table = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    TEST_UTIL.waitUntilAllRegionsAssigned(TEST_UTIL.createMultiRegions(table, TEST_FAMILY));

    // Note which regionServer will abort (after put is attempted).
    final HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(TEST_TABLE);

    final byte[] ROW = Bytes.toBytes("aaa");
    Put put = new Put(ROW);
    put.add(TEST_FAMILY, ROW, ROW);

    Assert.assertFalse("The region server should be available", regionServer.isAborted());
    try {
      table.put(put);
      fail("The put should have failed, as the coprocessor is buggy");
    } catch (IOException ignored) {
      // Expected.
    }
    Assert.assertTrue("The region server should have aborted", regionServer.isAborted());
    table.close();
  }

  public static class BuggyRegionObserver extends SimpleRegionObserver {
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
                       final Put put, final WALEdit edit,
                       final boolean writeToWAL) {
      String tableName = c.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
      if (TABLE_NAME.equals(tableName)) {
        throw new NullPointerException("Buggy coprocessor");
      }
    }
  }

}
