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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;
import org.apache.hadoop.hbase.util.TestTableName;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestScannerRetriableFailure {
  private static final Log LOG = LogFactory.getLog(TestScannerRetriableFailure.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final String FAMILY_NAME_STR = "f";
  private static final byte[] FAMILY_NAME = Bytes.toBytes(FAMILY_NAME_STR);

  @Rule public TestTableName TEST_TABLE = new TestTableName();

  public static class FaultyScannerObserver extends BaseRegionObserver {
    private int faults = 0;

    @Override
    public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
        final InternalScanner s, final List<Result> results,
        final int limit, final boolean hasMore) throws IOException {
      final TableName tableName = e.getEnvironment().getRegionInfo().getTable();
      if (!tableName.isSystemTable() && (faults++ % 2) == 0) {
        LOG.debug(" Injecting fault in table=" + tableName + " scanner");
        throw new IOException("injected fault");
      }
      return hasMore;
    }
  }

  private static void setupConf(Configuration conf) {
    conf.setLong("hbase.hstore.compaction.min", 20);
    conf.setLong("hbase.hstore.compaction.max", 39);
    conf.setLong("hbase.hstore.blockingStoreFiles", 40);

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, FaultyScannerObserver.class.getName());
  }

  @BeforeClass
  public static void setup() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test(timeout=180000)
  public void testFaultyScanner() throws Exception {
    TableName tableName = TEST_TABLE.getTableName();
    HTable table = UTIL.createTable(tableName, FAMILY_NAME);
    try {
      final int NUM_ROWS = 100;
      loadTable(table, NUM_ROWS);
      checkTableRows(table, NUM_ROWS);
    } finally {
      table.close();
    }
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private FileSystem getFileSystem() {
    return UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
  }

  private Path getRootDir() {
    return UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
  }

  public void loadTable(final HTable table, int numRows) throws IOException {
    List<Put> puts = new ArrayList<Put>(numRows);
    for (int i = 0; i < numRows; ++i) {
      byte[] row = Bytes.toBytes(String.format("%09d", i));
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILY_NAME, null, row);
      table.put(put);
    }
  }

  private void checkTableRows(final HTable table, int numRows) throws Exception {
    Scan scan = new Scan();
    scan.setCaching(1);
    scan.setCacheBlocks(false);
    ResultScanner scanner = table.getScanner(scan);
    try {
      int count = 0;
      for (int i = 0; i < numRows; ++i) {
        byte[] row = Bytes.toBytes(String.format("%09d", i));
        Result result = scanner.next();
        assertTrue(result != null);
        assertTrue(Bytes.equals(row, result.getRow()));
        count++;
      }

      while (true) {
        Result result = scanner.next();
        if (result == null) break;
        count++;
      }
      assertEquals(numRows, count);
    } finally {
      scanner.close();
    }
  }
}
