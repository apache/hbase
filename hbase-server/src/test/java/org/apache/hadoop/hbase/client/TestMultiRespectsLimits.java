/*
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
package org.apache.hadoop.hbase.client;

import static junit.framework.TestCase.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This test sets the multi size WAAAAAY low and then checks to make sure that gets will still make
 * progress.
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestMultiRespectsLimits {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMultiRespectsLimits.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final MetricsAssertHelper METRICS_ASSERT =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);
  private final static byte[] FAMILY = Bytes.toBytes("D");
  public static final int MAX_SIZE = 90;
  private static String LOG_LEVEL;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // disable the debug log to avoid flooding the output
    LOG_LEVEL = Log4jUtils.getEffectiveLevel(AsyncRegionLocatorHelper.class.getName());
    Log4jUtils.setLogLevel(AsyncRegionLocatorHelper.class.getName(), "INFO");
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY,
      MAX_SIZE);

    // Only start on regionserver so that all regions are on the same server.
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (LOG_LEVEL != null) {
      Log4jUtils.setLogLevel(AsyncRegionLocatorHelper.class.getName(), LOG_LEVEL);
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMultiLimits() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table t = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    // Split the table to make sure that the chunking happens accross regions.
    try (final Admin admin = TEST_UTIL.getAdmin()) {
      admin.split(tableName);
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return admin.getTableRegions(tableName).size() > 1;
        }
      });
    }
    List<Get> gets = new ArrayList<>(MAX_SIZE);

    for (int i = 0; i < MAX_SIZE; i++) {
      gets.add(new Get(HBaseTestingUtility.ROWS[i]));
    }

    RpcServerInterface rpcServer = TEST_UTIL.getHBaseCluster().getRegionServer(0).getRpcServer();
    BaseSource s = rpcServer.getMetrics().getMetricsSource();
    long startingExceptions = METRICS_ASSERT.getCounter("exceptions", s);
    long startingMultiExceptions = METRICS_ASSERT.getCounter("exceptions.multiResponseTooLarge", s);

    Result[] results = t.get(gets);
    assertEquals(MAX_SIZE, results.length);

    // Cells from TEST_UTIL.loadTable have a length of 27.
    // Multiplying by less than that gives an easy lower bound on size.
    // However in reality each kv is being reported as much higher than that.
    METRICS_ASSERT.assertCounterGt("exceptions", startingExceptions + ((MAX_SIZE * 25) / MAX_SIZE),
      s);
    METRICS_ASSERT.assertCounterGt("exceptions.multiResponseTooLarge",
      startingMultiExceptions + ((MAX_SIZE * 25) / MAX_SIZE), s);
  }

  @Test
  public void testBlockMultiLimits() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    desc.addFamily(hcd);
    TEST_UTIL.getAdmin().createTable(desc);
    Table t = TEST_UTIL.getConnection().getTable(tableName);

    final HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    RpcServerInterface rpcServer = regionServer.getRpcServer();
    BaseSource s = rpcServer.getMetrics().getMetricsSource();
    long startingExceptions = METRICS_ASSERT.getCounter("exceptions", s);
    long startingMultiExceptions = METRICS_ASSERT.getCounter("exceptions.multiResponseTooLarge", s);

    byte[] row = Bytes.toBytes("TEST");
    byte[][] cols = new byte[][] { Bytes.toBytes("0"), // Get this
      Bytes.toBytes("1"), // Buffer
      Bytes.toBytes("2"), // Buffer
      Bytes.toBytes("3"), // Get This
      Bytes.toBytes("4"), // Buffer
      Bytes.toBytes("5"), // Buffer
      Bytes.toBytes("6"), // Buffer
      Bytes.toBytes("7"), // Get This
      Bytes.toBytes("8"), // Buffer
      Bytes.toBytes("9"), // Buffer
    };

    // Set the value size so that one result will be less than the MAX_SIZE
    // however the block being reference will be larger than MAX_SIZE.
    // This should cause the regionserver to try and send a result immediately.
    byte[] value = new byte[1];
    Bytes.random(value);

    for (int i = 0; i < cols.length; i++) {
      if (i == 6) {
        // do a flush here so we end up with 2 blocks, 55 and 45 bytes
        flush(regionServer, tableName);
      }
      byte[] col = cols[i];
      Put p = new Put(row);
      p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(row).setFamily(FAMILY)
        .setQualifier(col).setTimestamp(p.getTimestamp()).setType(Cell.Type.Put).setValue(value)
        .build());
      t.put(p);
    }

    // Make sure that a flush happens
    flush(regionServer, tableName);

    List<Get> gets = new ArrayList<>(4);
    // This get returns nothing since the filter doesn't match. Filtered cells still retain
    // blocks, and this is a full row scan of both blocks. This equals 100 bytes so we should
    // throw a multiResponseTooLarge after this get if we are counting filtered cells correctly.
    Get g0 = new Get(row).addFamily(FAMILY).setFilter(
      new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("sdf"))));
    gets.add(g0);

    // g1 and g2 each count the first 55 byte block, so we end up with block size of 110
    // after g2 and throw a multiResponseTooLarge before g3
    Get g1 = new Get(row);
    g1.addColumn(FAMILY, cols[0]);
    gets.add(g1);

    Get g2 = new Get(row);
    g2.addColumn(FAMILY, cols[3]);
    gets.add(g2);

    Get g3 = new Get(row);
    g3.addColumn(FAMILY, cols[7]);
    gets.add(g3);

    Result[] results = t.get(gets);
    assertEquals(4, results.length);
    // Expect 2 exceptions (thus 3 rpcs) -- one for g0, then another for g1 + g2, final rpc for g3.
    // If we tracked lastBlock we could squeeze g3 into the second rpc because g2 would be "free"
    // since it's in the same block as g1.
    METRICS_ASSERT.assertCounterGt("exceptions", startingExceptions + 1, s);
    METRICS_ASSERT.assertCounterGt("exceptions.multiResponseTooLarge", startingMultiExceptions + 1,
      s);
  }

  private void flush(HRegionServer regionServer, TableName tableName) throws IOException {
    for (HRegion region : regionServer.getRegions(tableName)) {
      region.flush(true);
    }
  }
}
