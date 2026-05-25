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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
public class TestHTableMultiplexerFlushCache {

  private static final Logger LOG = LoggerFactory.getLogger(TestHTableMultiplexerFlushCache.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER1 = Bytes.toBytes("testQualifier_1");
  private static byte[] QUALIFIER2 = Bytes.toBytes("testQualifier_2");
  private static byte[] VALUE1 = Bytes.toBytes("testValue1");
  private static byte[] VALUE2 = Bytes.toBytes("testValue2");
  private static int SLAVES = 3;
  private static int PER_REGIONSERVER_QUEUE_SIZE = 100000;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void checkExistence(final Table htable, final byte[] row, final byte[] family,
    final byte[] quality, final byte[] value) throws Exception {
    // verify that the Get returns the correct result
    TEST_UTIL.waitFor(30000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Result r;
        Get get = new Get(row);
        get.addColumn(family, quality);
        r = htable.get(get);
        return r != null && r.getValue(family, quality) != null
          && Bytes.toStringBinary(value).equals(Bytes.toStringBinary(r.getValue(family, quality)));
      }
    });
  }

  @Test
  public void testOnRegionChange(TestInfo testInfo) throws Exception {
    final TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    final int NUM_REGIONS = 10;
    Table htable = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 3,
      Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);

    HTableMultiplexer multiplexer =
      new HTableMultiplexer(TEST_UTIL.getConfiguration(), PER_REGIONSERVER_QUEUE_SIZE);

    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      byte[][] startRows = r.getStartKeys();
      byte[] row = startRows[1];
      assertTrue(row != null && row.length > 0, "2nd region should not start with empty row");

      Put put = new Put(row).addColumn(FAMILY, QUALIFIER1, VALUE1);
      assertTrue(multiplexer.put(tableName, put), "multiplexer.put returns");

      checkExistence(htable, row, FAMILY, QUALIFIER1, VALUE1);

      // Now let's shutdown the regionserver and let regions moved to other servers.
      HRegionLocation loc = r.getRegionLocation(row);
      MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();
      hbaseCluster.stopRegionServer(loc.getServerName());
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

      // put with multiplexer.
      put = new Put(row).addColumn(FAMILY, QUALIFIER2, VALUE2);
      assertTrue(multiplexer.put(tableName, put), "multiplexer.put returns");

      checkExistence(htable, row, FAMILY, QUALIFIER2, VALUE2);
    }
  }

  @Test
  public void testOnRegionMove(TestInfo testInfo) throws Exception {
    // This test is doing near exactly the same thing that testOnRegionChange but avoiding the
    // potential to get a ConnectionClosingException. By moving the region, we can be certain that
    // the connection is still valid and that the implementation is correctly handling an invalid
    // Region cache (and not just tearing down the entire connection).
    final TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    final int NUM_REGIONS = 10;
    Table htable = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 3,
      Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);

    HTableMultiplexer multiplexer =
      new HTableMultiplexer(TEST_UTIL.getConfiguration(), PER_REGIONSERVER_QUEUE_SIZE);

    final RegionLocator regionLocator = TEST_UTIL.getConnection().getRegionLocator(tableName);
    Pair<byte[][], byte[][]> startEndRows = regionLocator.getStartEndKeys();
    byte[] row = startEndRows.getFirst()[1];
    assertTrue(row != null && row.length > 0, "2nd region should not start with empty row");

    Put put = new Put(row).addColumn(FAMILY, QUALIFIER1, VALUE1);
    assertTrue(multiplexer.put(tableName, put), "multiplexer.put returns");

    checkExistence(htable, row, FAMILY, QUALIFIER1, VALUE1);

    final HRegionLocation loc = regionLocator.getRegionLocation(row);
    final MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();
    // The current server for the region we're writing to
    final ServerName originalServer = loc.getServerName();
    ServerName newServer = null;
    // Find a new server to move that region to
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer rs = hbaseCluster.getRegionServer(i);
      if (!rs.getServerName().equals(originalServer.getServerName())) {
        newServer = rs.getServerName();
        break;
      }
    }
    assertNotNull(newServer, "Did not find a new RegionServer to use");

    // Move the region
    LOG.info("Moving " + loc.getRegionInfo().getEncodedName() + " from " + originalServer + " to "
      + newServer);
    TEST_UTIL.getAdmin().move(loc.getRegionInfo().getEncodedNameAsBytes(),
      Bytes.toBytes(newServer.getServerName()));

    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    // Send a new Put
    put = new Put(row).addColumn(FAMILY, QUALIFIER2, VALUE2);
    assertTrue(multiplexer.put(tableName, put), "multiplexer.put returns");

    // We should see the update make it to the new server eventually
    checkExistence(htable, row, FAMILY, QUALIFIER2, VALUE2);
  }
}
