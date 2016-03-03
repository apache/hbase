/**
 * Copyright The Apache Software Foundation
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({ LargeTests.class, ClientTests.class })
public class TestHTableMultiplexerFlushCache {
  private static final Log LOG = LogFactory.getLog(TestHTableMultiplexerFlushCache.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER1 = Bytes.toBytes("testQualifier_1");
  private static byte[] QUALIFIER2 = Bytes.toBytes("testQualifier_2");
  private static byte[] VALUE1 = Bytes.toBytes("testValue1");
  private static byte[] VALUE2 = Bytes.toBytes("testValue2");
  private static int SLAVES = 3;
  private static int PER_REGIONSERVER_QUEUE_SIZE = 100000;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void checkExistence(final Table htable, final byte[] row, final byte[] family,
      final byte[] quality,
      final byte[] value) throws Exception {
    // verify that the Get returns the correct result
    TEST_UTIL.waitFor(30000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Result r;
        Get get = new Get(row);
        get.addColumn(family, quality);
        r = htable.get(get);
        return r != null && r.getValue(family, quality) != null
            && Bytes.toStringBinary(value).equals(
            Bytes.toStringBinary(r.getValue(family, quality)));
      }
    });
  }

  @Test
  public void testOnRegionChange() throws Exception {
    TableName TABLE = TableName.valueOf("testOnRegionChange");
    final int NUM_REGIONS = 10;
    Table htable = TEST_UTIL.createTable(TABLE, new byte[][] { FAMILY }, 3,
      Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);

    HTableMultiplexer multiplexer = new HTableMultiplexer(TEST_UTIL.getConfiguration(), 
      PER_REGIONSERVER_QUEUE_SIZE);
    
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(TABLE)) {
      byte[][] startRows = r.getStartKeys();
      byte[] row = startRows[1];
      assertTrue("2nd region should not start with empty row", row != null && row.length > 0);

      Put put = new Put(row).addColumn(FAMILY, QUALIFIER1, VALUE1);
      assertTrue("multiplexer.put returns", multiplexer.put(TABLE, put));

      checkExistence(htable, row, FAMILY, QUALIFIER1, VALUE1);

      // Now let's shutdown the regionserver and let regions moved to other servers.
      HRegionLocation loc = r.getRegionLocation(row);
      MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();
      hbaseCluster.stopRegionServer(loc.getServerName());
      TEST_UTIL.waitUntilAllRegionsAssigned(TABLE);

      // put with multiplexer.
      put = new Put(row).addColumn(FAMILY, QUALIFIER2, VALUE2);
      assertTrue("multiplexer.put returns", multiplexer.put(TABLE, put));

      checkExistence(htable, row, FAMILY, QUALIFIER2, VALUE2);
    }
  }

  @Test
  public void testOnRegionMove() throws Exception {
    // This test is doing near exactly the same thing that testOnRegionChange but avoiding the
    // potential to get a ConnectionClosingException. By moving the region, we can be certain that
    // the connection is still valid and that the implementation is correctly handling an invalid
    // Region cache (and not just tearing down the entire connection).
    TableName TABLE = TableName.valueOf("testOnRegionMove");
    final int NUM_REGIONS = 10;
    HTable htable = TEST_UTIL.createTable(TABLE, new byte[][] { FAMILY }, 3,
      Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);

    HTableMultiplexer multiplexer = new HTableMultiplexer(TEST_UTIL.getConfiguration(),
      PER_REGIONSERVER_QUEUE_SIZE);

    final RegionLocator regionLocator = TEST_UTIL.getConnection().getRegionLocator(TABLE);
    Pair<byte[][],byte[][]> startEndRows = regionLocator.getStartEndKeys();
    byte[] row = startEndRows.getFirst()[1];
    assertTrue("2nd region should not start with empty row", row != null && row.length > 0);

    Put put = new Put(row).addColumn(FAMILY, QUALIFIER1, VALUE1);
    assertTrue("multiplexer.put returns", multiplexer.put(TABLE, put));

    checkExistence(htable, row, FAMILY, QUALIFIER1, VALUE1);

    final HRegionLocation loc = regionLocator.getRegionLocation(row);
    final MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();
    // The current server for the region we're writing to
    final ServerName originalServer = loc.getServerName();
    ServerName newServer = null;
    // Find a new server to move that region to
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer rs = hbaseCluster.getRegionServer(0);
      if (!rs.getServerName().equals(originalServer.getServerName())) {
        newServer = rs.getServerName();
        break;
      }
    }
    assertNotNull("Did not find a new RegionServer to use", newServer);

    // Move the region
    LOG.info("Moving " + loc.getRegionInfo().getEncodedName() + " from " + originalServer
        +  " to " + newServer);
    TEST_UTIL.getHBaseAdmin().move(loc.getRegionInfo().getEncodedNameAsBytes(),
        Bytes.toBytes(newServer.getServerName()));

    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE);

    // Send a new Put
    put = new Put(row).addColumn(FAMILY, QUALIFIER2, VALUE2);
    assertTrue("multiplexer.put returns", multiplexer.put(TABLE, put));

    // We should see the update make it to the new server eventually
    checkExistence(htable, row, FAMILY, QUALIFIER2, VALUE2);
  }
}
