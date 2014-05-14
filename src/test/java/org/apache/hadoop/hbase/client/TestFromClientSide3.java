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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestFromClientSide3 {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL
    = new HBaseTestingUtility();
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[] VALUE = Bytes.toBytes("testValue");
  private static Random random = new Random();
  private static int SLAVES = 3;

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

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  private void randomCFPuts(HTable table, byte[] row, byte[] family, int nPuts)
      throws Exception {
    Put put = new Put(row);
    for (int i = 0; i < nPuts; i++) {
      byte[] qualifier = Bytes.toBytes(random.nextInt());
      byte[] value = Bytes.toBytes(random.nextInt());
      put.add(family, qualifier, value);
    }
    table.put(put);
  }

  private void performMultiplePutAndFlush(HBaseAdmin admin, HTable table,
      byte[] row, byte[] family, int nFlushes, int nPuts) throws Exception {

    // connection needed for poll-wait
    HConnection conn = HConnectionManager.getConnection(TEST_UTIL
        .getConfiguration());
    HRegionLocation loc = table.getRegionLocation(row);
    HRegionInterface server = conn.getHRegionConnection(loc.getServerAddress());
    byte[] regName = loc.getRegionInfo().getRegionName();

    for (int i = 0; i < nFlushes; i++) {
      randomCFPuts(table, row, family, nPuts);
      int sfCount = server.getStoreFileList(regName, FAMILY).size();

      // TODO: replace this api with a synchronous flush after HBASE-2949
      admin.flush(table.getTableName());

      // synchronously poll wait for a new storefile to appear (flush happened)
      while (server.getStoreFileList(regName, FAMILY).size() == sfCount) {
        Thread.sleep(40);
      }
    }
  }

  // override the config settings at the CF level and ensure priority
  @Test(timeout = 60000)
  public void testAdvancedConfigOverride() throws Exception {
    /*
     * Overall idea: (1) create 3 store files and issue a compaction. config's
     * compaction.min == 3, so should work. (2) Increase the compaction.min
     * toggle in the HTD to 5 and modify table. If we use the HTD value instead
     * of the default config value, adding 3 files and issuing a compaction
     * SHOULD NOT work (3) Decrease the compaction.min toggle in the HCD to 2
     * and modify table. The CF schema should override the Table schema and now
     * cause a minor compaction.
     */
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compaction.min", 3);

    String tableName = "testAdvancedConfigOverride";
    byte[] TABLE = Bytes.toBytes(tableName);
    HTable hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HConnection connection = HConnectionManager.getConnection(TEST_UTIL
        .getConfiguration());

    // Create 3 store files.
    byte[] row = Bytes.toBytes(random.nextInt());
    performMultiplePutAndFlush(admin, hTable, row, FAMILY, 3, 100);

    // Verify we have multiple store files.
    HRegionLocation loc = hTable.getRegionLocation(row);
    byte[] regionName = loc.getRegionInfo().getRegionName();
    HRegionInterface server = connection.getHRegionConnection(loc
        .getServerAddress());
    assertTrue(server.getStoreFileList(regionName, FAMILY).size() > 1);

    // Issue a compaction request
    admin.compact(TABLE, FAMILY);

    // poll wait for the compactions to happen
    for (int i = 0; i < 10 * 1000 / 40; ++i) {
      // The number of store files after compaction should be lesser.
      loc = hTable.getRegionLocation(row);
      if (!loc.getRegionInfo().isOffline()) {
        regionName = loc.getRegionInfo().getRegionName();
        server = connection.getHRegionConnection(loc.getServerAddress());
        if (server.getStoreFileList(regionName, FAMILY).size() <= 1) {
          break;
        }
      }
      Thread.sleep(40);
    }
    // verify the compactions took place and that we didn't just time out
    assertTrue(server.getStoreFileList(regionName, FAMILY).size() <= 1);

    // change the compaction.min config option for this table to 5
    HTableDescriptor htd = new HTableDescriptor(hTable.getTableDescriptor());
    htd.setValue("hbase.hstore.compaction.min", String.valueOf(5));

    admin.disableTable(TABLE);
    admin.modifyTable(TABLE, htd);
    admin.enableTable(TABLE);

    // Create 3 more store files.
    performMultiplePutAndFlush(admin, hTable, row, FAMILY, 3, 10);

    // Issue a compaction request
    admin.compact(TABLE, FAMILY);

    // This time, the compaction request should not happen
    Thread.sleep(10 * 1000);
    int sfCount = 0;
    loc = hTable.getRegionLocation(row);
    regionName = loc.getRegionInfo().getRegionName();
    server = connection.getHRegionConnection(loc.getServerAddress());
    sfCount = server.getStoreFileList(regionName, FAMILY).size();
    assertTrue(sfCount > 1);

    // change an individual CF's config option to 2 & online schema update
    HColumnDescriptor hcd = new HColumnDescriptor(htd.getFamily(FAMILY));
    hcd.setValue("hbase.hstore.compaction.min", String.valueOf(2));
    admin.alterTable(TABLE, new ArrayList<HColumnDescriptor>(), Lists
        .newArrayList(new Pair<byte[], HColumnDescriptor>(FAMILY, hcd)),
        new ArrayList<byte[]>());
    Pair<Integer, Integer> st;
    while (null != (st = admin.getAlterStatus(TABLE)) && st.getFirst() > 0) {
      LOG.debug(st.getFirst() + " regions left to update");
      Thread.sleep(40);
    }

    // Issue a compaction request
    admin.compact(TABLE, FAMILY);

    // poll wait for the compactions to happen
    for (int i = 0; i < 10 * 1000 / 40; ++i) {
      loc = hTable.getRegionLocation(row);
      regionName = loc.getRegionInfo().getRegionName();
      try {
        server = connection.getHRegionConnection(loc.getServerAddress());
        if (server.getStoreFileList(regionName, FAMILY).size() < sfCount) {
          break;
        }
      } catch (Exception e) {
        LOG.debug("Waiting for region to come online: " + regionName);
      }
      Thread.sleep(40);
    }
    // verify the compaction took place and that we didn't just time out
    assertTrue(server.getStoreFileList(regionName, FAMILY).size() < sfCount);

    // Finally, ensure that we can remove a custom config value after we made it
    hcd.setValue("hbase.hstore.compaction.min", null);
    admin.alterTable(TABLE, new ArrayList<HColumnDescriptor>(), Lists
        .newArrayList(new Pair<byte[], HColumnDescriptor>(FAMILY, hcd)),
        new ArrayList<byte[]>());
    while (null != (st = admin.getAlterStatus(TABLE)) && st.getFirst() > 0) {
      LOG.debug(st.getFirst() + " regions left to update");
      Thread.sleep(40);
    }
    assertNull(hTable.getTableDescriptor().getFamily(FAMILY).getValue(
        "hbase.hstore.compaction.min"));
  }

}
